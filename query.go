package stored

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/subspace"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
)

// Query is interface for query building
type Query struct {
	object  *Object
	index   *Index
	primary tuple.Tuple
	from    tuple.Tuple
	to      tuple.Tuple
	next    struct {
		from    tuple.Tuple // fills after first slice of data was scanned
		started bool
	}
	limit   int
	reverse bool
	fn      func()
}

// Use is an index selector for query building
func (q *Query) Use(indexFieldNames ...string) *Query {
	indexName := strings.Join(indexFieldNames, ",")
	for key, i := range q.object.indexes {
		if key == indexName {
			q.index = i
			return q
		}
	}
	q.object.panic("index " + indexName + " is undefined")
	return q
}

// Limit sets limit for the query
func (q *Query) Limit(limit int) *Query {
	q.limit = limit
	return q
}

// Reverse reverse the query order
func (q *Query) Reverse() *Query {
	q.reverse = true
	return q
}

// SetReverse set reverse value from param
func (q *Query) SetReverse(reverse bool) *Query {
	q.reverse = reverse
	return q
}

// List queries list of items using primary key subspace. Pass no params if fetching all objects
func (q *Query) List(values ...interface{}) *Query {
	if len(values) == 0 {
		return q
	}
	q.primary = tuple.Tuple{}
	for _, v := range values {
		q.primary = append(q.primary, v)
	}

	if q.index == nil {
		maxFields := len(q.object.primaryFields)
		if len(q.primary) >= maxFields {
			q.object.panic("List should have less than " + strconv.Itoa(maxFields) + " params (number of primary keys)")
		}
	} else {
		if len(q.primary) != 1 {
			q.object.panic("List should have 1 property since indexes support only 1 value")
		}
	}
	return q
}

// From sets the part of primary key of item starting from which result will be returned
// primary key part passed to List param should be excluded
func (q *Query) From(values ...interface{}) *Query {
	if len(values) == 0 {
		return q
	}
	q.from = tuple.Tuple{}
	for _, v := range values {
		switch enc := v.(type) {
		case byte:
			q.from = append(q.from, []byte{enc})
		default:
			q.from = append(q.from, v)
		}
	}
	return q
}

// To sets the part of primary key which returning list should be ended
// primary key part passed to List param should be excluded
func (q *Query) To(values ...interface{}) *Query {
	if len(values) == 0 {
		return q
	}
	q.to = tuple.Tuple{}
	for _, v := range values {
		switch enc := v.(type) {
		case byte:
			q.to = append(q.to, []byte{enc})
		default:
			q.to = append(q.to, v)
		}
	}
	return q
}

// ScanAll scans the query result into the passed
func (q *Query) ScanAll(slicePointer interface{}) error {
	// sould be queried here
	sliceI := q.execute()
	slice := sliceI.(*Slice)
	return slice.ScanAll(slicePointer)
}

// Slice will return slice object
func (q *Query) Slice() *Slice {
	sliceI := q.execute()
	return sliceI.(*Slice)
}

// execute the query
// could be called several times with one query
func (q *Query) execute() interface{} {
	keyLen := len(q.object.primaryFields)
	resp, err := q.object.db.ReadTransact(func(tr fdb.ReadTransaction) (ret interface{}, e error) {
		tr = tr.Snapshot()
		if q.index != nil { // select using index
			values, err := q.index.getList(tr, q)
			if err != nil {
				return nil, err
			}
			slice := Slice{}
			for _, needed := range values {
				v, err := needed.fetch()
				if err != nil {
					return nil, err
				}
				slice.Append(v)
			}
			return &slice, nil
		}

		var sub subspace.Subspace
		sub = q.object.primary
		if q.primary != nil {
			sub = sub.Sub(q.primary...)
		}
		start, end := sub.FDBRangeKeys()
		if q.from != nil {
			if q.reverse {
				end = sub.Pack(q.from)
			} else {
				start = sub.Pack(q.from)
			}
		}
		if q.to != nil {
			if q.reverse {
				start = sub.Pack(q.to)
			} else {
				end = sub.Pack(q.to)
			}
		}

		r := fdb.KeyRange{Begin: start, End: end}

		limit := q.object.getKeyLimit(q.limit)
		if q.next.started {
			limit++
		}

		rangeResult := tr.GetRange(r, fdb.RangeOptions{Mode: fdb.StreamingModeWantAll, Limit: limit, Reverse: q.reverse})
		iterator := rangeResult.Iterator()
		elem := valueRaw{}
		//res := []valueRaw{}

		slice := Slice{}
		var lastTuple tuple.Tuple
		rowsNum := 0
		for iterator.Advance() {
			kv, err := iterator.Get()
			if err != nil {
				return nil, err
			}
			fullTuple, err := q.object.primary.Unpack(kv.Key)
			if err != nil {
				return nil, err
			}

			if len(fullTuple) < keyLen {
				fmt.Println("data corrupt", len(fullTuple), "vs", keyLen)
				return nil, ErrDataCorrupt
			}
			primaryTuple := fullTuple[:keyLen]

			if lastTuple != nil && !reflect.DeepEqual(primaryTuple, lastTuple) {
				value := Value{
					object: q.object,
				}
				value.fromRaw(elem)
				value.fromKeyTuple(lastTuple)
				slice.Append(&value)
				// push to items here
				//res = append(res, elem)
				elem = valueRaw{}
				rowsNum = 0
			}
			fieldsKey := fullTuple[keyLen:]
			if len(fieldsKey) > 1 {
				q.object.panic("nested fields not yet supported")
			}
			if len(fieldsKey) == 1 {
				keyName, ok := fieldsKey[0].(string)
				if !ok {
					q.object.panic("invalid key, not string")
				}
				elem[keyName] = kv.Value
			}
			lastTuple = primaryTuple
			rowsNum++
		}
		if rowsNum != 0 && (q.limit == 0 || slice.Len() < q.limit) {
			value := Value{
				object: q.object,
			}
			value.fromRaw(elem)
			value.fromKeyTuple(lastTuple)
			slice.Append(&value)
			//res = append(res, elem)
		}
		/*if len(res) == 0 {
			return &Slice{values: []*Value{}}, nil
			//return nil, ErrNotFound
		}*/

		if !reflect.DeepEqual(q.from, lastTuple) {
			q.next.from = lastTuple
			//q.next.from = incrementTuple(lastTuple)
		} else {
			q.next.from = nil
		}

		//return q.object.wrapObjectList(res)
		return &slice, nil
	})
	if err != nil {
		return &Slice{err: err}
	}
	return resp
}

// Next sets from identifier from nextFrom; return true if more data could be fetched
func (q *Query) Next() bool {
	if q.next.started {
		if q.next.from != nil {
			q.from = q.next.from
			q.next.from = nil
			return true
		}
		return false
	}
	q.next.started = true // prevent endless circle if no queries presented
	return true
}
