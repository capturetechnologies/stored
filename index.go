package stored

import (
	"bytes"
	"errors"
	"fmt"
	"reflect"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
	"github.com/apple/foundationdb/bindings/go/src/fdb/subspace"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
	"github.com/mmcloughlin/geohash"
)

// Index represend all indexes sored has
type Index struct {
	Name         string
	Unique       bool
	Geo          int  // geo precision used to
	search       bool // means for each word
	dir          directory.DirectorySubspace
	object       *Object
	optional     bool
	fields       []*Field
	handle       func(interface{}) KeyTuple
	checkHandler func(obj interface{}) bool
}

// IndexOption is in option struct which allow to set differnt options
type IndexOption struct {
	// CheckHandler describes should index be written for specific object or not
	CheckHandler func(obj interface{}) bool
}

func (i *Index) isEmpty(input *Struct) bool {
	for _, field := range i.fields {
		if !field.isEmpty(input.Get(field)) {
			return false
		}
	}
	return true
}

// getKey will return index tuple
func (i *Index) getKey(input *Struct) (key tuple.Tuple) {
	if i.handle != nil {
		keyTuple := i.handle(input.value.Interface())
		// Would not index object if key is empty
		if keyTuple == nil || len(keyTuple) == 0 {
			return nil
		}
		tmpTuple := tuple.Tuple{}
		for _, element := range keyTuple {
			tmpTuple = append(tmpTuple, element)
		}

		// embedded tuple cause problems with partitial fetching
		key = tmpTuple
		//key = tuple.Tuple{tmpTuple} // embedded tuple will be better in that case
	} else {
		key = tuple.Tuple{}
		if i.Geo != 0 {
			latInterface := input.Get(i.fields[0])
			lngInterface := input.Get(i.fields[1])
			lat, long := latInterface.(float64), lngInterface.(float64)
			if lat == 0.0 && long == 0.0 {
				return nil
			}
			hash := geohash.Encode(lat, long)
			if i.Geo < 12 {
				hash = hash[0:i.Geo] // Cutting hash to needed precision
			}
			key = append(key, hash)
		} else {
			//key = tuple.Tuple{indexValue}
			for _, field := range i.fields {
				indexValue := input.Get(field)
				key = append(key, field.tupleElement(indexValue))
			}
		}
	}
	return
}

// writeSearch will set new index keys and delete old ones for text search index
func (i *Index) writeSearch(tr fdb.Transaction, primaryTuple tuple.Tuple, input, oldObject *Struct) error {
	newWords := searchGetInputWords(i, input)
	toAddWords := map[string]bool{}
	skip := false
	if i.checkHandler != nil {
		if !i.checkHandler(input.value.Interface()) {
			//fmt.Println("skipping index")
			skip = true
		}
		// old value is better to delete any way
	}
	if !skip {
		for _, word := range newWords {
			toAddWords[word] = true
		}
		fmt.Println("index words >>", newWords)
	}
	toDeleteWords := map[string]bool{}
	if oldObject != nil {
		oldWords := searchGetInputWords(i, oldObject)
		for _, word := range oldWords {
			_, ok := toAddWords[word]
			if ok {
				delete(toAddWords, word)
			} else {
				toDeleteWords[word] = true
			}

		}
	}
	for word := range toAddWords {
		key := tuple.Tuple{word}
		fullKey := append(key, primaryTuple...)
		fmt.Println("write search key", fullKey, "packed", i.dir.Pack(fullKey))
		tr.Set(i.dir.Pack(fullKey), []byte{})
	}
	for word := range toDeleteWords {
		key := tuple.Tuple{word}
		fullKey := append(key, primaryTuple...)
		tr.Clear(i.dir.Pack(fullKey))
	}
	return nil
}

// Write writes index related keys
func (i *Index) Write(tr fdb.Transaction, primaryTuple tuple.Tuple, input, oldObject *Struct) error {
	if i.search {
		return i.writeSearch(tr, primaryTuple, input, oldObject)
	}
	key := i.getKey(input)
	if oldObject != nil {
		toDelete := i.getKey(oldObject)
		if toDelete != nil {
			if reflect.DeepEqual(toDelete, key) {
				return nil
			}
			i.Delete(tr, primaryTuple, toDelete)
		}
	}
	if i.optional && i.isEmpty(input) { // no need to delete any inex than
		return nil
	}
	// nil means should not index this object
	if key == nil {
		return nil
	}

	if i.Unique {
		previousPromise := tr.Get(i.dir.Pack(key))

		tr.Set(i.dir.Pack(key), primaryTuple.Pack()) // will be cancelled in case of error

		previousBytes, err := previousPromise.Get()
		if err != nil {
			return err
		}
		if len(previousBytes) != 0 {
			if !bytes.Equal(primaryTuple.Pack(), previousBytes) {
				return ErrAlreadyExist
			}
		}
	} else {
		fullKey := append(key, primaryTuple...)
		tr.Set(i.dir.Pack(fullKey), []byte{})
	}
	return nil
}

// Delete removes selected index
func (i *Index) Delete(tr fdb.Transaction, primaryTuple tuple.Tuple, key tuple.Tuple) {
	if key == nil {
		// no need to clean, this field wasn't indexed
		return
	}
	sub := i.dir.Sub(key...)
	if i.Unique {
		tr.Clear(sub)
	} else {
		// Add primary here
		sub = sub.Sub(primaryTuple...)
		tr.Clear(sub) // removing old keys
	}
}

func (i *Index) getIterator(tr fdb.ReadTransaction, q *Query) (subspace.Subspace, *fdb.RangeIterator) {
	if i.Unique {
		i.object.panic("index is unique (lists not supported)")
	}
	//if len(q.primary) != 0 {
	sub := i.dir.Sub(q.primary...)
	start, end := sub.FDBRangeKeys()
	if q.from != nil {
		//start = sub.Sub(q.from...)
		if q.reverse {
			end = sub.Pack(q.from)
		} else {
			start = sub.Pack(q.from)
		}
		if q.to != nil {
			if q.reverse {
				start = sub.Pack(q.to)
			} else {
				end = sub.Pack(q.to)
			}
		}
	}

	r := fdb.KeyRange{Begin: start, End: end}
	rangeResult := tr.GetRange(r, fdb.RangeOptions{Mode: fdb.StreamingModeWantAll, Limit: q.limit, Reverse: q.reverse})
	iterator := rangeResult.Iterator()
	return sub, iterator
}

// getList will fetch and request all the objects using the index
func (i *Index) getList(tr fdb.ReadTransaction, q *Query) ([]*needObject, error) {
	sub, iterator := i.getIterator(tr, q)

	primaryLen := len(i.object.primaryFields)
	values := []*needObject{}
	for iterator.Advance() {
		kv, err := iterator.Get()
		if err != nil {
			return nil, err
		}

		fullTuple, err := sub.Unpack(kv.Key)
		if err != nil {
			return nil, err
		}
		if len(fullTuple)-primaryLen < 0 {
			return nil, errors.New("invalid data: key too short")
		}
		key := fullTuple[len(fullTuple)-primaryLen:]

		values = append(values, i.object.need(tr, i.object.sub(key)))
	}
	return values, nil
}

// getPrimariesList will fetch just an list of primaries
func (i *Index) getPrimariesList(tr fdb.ReadTransaction, q *Query) (*Slice, error) {
	sub, iterator := i.getIterator(tr, q)

	primaryLen := len(i.object.primaryFields)
	values := []*Value{}
	for iterator.Advance() {
		kv, err := iterator.Get()
		if err != nil {
			return nil, err
		}
		fullTuple, err := sub.Unpack(kv.Key)
		if err != nil {
			return nil, err
		}
		if len(fullTuple)-primaryLen < 0 {
			return nil, errors.New("invalid data: key too short")
		}
		key := fullTuple[len(fullTuple)-primaryLen:]
		value := Value{object: i.object}
		value.fromKeyTuple(key)

		values = append(values, &value)
	}
	return &Slice{values: values}, nil
}

func (i *Index) getPrimary(tr fdb.ReadTransaction, indexKey tuple.Tuple) (subspace.Subspace, error) {
	sub := i.dir.Sub(indexKey...)
	if i.Unique {
		bytes, err := tr.Get(sub).Get()
		if err != nil {
			return nil, err
		}
		if len(bytes) == 0 {
			return nil, ErrNotFound
		}
		primaryTuple, err := tuple.Unpack(bytes)
		if err != nil {
			return nil, err
		}
		return i.object.primary.Sub(primaryTuple...), nil
	}

	sel := fdb.FirstGreaterThan(sub)
	primaryKey, err := tr.GetKey(sel).Get()
	if err != nil {
		return nil, err
	}
	primaryTuple, err := sub.Unpack(primaryKey)
	//primary, err := UnpackKeyIndex(indexKey, primaryKey)
	if err != nil || len(primaryTuple) < 1 {
		return nil, ErrNotFound
	}

	return i.object.primary.Sub(primaryTuple...), nil
}

// ReindexUnsafe will update index info (NOT consistency safe function)
// this function will use data provited by th object so should be used with care
func (i *Index) ReindexUnsafe(data interface{}) *PromiseErr {
	input := structAny(data)
	p := i.object.promiseErr()
	p.do(func() Chain {
		primaryTuple := input.getPrimary(i.object)
		err := i.Write(p.tr, primaryTuple, input, nil)
		if err != nil {
			return p.fail(err)
		}
		return p.done(nil)
	})
	return p
}

func (i *Index) doClearAll(tr fdb.Transaction) {
	start, end := i.dir.FDBRangeKeys()
	tr.ClearRange(fdb.KeyRange{Begin: start, End: end})
}

// ClearAll will remove all data for specific index
func (i *Index) ClearAll() error {
	_, err := i.object.db.Transact(func(tr fdb.Transaction) (ret interface{}, e error) {
		i.doClearAll(tr)
		return
	})
	return err
}

// Reindex will reindex index data
func (i *Index) Reindex() {
	i.ClearAll()
	object := i.object
	query := object.ListAll().Limit(100)
	errorCount := 0
	for query.Next() {
		query.Slice().Each(func(item interface{}) {
			input := structAny(item)
			primaryTuple := input.getPrimary(object)
			_, err := object.db.Transact(func(tr fdb.Transaction) (ret interface{}, e error) {

				/*sub := object.sub(primaryTuple)
				needed := object.need(tr, sub)
				value, err := needed.fetch()
				var oldObject *Struct
				if err != ErrNotFound {
					if err != nil {
						return
					}
					err = value.Err()
					if err != nil {
						return
					}
					oldObject = structAny(value.Interface())
				}*/

				//err = i.Write(tr, primaryTuple, input, oldObject)
				err := i.Write(tr, primaryTuple, input, nil) // write everything

				return nil, err
			})
			if err != nil {
				fmt.Println("reindex fail of object «"+object.name+"»:", err)
				errorCount++
			}
		})
	}
	if errorCount > 0 {
		fmt.Printf("Reindex finished with %d errors\n", errorCount)
	} else {
		fmt.Println("Reindex successfully finished")
	}
}

// SetOption allow to set option
func (i *Index) SetOption(option IndexOption) {
	if option.CheckHandler != nil {
		i.checkHandler = option.CheckHandler
	}
}

// Options allow to set list of options
func (i *Index) Options(options ...IndexOption) {
	for _, option := range options {
		i.SetOption(option)
	}
}
