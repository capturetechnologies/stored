package stored

import (
	"bytes"
	"errors"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
	"github.com/apple/foundationdb/bindings/go/src/fdb/subspace"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
)

// Index represend all indexes sored has
type Index struct {
	Name   string
	Unique bool
	dir    directory.DirectorySubspace
	object *Object
	field  *Field
}

// Write writes index related keys
func (i *Index) Write(tr fdb.Transaction, primaryTuple tuple.Tuple, input *Struct) error {
	indexValue := input.Get(i.field)
	if i.field.isEmpty(indexValue) {
		return nil
	}

	if i.Unique {
		key := tuple.Tuple{indexValue}
		previousPromise := tr.Get(i.dir.Pack(key))

		tr.Set(i.dir.Pack(key), primaryTuple.Pack()) // will be cancelled in case of error

		previousBytes, err := previousPromise.Get()
		if err != nil {
			return err
		}
		if len(previousBytes) != 0 {
			//previousTuple := tuple.Unpack(previousBytes)
			//previous := i.object.GetPrimaryField().ToInterface(previousBytes)
			if !bytes.Equal(primaryTuple.Pack(), previousBytes) {
				//if previousTuple != primaryTuple {
				return errors.New("Object " + i.object.name + " with index (" + i.Name + ") already set")
			}
		}
	} else {
		key := append(tuple.Tuple{indexValue}, primaryTuple...)
		tr.Set(i.dir.Pack(key), []byte{})
	}
	return nil
}

// Delete removes selected index
func (i *Index) Delete(tr fdb.Transaction, input *Struct) {
	indexValue := input.Get(i.field)
	sub := i.dir.Sub(indexValue)
	if i.Unique {
		tr.Clear(sub)
	} else {
		start, end := sub.FDBRangeKeys()
		tr.ClearRange(fdb.KeyRange{Begin: start, End: end})
	}
}

func (i *Index) getList(tr fdb.ReadTransaction, q *Query) ([]*needObject, error) {
	if i.Unique {
		i.object.panic("index is unique (lists not supported)")
	}
	sub := i.dir.Sub(q.primary...)
	start, end := sub.FDBRangeKeys()
	if q.from != nil {
		start = sub.Sub(q.from...)
	}
	r := fdb.KeyRange{Begin: start, End: end}
	rangeResult := tr.GetRange(r, fdb.RangeOptions{Mode: fdb.StreamingModeWantAll, Limit: q.limit, Reverse: q.reverse})
	iterator := rangeResult.Iterator()

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

func (i *Index) getPrimary(tr fdb.ReadTransaction, data interface{}) (subspace.Subspace, error) {
	sub := i.dir.Sub(data)
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
	} else {
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
}
