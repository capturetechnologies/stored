package stored

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
	"github.com/apple/foundationdb/bindings/go/src/fdb/subspace"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
	"github.com/mmcloughlin/geohash"
)

// Index represend all indexes sored has
type Index struct {
	Name      string
	Unique    bool
	Geo       int // geo precision used to
	dir       directory.DirectorySubspace
	object    *Object
	field     *Field
	secondary *Field
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
			if !bytes.Equal(primaryTuple.Pack(), previousBytes) {
				return ErrAlreadyExist
			}
		}
	} else if i.Geo != 0 {
		lngInterface := input.Get(i.secondary)
		lat, long := indexValue.(float64), lngInterface.(float64)
		if lat != 0.0 && long != 0.0 {
			hash := geohash.Encode(lat, long)
			if i.Geo < 12 {
				hash = hash[0:i.Geo] // Cutting hash to needed precision
			}
			key := append(tuple.Tuple{hash}, primaryTuple...)
			tr.Set(i.dir.Pack(key), []byte{})
			fmt.Println("[A] index writed", i.dir.Pack(key))
		}
	} else {
		key := append(tuple.Tuple{indexValue}, primaryTuple...)
		keyPacked := i.dir.Pack(key)
		tr.Set(keyPacked, []byte{})
	}
	return nil
}

// Delete removes selected index
func (i *Index) Delete(tr fdb.Transaction, primaryTuple tuple.Tuple, oldObject *Struct) {
	indexValue := oldObject.Get(i.field)
	sub := i.dir.Sub(indexValue)
	if i.Unique {
		tr.Clear(sub)
	} else {
		// Add primary here
		sub = sub.Sub(primaryTuple...)
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
		//start = sub.Sub(q.from...)
		if q.reverse {
			end = sub.Pack(q.from)
		} else {
			start = sub.Pack(q.from)
		}
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
