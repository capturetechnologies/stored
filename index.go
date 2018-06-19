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

func (i *Index) Write(tr fdb.Transaction, primaryTuple tuple.Tuple, input *Struct) error {
	indexValue := input.Get(i.field)
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
				return errors.New("Object with this index already set")
			}
		}
	} else {
		key := append(tuple.Tuple{indexValue}, primaryTuple...)
		tr.Set(i.dir.Pack(key), []byte{})
	}
	return nil
}

func (i *Index) GetPrimary(tr fdb.ReadTransaction, data interface{}) (subspace.Subspace, error) {
	//indexKey := tuple.Tuple{data}
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

		//primaryField := i.object.GetPrimaryField()
		//primaryData := primaryField.ToInterface(bytes)

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
