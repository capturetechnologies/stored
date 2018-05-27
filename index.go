package stored

import (
	"errors"
	"fmt"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
	"github.com/apple/foundationdb/bindings/go/src/fdb/subspace"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
)

type Index struct {
	Name     string
	Unique   bool
	subspace directory.DirectorySubspace
	object   *Object
	field    *Field
}

func (i *Index) Write(tr fdb.Transaction, primary interface{}, primaryBytes []byte, data interface{}) error {
	indexValue := i.field.GetInterface(data)
	if i.Unique {
		key := tuple.Tuple{indexValue}
		previousBytes, err := tr.Get(i.subspace.Pack(key)).Get()
		if err != nil {
			fmt.Println("previous error", err)
			return err
		}
		fmt.Println("previous bytes", previousBytes)
		if len(previousBytes) != 0 {
			previous := i.object.GetPrimaryField().ToInterface(previousBytes)
			if previous != indexValue {
				return errors.New("Object with this index already set")
			}
		} else {
			tr.Set(i.subspace.Pack(key), primaryBytes)
		}

		fmt.Println("writing unique index", key)
	} else {
		key := tuple.Tuple{indexValue, primary}
		tr.Set(i.subspace.Pack(key), []byte{})
	}
	return nil
}

func (i *Index) GetPrimary(tr fdb.Transaction, data interface{}) (subspace.Subspace, error) {
	indexKey := tuple.Tuple{data}
	if i.Unique {
		bytes, err := tr.Get(i.subspace.Pack(indexKey)).Get()
		if err != nil {
			return nil, err
		}
		if len(bytes) == 0 {
			return nil, errors.New("row not found")
		}
		primaryField := i.object.GetPrimaryField()
		primaryData := primaryField.ToInterface(bytes)
		return i.object.primary.Sub(primaryData), nil
	} else {
		sel := fdb.FirstGreaterThan(i.subspace.Pack(indexKey))
		primaryKey, err := tr.GetKey(sel).Get()
		if err != nil {
			return nil, err
		}
		primary, err := i.subspace.Unpack(primaryKey)
		//primary, err := UnpackKeyIndex(indexKey, primaryKey)
		if err != nil || len(primary) < 2 || primary[0] != data {
			return nil, errors.New("row not found")
		}

		return i.object.primary.Sub(primary[1]), nil
	}
}
