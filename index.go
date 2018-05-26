package stored

import (
	"errors"
	"fmt"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
)

type Index struct {
	Name   string
	Unique bool
	object *Object
	field  *Field
}

func (i *Index) Write(tr fdb.Transaction, primary interface{}, primaryBytes []byte, data interface{}) error {
	indexName := i.Name
	indexValue := i.field.GetInterface(data)
	if i.Unique {
		key := append(i.object.GetKey(), indexName, indexValue)
		previousBytes, err := tr.Get(key).Get()
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
			tr.Set(key, primaryBytes)
		}

		fmt.Println("writing unique index", key)
	} else {
		key := append(i.object.GetKey(), indexName, indexValue, primary)
		tr.Set(key, []byte{})
	}
	return nil
}

func (i *Index) GetPrimary(tr fdb.Transaction, data interface{}) (tuple.Tuple, error) {
	indexName := i.Name
	indexKey := append(i.object.GetKey(), indexName, data)
	if i.Unique {
		bytes, err := tr.Get(indexKey).Get()
		if err != nil {
			return nil, err
		}
		if len(bytes) == 0 {
			return nil, errors.New("row not found")
		}
		primaryField := i.object.GetPrimaryField()
		primaryData := primaryField.ToInterface(bytes)
		key := append(i.object.GetKey(), i.object.primary, primaryData)
		return key, nil
	} else {
		sel := fdb.FirstGreaterThan(indexKey)
		primaryKey, err := tr.GetKey(sel).Get()
		if err != nil {
			return nil, err
		}
		fmt.Println("KEY CAME", primaryKey)
		primary, err := tuple.Unpack(primaryKey)
		if err != nil {
			return nil, err
		}
		fmt.Println("KEY UNPACKED", primary)
		primaryData := primary[len(primary)-1]
		fmt.Println("KEY LAST", primaryData)
		key := append(i.object.GetKey(), i.object.primary, primaryData)
		return key, nil
	}
}
