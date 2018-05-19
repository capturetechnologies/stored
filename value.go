package stored

import (
	"fmt"
	"reflect"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
)

type Value struct {
	object *Object
	data   map[string]interface{}
	err    error
}

func (v *Value) FromKeyValue(rows []fdb.KeyValue) {
	fmt.Println("from kv", rows)
	v.data = map[string]interface{}{}
	for _, row := range rows {
		fmt.Println("read rows, key", row.Key)
		key := string(row.Key)
		field, ok := v.object.Fields[key]
		if !ok {
			continue
		}
		v.data[key] = field.ToInterface(row.Value)
	}
}

func (v *Value) Scan(obj interface{}) error {
	fmt.Println("on scan", obj)
	if v.err != nil {
		return v.err
	}
	object := reflect.ValueOf(obj)
	for key, val := range v.data {
		field, ok := v.object.Fields[key]
		if !ok {
			continue
		}
		objField := object.Field(field.Num)

		interfaceValue := reflect.ValueOf(val)
		objField.Set(interfaceValue)
	}
	return nil
}
