package stored

import (
	"fmt"
	"reflect"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/subspace"
)

type Value struct {
	object *Object
	data   map[string]interface{}
	err    error
}

func (v *Value) FromKeyValue(sub subspace.Subspace, rows []fdb.KeyValue) {
	v.data = map[string]interface{}{}
	for _, row := range rows {
		key, err := sub.Unpack(row.Key)
		//key, err := tuple.Unpack(row.Key)
		if err != nil || len(key) < 1 {
			fmt.Println("key in invalid", err)
			continue
		}

		fieldName, ok := key[0].(string)
		if !ok {
			fmt.Println("field is not string")
			continue
		}

		field, ok := v.object.Fields[fieldName]

		if !ok {
			fmt.Println("field has no value")
			continue
		}

		//fmt.Println("kv get:", fieldName, row.Value, field.ToInterface(row.Value))
		v.data[fieldName] = field.ToInterface(row.Value)
	}
}

func (v *Value) Scan(obj interface{}) error {
	if v.err != nil {
		return v.err
	}
	object := reflect.ValueOf(obj).Elem()
	for key, val := range v.data {
		field, ok := v.object.Fields[key]
		if !ok {
			continue
		}
		objField := object.Field(field.Num)
		if !objField.CanSet() {
			fmt.Println("Could not set object", key)
			continue
		}

		interfaceValue := reflect.ValueOf(val)
		objField.Set(interfaceValue)
	}
	return nil
}

// Reflect returns link to reflact value of object
func (v *Value) Reflect() (reflect.Value, error) {
	value := reflect.New(v.object.reflectType)
	if v.err != nil {
		return value, v.err
	}
	value = value.Elem()
	for key, val := range v.data {
		field, ok := v.object.Fields[key]
		if !ok {
			continue
		}
		objField := value.Field(field.Num)
		if !objField.CanSet() {
			fmt.Println("Could not set object", key)
			continue
		}
		interfaceValue := reflect.ValueOf(val)
		objField.Set(interfaceValue)
	}
	return value, nil
}

// Interface returns an interface
func (v *Value) Interface() interface{} {
	refl, _ := v.Reflect()
	return refl.Interface()
}
