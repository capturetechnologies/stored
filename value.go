package stored

import (
	"fmt"
	"reflect"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/subspace"
)

type valueRaw map[string][]byte

// Value is representation of the fetched raw object from the db
type Value struct {
	object *Object
	fetch  func()
	//data   map[string]interface{}
	raw valueRaw
	err error
}

func (v *Value) get() {
	if v.fetch != nil {
		v.fetch()
		v.fetch = nil
	}
}

func (v *Value) fromRaw(raw valueRaw) {
	v.raw = raw

	//todelete
	/*v.data = map[string]interface{}{}
	for fieldName, binaryValue := range raw {
		field, ok := v.object.fields[fieldName]

		if !ok {
			continue
		}
		v.data[fieldName] = field.ToInterface(binaryValue)
	}*/
}

// FromKeyValue pasrses key value from foundationdb
func (v *Value) FromKeyValue(sub subspace.Subspace, rows []fdb.KeyValue) {
	v.raw = valueRaw{}
	for _, row := range rows {
		key, err := sub.Unpack(row.Key)
		if err != nil || len(key) < 1 {
			fmt.Println("key in invalid", err)
			continue
		}
		fieldName, ok := key[0].(string)
		if !ok {
			fmt.Println("field is not string")
			continue
		}
		v.raw[fieldName] = row.Value
	}

	// todelete
	/*v.data = map[string]interface{}{}
	for _, row := range rows {
		key, err := sub.Unpack(row.Key)
		if err != nil || len(key) < 1 {
			fmt.Println("key in invalid", err)
			continue
		}
		fieldName, ok := key[0].(string)
		if !ok {
			fmt.Println("field is not string")
			continue
		}
		field, ok := v.object.fields[fieldName]
		if !ok {
			fmt.Println("SKIP FIELD", fieldName)
			continue
		}
		v.data[fieldName] = field.ToInterface(row.Value)
	}*/
}

// Scan fills object with data from value
func (v *Value) Scan(objectPtr interface{}) error {
	if v.fetch != nil {
		v.fetch()
		v.fetch = nil
	}
	if v.err != nil {
		return v.err
	}

	value := structEditable(objectPtr)
	value.Fill(v.object, v)
	/*object := reflect.ValueOf(objectPtr).Elem()
	for key, val := range v.data {
		field, ok := v.object.fields[key]
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
	}*/
	return nil
}

// Reflect returns link to reflact value of object
func (v *Value) Reflect() (reflect.Value, error) {
	if v.fetch != nil {
		v.fetch()
		v.fetch = nil
	}
	value := reflect.New(v.object.reflectType)
	if v.err != nil {
		return value, v.err
	}
	value = value.Elem()
	for key, binaryValue := range v.raw {
		field, ok := v.object.fields[key]
		if !ok {
			continue
		}
		objField := value.Field(field.Num)
		if !objField.CanSet() {
			fmt.Println("Could not set object", key)
			continue
		}

		val := field.ToInterface(binaryValue)
		interfaceValue := reflect.ValueOf(val)
		objField.Set(interfaceValue)
	}
	return value, nil
}

// Err returns an error
func (v *Value) Err() error {
	if v.fetch != nil {
		v.fetch()
		v.fetch = nil
	}
	return v.err
}

// Interface returns an interface
func (v *Value) Interface() interface{} {
	refl, _ := v.Reflect()
	return refl.Interface()
}
