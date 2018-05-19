package stored

import (
	"fmt"
	"reflect"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
)

type Object struct {
	name    string
	db      *fdb.Database
	primary string
	key     tuple.Tuple
	Fields  map[string]Field
}

func (o *Object) Init(name string, db *fdb.Database, schemaObj interface{}) {
	o.name = name
	o.db = db
	o.key = tuple.Tuple{name}
	o.buildSchema(schemaObj)
}

func (o *Object) buildSchema(schemaObj interface{}) {
	t := reflect.TypeOf(schemaObj)
	v := reflect.ValueOf(schemaObj)
	if v.Kind() == reflect.Ptr {
		t = t.Elem()
		v = v.Elem()
	}
	numFields := v.NumField()
	o.Fields = map[string]Field{}
	for i := 0; i < numFields; i++ {
		field := Field{
			Num:   i,
			Type:  t.Field(i),
			Value: v.Field(i),
		}
		field.Kind = field.Value.Kind()
		tag := field.ParseTag()
		if tag != nil {
			o.Fields[tag.Name] = field
			if tag.Primary {
				if o.primary != "" {
					panic("Object " + o.name + " primary key already set to «" + o.primary + "», could not set to «" + tag.Name + "»")
				}
				o.primary = tag.Name
			}
		}
	}
	return
}

func (o *Object) getPrimaryValue(data interface{}) interface{} {
	if o.primary == "" {
		panic("Object " + o.name + " has no primary key")
	}
	field, ok := o.Fields[o.primary]
	if !ok {
		panic("Object " + o.name + " has invalid primary field")
	}
	return field.GetInterface(data)
}

func (o *Object) Primary(key string) *Object {
	if o.primary != "" {
		panic("Object " + o.name + " already has primary key")
	}
	_, ok := o.Fields[key]
	if !ok {
		panic("Object " + o.name + " has no key «" + key + "» could not set primary")
	}
	o.primary = key
	return o
}

func (o *Object) Write(tr fdb.Transaction, data interface{}) {
	primary := o.getPrimaryValue(data)
	if primary == nil {
		panic("Object " + o.name + ", primary key «" + o.primary + "» is undefined")
	}
	mainKey := append(o.key, primary)
	for key, field := range o.Fields {
		value := field.GetBytes(data)
		k := append(mainKey, key)
		tr.Set(k, value)
		fmt.Println("set", k, " -> ", value)
	}
}

func (o *Object) Set(data interface{}) error {
	fmt.Println("transaction start")
	_, err := o.db.Transact(func(tr fdb.Transaction) (ret interface{}, e error) {
		o.Write(tr, data)
		return
	})
	fmt.Println("transaction end")
	if err != nil {
		return err
	}
	return nil
}

func (o *Object) toKey(data interface{}) tuple.TupleElement {
	//return data
	switch data.(type) {
	case string:
		return data.(string)
	case []byte:
		return data.([]byte)
	default:
		return fmt.Sprintf("%v", data)
	}
}

func (o *Object) Get(data interface{}) *Value {
	key := append(o.key, o.toKey(data))
	resp, err := o.db.Transact(func(tr fdb.Transaction) (ret interface{}, e error) {
		fmt.Println("fetch keys", key)
		//keyRange := fdb.KeyRange{append(key, ""), append(key, "\xFF")}
		start, end := key.FDBRangeKeys()
		r := fdb.KeyRange{Begin: start, End: end}

		fmt.Println("fetch keys", r)
		res, err := tr.GetRange(r, fdb.RangeOptions{}).GetSliceWithError()
		fmt.Println("fetch res", res)
		return res, err
	})
	if err != nil {
		return &Value{err: err}
	}
	rows := resp.([]fdb.KeyValue)
	value := Value{
		object: o,
	}
	value.FromKeyValue(rows)
	return &value
}
