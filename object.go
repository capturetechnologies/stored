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
	Indexes map[string]Index
}

func (o *Object) Init(name string, db *fdb.Database, schemaObj interface{}) {
	o.name = name
	o.db = db
	o.key = tuple.Tuple{name}
	o.Indexes = map[string]Index{}
	o.buildSchema(schemaObj)
}

func (o *Object) GetKey() tuple.Tuple {
	return o.key
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
		if field.Kind == reflect.Slice {
			field.SubKind = field.Value.Type().Elem().Kind()
		}
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

func (o *Object) GetPrimaryField() *Field {
	if o.primary == "" {
		panic("Object " + o.name + " has no primary key")
	}
	field, ok := o.Fields[o.primary]
	if !ok {
		panic("Object " + o.name + " has invalid primary field")
	}
	return &field
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

func (o *Object) addIndex(key string, unique bool) {
	field, ok := o.Fields[key]
	if !ok {
		panic("Object " + o.name + " has no key «" + key + "» could not set index")
	}
	_, ok = o.Indexes[key]
	if ok {
		panic("Object " + o.name + " already has index «" + key + "»")
	}
	o.Indexes[key] = Index{
		Name:   key,
		field:  &field,
		object: o,
		Unique: unique,
	}
}

func (o *Object) Unique(key string) *Object {
	o.addIndex(key, true)
	return o
}

func (o *Object) Index(key string) *Object {
	o.addIndex(key, false)
	return o
}

func (o *Object) Write(tr fdb.Transaction, data interface{}) error {
	primaryField := o.GetPrimaryField()
	primary := primaryField.GetInterface(data)
	primaryBytes := primaryField.GetBytes(data)
	if primary == nil {
		panic("Object " + o.name + ", primary key «" + o.primary + "» is undefined")
	}
	mainKey := append(o.key, o.primary, primary)
	for key, field := range o.Fields {
		value := field.GetBytes(data)
		k := append(mainKey, key)
		tr.Set(k, value)
		fmt.Println("kv set:", k, value)
	}
	for _, index := range o.Indexes {
		err := index.Write(tr, primary, primaryBytes, data)
		if err != nil {
			return err
		}
	}
	return nil
}

func (o *Object) Set(data interface{}) error {
	_, err := o.db.Transact(func(tr fdb.Transaction) (ret interface{}, e error) {
		e = o.Write(tr, data)
		/*if e != nil {
			fmt.Println("canceling transaction")
			tr.Cancel()
		}*/
		return
	})
	if err != nil {
		return err
	}
	return nil
}

func (o *Object) GetBy(indexKey string, data interface{}) *Value {
	index, ok := o.Indexes[indexKey]
	if !ok {
		panic("Object " + o.name + ", index «" + indexKey + "» is undefined")
	}

	resp, err := o.db.Transact(func(tr fdb.Transaction) (ret interface{}, e error) {
		primary, err := index.GetPrimary(tr, data)
		if err != nil {
			return nil, err
		}

		start, end := primary.FDBRangeKeys()
		r := fdb.KeyRange{Begin: start, End: end}

		res, err := tr.GetRange(r, fdb.RangeOptions{}).GetSliceWithError()

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

func (o *Object) Get(data interface{}) *Value {
	key := append(o.key, o.primary, data)
	resp, err := o.db.Transact(func(tr fdb.Transaction) (ret interface{}, e error) {
		start, end := key.FDBRangeKeys()
		r := fdb.KeyRange{Begin: start, End: end}

		res, err := tr.GetRange(r, fdb.RangeOptions{}).GetSliceWithError()

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
