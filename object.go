package stored

import (
	"fmt"
	"reflect"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
	"github.com/apple/foundationdb/bindings/go/src/fdb/subspace"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
)

type Object struct {
	name       string
	db         *fdb.Database
	primaryKey string
	//key       tuple.Tuple
	directory *Directory
	subspace  directory.DirectorySubspace
	primary   directory.DirectorySubspace
	Fields    map[string]Field
	Indexes   map[string]Index
}

func (o *Object) Init(name string, db *fdb.Database, dir *Directory, schemaObj interface{}) {
	o.name = name
	o.db = db
	o.directory = dir
	var err error
	o.subspace, err = dir.Subspace.CreateOrOpen(db, []string{name}, nil)
	if err != nil {
		panic(err)
	}
	//o.key = tuple.Tuple{name}
	o.Indexes = map[string]Index{}
	o.buildSchema(schemaObj)
}

func (o *Object) setPrimary(name string) {
	if o.primaryKey != "" {
		if o.primaryKey == name {
			return
		}
		panic("Object " + o.name + " primary key already set to «" + o.primaryKey + "», could not set to «" + name + "»")
	}
	o.primaryKey = name
	var err error
	o.primary, err = o.subspace.CreateOrOpen(o.db, []string{name}, nil)
	if err != nil {
		panic(err)
	}
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
				o.setPrimary(tag.Name)
			}
		}
	}
	return
}

func (o *Object) GetPrimaryField() *Field {
	if o.primaryKey == "" {
		panic("Object " + o.name + " has no primary key")
	}
	field, ok := o.Fields[o.primaryKey]
	if !ok {
		panic("Object " + o.name + " has invalid primary field")
	}
	return &field
}

func (o *Object) Primary(name string) *Object {
	_, ok := o.Fields[name]
	if !ok {
		panic("Object " + o.name + " has no key «" + name + "» could not set primary")
	}
	o.setPrimary(name)
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
	indexSubspace, err := o.subspace.CreateOrOpen(o.db, []string{key}, nil)
	if err != nil {
		panic(err)
	}
	o.Indexes[key] = Index{
		Name:     key,
		field:    &field,
		object:   o,
		subspace: indexSubspace,
		Unique:   unique,
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
		panic("Object " + o.name + ", primary key «" + o.primaryKey + "» is undefined")
	}
	for key, field := range o.Fields {
		value := field.GetBytes(data)
		k := tuple.Tuple{primary, key}
		tr.Set(o.primary.Pack(k), value)
		fmt.Println("kv set:", k, " -> ", value)
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

	var keysub subspace.Subspace
	resp, err := o.db.Transact(func(tr fdb.Transaction) (ret interface{}, e error) {
		primary, err := index.GetPrimary(tr, data)
		if err != nil {
			return nil, err
		}
		keysub = primary

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
	value.FromKeyValue(keysub, rows)
	return &value
}

func (o *Object) Get(data interface{}) *Value {
	//key := tuple.Tuple{data}
	keysub := o.primary.Sub(data)
	resp, err := o.db.Transact(func(tr fdb.Transaction) (ret interface{}, e error) {
		//start, end := key.FDBRangeKeys()
		start, end := keysub.FDBRangeKeys()
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
	value.FromKeyValue(keysub, rows)
	return &value
}
