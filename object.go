package stored

import (
	"reflect"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
	"github.com/apple/foundationdb/bindings/go/src/fdb/subspace"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
)

// Object is an abstraction for working with objects
type Object struct {
	name        string
	reflectType reflect.Type
	db          *fdb.Database
	primaryKey  string
	directory   *Directory
	dir         directory.DirectorySubspace
	miscDir     directory.DirectorySubspace
	primary     directory.DirectorySubspace
	Fields      map[string]Field
	Indexes     map[string]Index
}

func (o *Object) init(name string, db *fdb.Database, dir *Directory, schemaObj interface{}) {
	o.name = name
	o.db = db
	o.directory = dir
	var err error
	o.dir, err = dir.Subspace.CreateOrOpen(db, []string{name}, nil)
	if err != nil {
		panic(err)
	}
	o.miscDir, err = o.dir.CreateOrOpen(db, []string{"misc"}, nil)
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
	//index := o.addIndex(name, true) // primary is also index
	var err error
	o.primary, err = o.dir.CreateOrOpen(o.db, []string{name}, nil)
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
	o.reflectType = t
	numFields := v.NumField()
	o.Fields = map[string]Field{}
	for i := 0; i < numFields; i++ {
		field := Field{
			object: o,
			Num:    i,
			Type:   t.Field(i),
			Value:  v.Field(i),
		}
		field.Kind = field.Value.Kind()
		if field.Kind == reflect.Slice {
			field.SubKind = field.Value.Type().Elem().Kind()
		}
		tag := field.ParseTag()
		if tag != nil {
			field.Name = tag.Name
			if tag.AutoIncrement {
				field.SetAutoIncrement()
			}
			o.Fields[tag.Name] = field
			if tag.Primary {
				o.setPrimary(tag.Name)
			}
		}
	}
	return
}

// GetPrimaryField return primary field of an STORED object
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

// Primary sets primary field in case it wasnot set with annotations
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
	indexSubspace, err := o.dir.CreateOrOpen(o.db, []string{key}, nil)
	if err != nil {
		panic(err)
	}
	o.Indexes[key] = Index{
		Name:   key,
		field:  &field,
		object: o,
		dir:    indexSubspace,
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

func (o *Object) Write(tr fdb.Transaction, input *Struct) error {
	primaryField := o.GetPrimaryField()
	primary := input.Get(primaryField)
	primaryBytes := input.GetBytes(primaryField)
	if primary == nil {
		panic("Object " + o.name + ", primary key «" + o.primaryKey + "» is undefined")
	}
	for key, field := range o.Fields {
		value := input.GetBytes(&field)
		k := tuple.Tuple{primary, key}
		tr.Set(o.primary.Pack(k), value)
	}
	for _, index := range o.Indexes {
		err := index.Write(tr, primary, primaryBytes, input)
		if err != nil {
			return err
		}
	}
	return nil
}

// Set writes data, would return error if primary key is empty
func (o *Object) Set(data interface{}) error {
	input := StructAny(data)
	_, err := o.db.Transact(func(tr fdb.Transaction) (ret interface{}, e error) {
		e = o.Write(tr, input)
		return
	})
	if err != nil {
		return err
	}
	return nil
}

// Add writes data even in primary key is empty, by setting it. Take a look at autoincrement tag
func (o *Object) Add(data interface{}) error {
	input := StructEditable(data)
	_, err := o.db.Transact(func(tr fdb.Transaction) (ret interface{}, e error) {
		for _, field := range o.Fields {
			if field.AutoIncrement {
				incKey := o.miscDir.Pack(tuple.Tuple{"ai", field.Name})
				tr.Add(incKey, field.Get1())
				autoIncrementValue := tr.Get(incKey).MustGet()
				input.Set(&field, autoIncrementValue)
			}
		}
		e = o.Write(tr, input)
		return
	})
	if err != nil {
		return err
	}
	return nil
}

// GetBy fetch one row using index bye name or name of the index field
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

// Get fetch object using primary id
func (o *Object) Get(data interface{}) *Value {
	//key := tuple.Tuple{data}
	key := o.primary.Sub(data)
	resp, err := o.db.Transact(func(tr fdb.Transaction) (ret interface{}, e error) {
		res, err := NeedRange(tr, key).GetSliceWithError()

		return res, err
	})
	if err != nil {
		return &Value{err: err}
	}
	rows := resp.([]fdb.KeyValue)
	if len(rows) == 0 {
		return &Value{err: ErrNotFound}
	}
	value := Value{
		object: o,
	}
	value.FromKeyValue(key, rows)
	return &value
}

// MultiGet fetch list of objects using primary id
func (o *Object) MultiGet(data []int64) *Slice {
	resp, err := o.db.Transact(func(tr fdb.Transaction) (ret interface{}, e error) {
		needed := make([]fdb.RangeResult, len(data))
		for k, v := range data { // iterate each key
			needed[k] = NeedRange(tr, o.primary.Sub(v))
		}
		results := make([][]fdb.KeyValue, len(data))
		for k, v := range needed {
			res, err := v.GetSliceWithError()
			if err != nil {
				return nil, err
			}
			results[k] = res
		}
		return results, nil
	})
	if err != nil {
		return &Slice{err: err}
	}
	rows := resp.([][]fdb.KeyValue)
	if len(rows) == 0 {
		return &Slice{err: ErrNotFound}
	}
	slice := Slice{}
	for k, v := range rows {
		key := o.primary.Sub(data[k])
		value := Value{
			object: o,
		}
		value.FromKeyValue(key, v)
		slice.Append(&value)
	}
	return &slice
}
