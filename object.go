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
	Fields      map[string]*Field
	Indexes     map[string]*Index
	Relations   []*Relation
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
	o.Indexes = map[string]*Index{}
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
	o.Fields = map[string]*Field{}
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
			o.Fields[tag.Name] = &field
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
	return field
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

func (o *Object) AutoIncrement(name string) *Object {
	field, ok := o.Fields[name]
	if !ok {
		panic("Object " + o.name + " has no key «" + name + "» could not set autoincrement")
	}
	field.SetAutoIncrement()
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
	o.Indexes[key] = &Index{
		Name:   key,
		field:  field,
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

func (o *Object) Write(tr fdb.Transaction, input *Struct, clear bool) error {
	primaryField := o.GetPrimaryField()
	primaryID := input.Get(primaryField)
	primaryBytes := input.GetBytes(primaryField)
	if primaryID == nil {
		panic("Object " + o.name + ", primary key «" + o.primaryKey + "» is undefined")
	}

	if clear {
		start, end := o.primary.Sub(primaryID).FDBRangeKeys()
		tr.ClearRange(fdb.KeyRange{Begin: start, End: end})
	}

	for key, field := range o.Fields {
		value := input.GetBytes(field)
		k := tuple.Tuple{primaryID, key}
		tr.Set(o.primary.Pack(k), value)
	}
	for _, index := range o.Indexes {
		err := index.Write(tr, primaryID, primaryBytes, input)
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
		e = o.Write(tr, input, true)
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
				input.Set(field, autoIncrementValue)
			}
		}
		e = o.Write(tr, input, false)
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
	resp, err := o.db.ReadTransact(func(tr fdb.ReadTransaction) (ret interface{}, e error) {
		primary, err := index.GetPrimary(tr, data)
		if err != nil {
			return nil, err
		}
		keysub = primary

		start, end := primary.FDBRangeKeys()
		r := fdb.KeyRange{Begin: start, End: end}

		res, err := tr.GetRange(r, fdb.RangeOptions{
			Mode: fdb.StreamingModeWantAll,
		}).GetSliceWithError()

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
	resp, err := o.db.ReadTransact(func(tr fdb.ReadTransaction) (ret interface{}, e error) {
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
func (o *Object) MultiGet(data interface{}) *Slice {
	primaryField := o.GetPrimaryField()
	var dataKeys []subspace.Subspace
	switch primaryField.Kind {
	case reflect.Int32:
		vData, ok := data.([]int32)
		if !ok {
			panic("you should pass []int32 to the multiget")
		}
		dataKeys = make([]subspace.Subspace, len(vData))
		for k, v := range vData {
			dataKeys[k] = o.primary.Sub(v)
		}
	case reflect.Int:
		vData, ok := data.([]int)
		if !ok {
			panic("you should pass []int to the multiget")
		}
		dataKeys = make([]subspace.Subspace, len(vData))
		for k, v := range vData {
			dataKeys[k] = o.primary.Sub(v)
		}
	case reflect.Int64:
		vData, ok := data.([]int64)
		if !ok {
			panic("you should pass []int64 to the multiget")
		}
		dataKeys = make([]subspace.Subspace, len(vData))
		for k, v := range vData {
			dataKeys[k] = o.primary.Sub(v)
		}
	case reflect.Int8:
		vData, ok := data.([]int8)
		if !ok {
			panic("you should pass []int8 to the multiget")
		}
		dataKeys = make([]subspace.Subspace, len(vData))
		for k, v := range vData {
			dataKeys[k] = o.primary.Sub(v)
		}
	case reflect.Int16:
		vData, ok := data.([]int16)
		if !ok {
			panic("you should pass []int16 to the multiget")
		}
		dataKeys = make([]subspace.Subspace, len(vData))
		for k, v := range vData {
			dataKeys[k] = o.primary.Sub(v)
		}
	case reflect.String:
		vData, ok := data.([]string)
		if !ok {
			panic("you should pass []string to the multiget")
		}
		dataKeys = make([]subspace.Subspace, len(vData))
		for k, v := range vData {
			dataKeys[k] = o.primary.Sub(v)
		}
	case reflect.Slice:
		if primaryField.SubKind == reflect.Uint8 {
			vData, ok := data.([]byte)
			if !ok {
				panic("you should pass [][]byte to the multiget")
			}
			dataKeys = make([]subspace.Subspace, len(vData))
			for k, v := range vData {
				dataKeys[k] = o.primary.Sub(v)
			}
		} else {
			panic("only []byte slice supported for multiget")
		}
	default:
		panic("type not supported for multiget")
	}

	resp, err := o.db.ReadTransact(func(tr fdb.ReadTransaction) (ret interface{}, e error) {
		needed := make([]fdb.RangeResult, len(dataKeys))
		for k, v := range dataKeys { // iterate each key
			needed[k] = NeedRange(tr, v)
		}
		results := make([][]fdb.KeyValue, len(dataKeys))
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
		key := dataKeys[k]
		value := Value{
			object: o,
		}
		value.FromKeyValue(key, v)
		slice.Append(&value)
	}
	return &slice
}

// Creates object to object relation between current object and other one
// N2N represents relations when unlimited number of host objects connected to unlimited
// amount of client objects
func (o *Object) N2N(client *Object) *Relation {
	rel := Relation{}
	rel.Init(RelationN2N, o, client)
	return &rel
}
