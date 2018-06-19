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
	name            string
	reflectType     reflect.Type
	db              *fdb.Database
	primaryKey      string
	primaryFields   []*Field
	multiplePrimary bool
	directory       *Directory
	dir             directory.DirectorySubspace
	miscDir         directory.DirectorySubspace
	primary         directory.DirectorySubspace
	fields          map[string]*Field
	Indexes         map[string]*Index
	Relations       []*Relation
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

func (o *Object) setPrimary(names ...string) {
	var name string
	if len(names) == 1 {
		name = names[0]
	}
	if o.primaryKey != "" {
		if o.primaryKey == name {
			return
		}
		o.panic("primary key already set to «" + o.primaryKey + "», could not set to «" + name + "»")
	}
	var err error
	if len(names) > 1 {
		o.primaryFields = []*Field{}
		for _, name := range names {
			field := o.fields[name]
			o.primaryFields = append(o.primaryFields, field)
		}
		o.primaryKey = names[0]
		o.multiplePrimary = true
		o.primary, err = o.dir.CreateOrOpen(o.db, names, nil)
		//panic("not implemented yet")
	} else {
		o.primaryKey = name
		o.primaryFields = []*Field{o.fields[name]}
		//index := o.addIndex(name, true) // primary is also index
		o.primary, err = o.dir.CreateOrOpen(o.db, []string{name}, nil)
	}
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
	numfields := v.NumField()
	o.fields = map[string]*Field{}
	for i := 0; i < numfields; i++ {
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
			o.fields[tag.Name] = &field
			if tag.Primary {
				o.setPrimary(tag.Name)
			}
		}
	}
	return
}

func (o *Object) wrapRange(resp interface{}, err error, dataKeys []subspace.Subspace) *Slice {
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

// GetPrimaryField return primary field of an STORED object
func (o *Object) GetPrimaryField() *Field {
	if o.primaryKey == "" {
		panic("Object " + o.name + " has no primary key")
	}
	field, ok := o.fields[o.primaryKey]
	if !ok {
		panic("Object " + o.name + " has invalid primary field")
	}
	return field
}

// Primary sets primary field in case it wasnot set with annotations
func (o *Object) Primary(names ...string) *Object {
	for _, name := range names {
		_, ok := o.fields[name]
		if !ok {
			panic("Object " + o.name + " has no key «" + name + "» could not set primary")
		}
	}
	o.setPrimary(names...)
	return o
}

func (o *Object) AutoIncrement(name string) *Object {
	field, ok := o.fields[name]
	if !ok {
		panic("Object " + o.name + " has no key «" + name + "» could not set autoincrement")
	}
	field.SetAutoIncrement()
	return o
}

func (o *Object) addIndex(key string, unique bool) {
	field, ok := o.fields[key]
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
	//primaryField := o.GetPrimaryField()
	//primaryID := input.Get(primaryField)
	//primaryBytes := input.GetBytes(primaryField)

	primaryTuple := input.Primary(o)
	sub := o.primary.Sub(primaryTuple...)

	//key := o.primary.Sub(primaryID)

	if clear {
		start, end := o.primary.Sub(primaryTuple...).FDBRangeKeys()
		tr.ClearRange(fdb.KeyRange{Begin: start, End: end})
	}

	for k, field := range o.fields {
		value := input.GetBytes(field)
		tr.Set(sub.Pack(tuple.Tuple{k}), value)
	}
	for _, index := range o.Indexes {
		err := index.Write(tr, primaryTuple, input)
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

func (o *Object) panic(text string) {
	panic("Stored error, object " + o.name + ":")
}

// field return field using name, panic an error if no field presented
func (o *Object) field(fieldName string) *Field {
	field, ok := o.fields[fieldName]
	if !ok {
		o.panic("field «" + fieldName + "» not found")
	}
	return field
}

func (o *Object) getPrimaryTuple(objOrID interface{}) tuple.Tuple {
	object := reflect.ValueOf(objOrID)
	kind := object.Kind()
	if kind == reflect.Ptr {
		object = object.Elem()
		kind = object.Kind()
	}
	res := tuple.Tuple{}

	if kind == reflect.Struct {
		for _, field := range o.primaryFields {
			row := object.Field(field.Num)
			res = append(res, row.Interface())
		}
	} else {
		if o.multiplePrimary {
			o.panic("with multiple primary index objOrID should be object")
		}
		if len(o.primaryFields) < 1 {
			o.panic("primary key should be set")
		}
		field := o.primaryFields[0]
		if kind != field.Kind {
			o.panic("primary key type does not matched with passed one")
		}
		res = append(res, objOrID)
	}
	return res
}

// Subspace return subspace using object or interface
func (o *Object) Subspace(objOrID interface{}) subspace.Subspace {
	primaryTuple := o.getPrimaryTuple(objOrID)
	return o.primary.Sub(primaryTuple...)
}

// IncField increment field
func (o *Object) IncField(objOrID interface{}, fieldName string, incVal interface{}) error {
	field := o.field(fieldName)
	//primaryID := o.GetPrimaryField().fromAnyInterface(objOrID)
	sub := o.Subspace(objOrID)
	_, err := o.db.Transact(func(tr fdb.Transaction) (ret interface{}, e error) {
		incKey := sub.Pack(tuple.Tuple{field.Name})
		val, err := field.ToBytes(incVal)
		if err != nil {
			return nil, err
		}
		tr.Add(incKey, val)
		return
	})
	return err
}

// IncGetField increment field and return new value
func (o *Object) IncGetField(objOrID interface{}, fieldName string, incVal interface{}) *Var {
	field := o.field(fieldName)
	//primaryID := o.GetPrimaryField().fromAnyInterface(objOrID)
	sub := o.Subspace(objOrID)
	res, err := o.db.Transact(func(tr fdb.Transaction) (ret interface{}, e error) {
		incKey := sub.Pack(tuple.Tuple{field.Name})
		val, err := field.ToBytes(incVal)
		if err != nil {
			return nil, err
		}
		tr.Add(incKey, val)
		bytes, err := tr.Get(incKey).Get()
		if err != nil {
			return nil, err
		}
		return field.ToInterface(bytes), nil
	})
	return &Var{
		data: res,
		err:  err,
	}
}

// UpdateField sets any value to requested field
func (o *Object) UpdateField(objOrID interface{}, fieldName string, value interface{}) error {
	field := o.field(fieldName)
	bytesValue, err := field.ToBytes(value)
	if err != nil {
		return err
	}
	//primaryID := o.GetPrimaryField().fromAnyInterface(objOrID)
	sub := o.Subspace(objOrID)
	_, err = o.db.Transact(func(tr fdb.Transaction) (ret interface{}, e error) {
		key := sub.Pack(tuple.Tuple{field.Name})
		val, err := tr.Get(key).Get()
		if err != nil {
			return nil, err
		}
		if val == nil {
			return nil, ErrNotFound
		}
		tr.Set(key, bytesValue)
		return
	})
	return err
}

// Add writes data even in primary key is empty, by setting it. Take a look at autoincrement tag
func (o *Object) Add(data interface{}) error {
	input := StructEditable(data)
	_, err := o.db.Transact(func(tr fdb.Transaction) (ret interface{}, e error) {
		for _, field := range o.fields {
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

	//var keysub subspace.Subspace
	resp, err := o.db.ReadTransact(func(tr fdb.ReadTransaction) (ret interface{}, e error) {
		sub, err := index.GetPrimary(tr, data)
		if err != nil {
			return nil, err
		}

		start, end := sub.FDBRangeKeys()
		r := fdb.KeyRange{Begin: start, End: end}

		rows, err := tr.GetRange(r, fdb.RangeOptions{
			Mode: fdb.StreamingModeWantAll,
		}).GetSliceWithError()
		if err != nil {
			return nil, err
		}
		value := Value{
			object: o,
		}
		value.FromKeyValue(sub, rows)
		return &value, nil
	})
	if err != nil {
		return &Value{err: err}
	}
	return resp.(*Value)
	/*rows := resp.([]fdb.KeyValue)
	value := Value{
		object: o,
	}
	value.FromKeyValue(keysub, rows)
	return &value*/
}

// Get fetch object using primary id
func (o *Object) Get(objOrID interface{}) *Value {
	//key := tuple.Tuple{data}
	//key := o.primary.Sub(data)
	sub := o.Subspace(objOrID)
	resp, err := o.db.ReadTransact(func(tr fdb.ReadTransaction) (ret interface{}, e error) {
		res, err := NeedRange(tr, sub).GetSliceWithError()
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
	value.FromKeyValue(sub, rows)
	return &value
}

// Get fetch object using primary id
func (o *Object) GetList(opts SelectOptions) *Slice {
	//key := tuple.Tuple{data}
	//key := o.primary.Sub(data)
	var sub subspace.Subspace
	sub = o.primary
	keyLen := len(o.primaryFields)
	if opts.Primary != nil {
		sub = sub.Sub(opts.Primary...)
		keyLen -= len(opts.Primary)
	}
	if keyLen < 1 {
		o.panic("contain no list with that primary")
	}
	resp, err := o.db.ReadTransact(func(tr fdb.ReadTransaction) (ret interface{}, e error) {
		start, end := sub.FDBRangeKeys()
		r := fdb.KeyRange{Begin: start, End: end}
		rangeResult := tr.GetRange(r, fdb.RangeOptions{Mode: fdb.StreamingModeWantAll, Limit: opts.Limit})
		iterator := rangeResult.Iterator()
		elem := valueRaw{}
		res := []valueRaw{}
		var lastTuple tuple.Tuple
		rowsNum := 0
		for iterator.Advance() {
			kv, err := iterator.Get()
			if err != nil {
				return nil, err
			}
			fullTuple, err := sub.Unpack(kv.Key)
			if err != nil {
				return nil, err
			}
			if len(fullTuple) <= keyLen {
				return nil, ErrDataCorrupt
			}
			primaryTuple := fullTuple[:keyLen]
			if lastTuple != nil && !reflect.DeepEqual(primaryTuple, lastTuple) {
				// push to items here
				res = append(res, elem)
				elem = valueRaw{}
				rowsNum = 0
			}
			fieldsKey := fullTuple[keyLen:]
			if len(fieldsKey) > 1 {
				o.panic("nested fields not yet supported")
			}
			keyName, ok := fieldsKey[0].(string)
			if !ok {
				o.panic("invalid key, not string")
			}
			elem[keyName] = kv.Value
			lastTuple = primaryTuple
			rowsNum++
		}
		if rowsNum != 0 {
			res = append(res, elem)
		}
		if len(res) == 0 {
			return nil, ErrNotFound
		}

		return o.wrapObjectList(res)

		//res, err := NeedRange(tr, sub).GetSliceWithError()
		//return res, err
	})
	if err != nil {
		return &Slice{err: err}
	}
	return resp.(*Slice)
}

func (o *Object) wrapObjectList(rows []valueRaw) (*Slice, error) {
	slice := Slice{}
	for _, row := range rows {
		value := Value{
			object: o,
		}
		value.fromRaw(row)
		slice.Append(&value)
	}
	return &slice, nil
}

func (o *Object) sliceToKeys(data interface{}) []subspace.Subspace {
	if o.multiplePrimary {
		panic("multiget for multiple primary not implemented yet")
	}
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
	return dataKeys
}

// MultiGet fetch list of objects using primary id
func (o *Object) MultiGet(data interface{}) *Slice {
	dataKeys := o.sliceToKeys(data)
	resp, err := o.db.ReadTransact(func(tr fdb.ReadTransaction) (ret interface{}, e error) {
		needed := make([]fdb.RangeResult, len(dataKeys))
		for k, v := range dataKeys { // iterate each key
			needed[k] = NeedRange(tr, v)
		}
		return FetchRange(tr, needed)
	})
	return o.wrapRange(resp, err, dataKeys)
}

// Creates object to object relation between current object and other one
// N2N represents relations when unlimited number of host objects connected to unlimited
// amount of client objects
func (o *Object) N2N(client *Object) *Relation {
	rel := Relation{}
	rel.init(RelationN2N, o, client)
	return &rel
}
