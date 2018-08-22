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
	keysCount       int
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
		field.init()
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
			if tag.UnStored {
				field.UnStored = true
			} else {
				o.keysCount++
			}
		}
	}
	return
}

func (o *Object) wrapRange(needed []*needObject) *Slice {
	if len(needed) == 0 {
		return &Slice{values: []*Value{}} // empty slice instead of error
		//return &Slice{err: ErrNotFound}
	}
	slice := Slice{}
	for _, need := range needed {
		value, err := need.fetch()
		if err != nil {
			return &Slice{err: err}
		}
		slice.Append(value)
	}
	return &slice
}

/*func (o *Object) wrapRange(rowsList [][]fdb.KeyValue, dataKeys []subspace.Subspace) *Slice {
	if len(rowsList) == 0 {
		return &Slice{values: []*Value{}} // empty slice instead of error
		//return &Slice{err: ErrNotFound}
	}
	slice := Slice{}
	for k, rows := range rowsList {
		key := dataKeys[k]
		value := Value{
			object: o,
		}
		value.FromKeyValue(key, rows)
		slice.Append(&value)
	}
	return &slice
}*/

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

func (o *Object) getPrimarySub(objOrID interface{}) subspace.Subspace {
	key := o.getPrimaryTuple(objOrID)
	return o.primary.Sub(key...)
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
			res = append(res, field.tupleElement(row.Interface()))
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

func (o *Object) Unique(key string) *Object {
	o.addIndex(key, true)
	return o
}

// Index add an index
func (o *Object) Index(key string) *Object {
	o.addIndex(key, false)
	return o
}

func (o *Object) doWrite(tr fdb.Transaction, sub subspace.Subspace, primaryTuple tuple.Tuple, input *Struct, clear bool) error {
	if clear {
		start, end := o.primary.Sub(primaryTuple...).FDBRangeKeys()
		tr.ClearRange(fdb.KeyRange{Begin: start, End: end})
	}

	for k, field := range o.fields {
		if field.UnStored {
			continue
		}
		value := input.GetBytes(field)
		tr.Set(sub.Pack(tuple.Tuple{k}), value)
	}
	for _, index := range o.Indexes {
		//fmt.Println("WRITE index", primaryTuple, input)
		err := index.Write(tr, primaryTuple, input)
		if err != nil {
			return err
		}
	}
	return nil
}

func (o *Object) promise() *Promise {
	return &Promise{
		db: o.db,
	}
}

// Update writes data, only if row already exist
func (o *Object) Update(data interface{}) *Promise {
	input := StructAny(data)
	p := o.promise()
	p.do(func() Chain {
		primaryTuple := input.Primary(o)

		// delete all indexes data
		sub := o.sub(primaryTuple)
		needed := o.need(p.tr, sub)
		//res := needObject(p.tr, sub)
		return func() Chain {
			value, err := needed.fetch()
			if err == ErrNotFound {
				return p.fail(ErrNotFound)
			}
			if err != nil {
				return p.fail(err)
			}
			err = value.Err()
			if err != nil {
				return p.fail(err)
			}
			object := StructAny(value.Interface())

			// remove indexes
			for _, index := range o.Indexes {
				index.Delete(p.tr, object)
			}

			err = o.doWrite(p.tr, sub, primaryTuple, input, true)
			if err != nil {
				return p.fail(err)
			}
			return p.done(nil)
		}
	})

	return p
}

// Set writes data, would return error if primary key is empty
func (o *Object) Set(data interface{}) *Promise {
	input := StructAny(data)
	p := o.promise()
	p.do(func() Chain {
		primaryTuple := input.Primary(o)

		// delete all indexes data
		sub := o.sub(primaryTuple)
		needed := o.need(p.tr, sub)
		//res := needObject(p.tr, sub)
		return func() Chain {
			value, err := needed.fetch()
			if err != ErrNotFound {
				if err != nil {
					return p.fail(err)
				}
				err = value.Err()
				if err != nil {
					return p.fail(err)
				}
				object := StructAny(value.Interface())

				// remove indexes
				for _, index := range o.Indexes {
					index.Delete(p.tr, object)
				}
			}

			err = o.doWrite(p.tr, sub, primaryTuple, input, true)
			if err != nil {
				p.fail(err)
			}
			return p.done(nil)
		}
	})

	return p
}

// Subspace return subspace using object or interface
func (o *Object) Subspace(objOrID interface{}) subspace.Subspace {
	primaryTuple := o.getPrimaryTuple(objOrID)
	return o.primary.Sub(primaryTuple...)
}

// IncField increment field
func (o *Object) IncField(objOrID interface{}, fieldName string, incVal interface{}) *Promise {
	field := o.field(fieldName)
	//primaryID := o.GetPrimaryField().fromAnyInterface(objOrID)
	sub := o.Subspace(objOrID)
	p := o.promise()
	p.do(func() Chain {
		incKey := sub.Pack(tuple.Tuple{field.Name})
		val, err := field.ToBytes(incVal)
		if err != nil {
			return p.fail(err)
		}
		p.tr.Add(incKey, val)
		return p.done(nil)
	})
	return p
}

// IncGetField increment field and return new value
func (o *Object) IncGetField(objOrID interface{}, fieldName string, incVal interface{}) *Promise {
	field := o.field(fieldName)

	p := o.promise()
	p.do(func() Chain {
		sub := o.Subspace(objOrID)
		incKey := sub.Pack(tuple.Tuple{field.Name})
		val, err := field.ToBytes(incVal)
		if err != nil {
			return p.fail(err)
		}
		p.tr.Add(incKey, val)
		fieldGet := p.tr.Get(incKey)
		return func() Chain {
			bytes, err := fieldGet.Get()
			if err != nil {
				return p.fail(err)
			}
			p.setValueField(o, field, bytes)
			return p.done(nil)
		}
	})
	return p

	/*sub := o.Subspace(objOrID)
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
	}*/
}

// UpdateField updates object field via callback with old value
func (o *Object) UpdateField(objOrID interface{}, fieldName string, callback func(value interface{}) (interface{}, error)) *Promise {
	field := o.field(fieldName)

	p := o.promise()
	p.do(func() Chain {
		sub := o.Subspace(objOrID)
		key := sub.Pack(tuple.Tuple{field.Name})
		fieldGet := p.tr.Get(key)
		return func() Chain {
			val, err := fieldGet.Get()
			if err != nil {
				return p.fail(err)
			}
			if val == nil {
				return p.fail(ErrNotFound)
			}
			newValue, err := callback(field.ToInterface(val))
			if err != nil {
				return p.fail(err)
			}
			bytesValue, err := field.ToBytes(newValue)
			if err != nil {
				return p.fail(err)
			}
			p.tr.Set(key, bytesValue)
			return p.done(nil)
		}
	})
	return p

	/*sub := o.Subspace(objOrID)
	_, err := o.db.Transact(func(tr fdb.Transaction) (ret interface{}, e error) {
		key := sub.Pack(tuple.Tuple{field.Name})
		val, err := tr.Get(key).Get()
		if err != nil {
			return nil, err
		}
		if val == nil {
			return nil, ErrNotFound
		}
		newValue, err := callback(field.ToInterface(val))
		if err != nil {
			return nil, err
		}
		bytesValue, err := field.ToBytes(newValue)
		if err != nil {
			return nil, err
		}
		tr.Set(key, bytesValue)
		return
	})
	return err*/
}

// SetField sets any value to requested field
func (o *Object) SetField(objOrID interface{}, fieldName string, value interface{}) *Promise {
	field := o.field(fieldName)
	p := o.promise()
	p.do(func() Chain {
		bytesValue, err := field.ToBytes(value)
		if err != nil {
			return p.fail(err)
		}
		sub := o.Subspace(objOrID)
		key := sub.Pack(tuple.Tuple{field.Name})
		fieldGet := p.tr.Get(key)
		return func() Chain {
			val, err := fieldGet.Get()
			if err != nil {
				return p.fail(err)
			}
			if val == nil {
				return p.fail(ErrNotFound)
			}
			p.tr.Set(key, bytesValue)
			return p.done(nil)
		}
	})
	return p

	/*sub := o.Subspace(objOrID)
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
	return err*/
}

// Add writes data even in primary key is empty, by setting it. Take a look at autoincrement tag
func (o *Object) Add(data interface{}) *Promise {
	input := StructEditable(data)
	p := o.promise()
	p.do(func() Chain {
		for _, field := range o.fields {
			if field.AutoIncrement {
				incKey := o.miscDir.Pack(tuple.Tuple{"ai", field.Name})
				p.tr.Add(incKey, field.packed.Plus())
				autoIncrementValue := p.tr.Get(incKey).MustGet()
				input.Set(field, autoIncrementValue)
			}
		}

		primaryTuple := input.Primary(o)
		sub := o.primary.Sub(primaryTuple...)

		isSet := p.tr.GetKey(fdb.FirstGreaterThan(sub))
		return func() Chain {
			firstKey, err := isSet.Get()
			if err != nil {
				return p.fail(err)
			}
			if sub.Contains(firstKey) {
				return p.fail(ErrAlreadyExist)
			}

			err = o.doWrite(p.tr, sub, primaryTuple, input, false)
			if err != nil {
				return p.fail(err)
			}
			return nil
		}
	})
	return p

	/*_, err := o.db.Transact(func(tr fdb.Transaction) (ret interface{}, e error) {
		for _, field := range o.fields {
			if field.AutoIncrement {
				incKey := o.miscDir.Pack(tuple.Tuple{"ai", field.Name})
				tr.Add(incKey, field.packed.Plus())
				autoIncrementValue := tr.Get(incKey).MustGet()
				input.Set(field, autoIncrementValue)
			}
		}

		primaryTuple := input.Primary(o)
		sub := o.primary.Sub(primaryTuple...)

		isSet := tr.GetKey(fdb.FirstGreaterThan(sub))
		firstKey, err := isSet.Get()
		if err != nil {
			return nil, err
		}
		if sub.Contains(firstKey) {
			return nil, ErrAlreadyExist
		}

		e = o.doWrite(tr, sub, primaryTuple, input, false)
		return
	})
	if err != nil {
		return err
	}
	return nil*/
}

// Delete removes data
func (o *Object) Delete(objOrID interface{}) *Promise {
	//sub := o.Subspace(objOrID)
	primaryTuple := o.getPrimaryTuple(objOrID)
	sub := o.primary.Sub(primaryTuple...)

	p := o.promise()
	p.do(func() Chain {
		needed := o.need(p.tr, sub)
		//res := needObject(p.tr, sub)
		return func() Chain {
			value, err := needed.fetch()
			//value, err := o.valueFromRange(sub, res)
			if err != nil {
				return p.fail(err)
			}
			err = value.Err()
			if err != nil {
				return p.fail(err)
			}
			object := StructAny(value.Interface())

			// remove object key
			start, end := sub.FDBRangeKeys()
			p.tr.ClearRange(fdb.KeyRange{Begin: start, End: end})

			// remove indexes
			for _, index := range o.Indexes {
				index.Delete(p.tr, object)
			}

			return p.ok()
		}
	})
	return p

	/*_, err := o.db.Transact(func(tr fdb.Transaction) (ret interface{}, e error) {
		res := needObject(tr, sub)
		value, err := o.valueFromRange(sub, res)
		if err != nil {
			return nil, err
		}
		err = value.Err()
		if err != nil {
			return nil, err
		}
		object := StructAny(value.Interface())

		// remove object key
		start, end := sub.FDBRangeKeys()
		tr.ClearRange(fdb.KeyRange{Begin: start, End: end})

		// remove indexes
		for _, index := range o.Indexes {
			index.Delete(tr, object)
		}

		return
	})
	if err != nil {
		return err
	}
	return nil*/
}

// GetBy fetch one row using index bye name or name of the index field
func (o *Object) GetBy(indexKey string, data interface{}) *Promise {
	index, ok := o.Indexes[indexKey]
	if !ok {
		panic("Object " + o.name + ", index «" + indexKey + "» is undefined")
	}

	p := o.promise()
	p.doRead(func() Chain {
		sub, err := index.getPrimary(p.readTr, data)
		if err != nil {
			return p.fail(err)
		}

		start, end := sub.FDBRangeKeys()
		r := fdb.KeyRange{Begin: start, End: end}

		rangeGet := p.readTr.GetRange(r, fdb.RangeOptions{
			Mode: fdb.StreamingModeWantAll,
		})
		return func() Chain {
			rows, err := rangeGet.GetSliceWithError()
			if err != nil {
				return p.fail(err)
			}
			if len(rows) == 0 {
				return p.fail(ErrNotFound)
			}
			value := Value{
				object: o,
			}
			value.FromKeyValue(sub, rows)
			p.value = &value
			return p.done(nil)
		}
	})
	return p

	//var keysub subspace.Subspace
	/*resp, err := o.db.ReadTransact(func(tr fdb.ReadTransaction) (ret interface{}, e error) {
		sub, err := index.getPrimary(tr, data)
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
		if len(rows) == 0 {
			return nil, ErrNotFound
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
	return resp.(*Value)*/
}

// MultiGet fetch list of objects using primary id
func (o *Object) MultiGet(data interface{}) *Slice {
	dataKeys := o.sliceToKeys(data)
	resp, err := o.db.ReadTransact(func(tr fdb.ReadTransaction) (ret interface{}, e error) {
		needed := make([]*needObject, len(dataKeys))
		//needed := []*needObject{}
		for k, v := range dataKeys { // iterate each key
			//needed = append(needed, o.needSub(tr, v))
			needed[k] = o.need(tr, v)
		}
		return o.wrapRange(needed), nil
		/*kv, err := FetchRange(tr, needed)
		if err != nil {
			return nil, err
		}
		return o.wrapRange(kv, dataKeys), nil*/
	})
	if err != nil {
		return &Slice{err: err}
	}
	return resp.(*Slice)
}

func (o *Object) valueFromRange(sub subspace.Subspace, res fdb.RangeResult) (*Value, error) {
	rows, err := res.GetSliceWithError()
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, ErrNotFound
	}
	value := Value{
		object: o,
	}
	value.FromKeyValue(sub, rows)
	return &value, nil
}

// Get fetch object using primary id
func (o *Object) Get(objOrID interface{}) *Promise {
	p := o.promise()
	p.doRead(func() Chain {
		sub := o.getPrimarySub(objOrID)

		//needed := needObject(p.tr, sub)
		needed := o.need(p.readTr, sub)

		return func() Chain {
			var err error

			p.value, err = needed.fetch()
			if err != nil {
				return p.fail(err)
			}
			return p.done(nil)
		}
	})
	return p

	/*resp, err := o.db.ReadTransact(func(tr fdb.ReadTransaction) (ret interface{}, e error) {
		needed := o.need(tr, o.sub(o.getPrimaryTuple(objOrID)))
		return needed.fetch()
	})
	if err != nil {
		return &Value{err: err}
	}
	value := resp.(*Value)
	return value*/
}

func (o *Object) getKeyLimit(limit int) int {
	if limit == 0 {
		return 0
	}
	return limit * o.keysCount
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

// N2N Creates object to object relation between current object and other one.
// Other words it represents relations when unlimited number of host objects connected to unlimited
// amount of client objects
func (o *Object) N2N(client *Object) *Relation {
	rel := Relation{}
	rel.init(RelationN2N, o, client)
	return &rel
}

// Clear clears all info in object storage
func (o *Object) Clear() error {
	_, err := o.db.Transact(func(tr fdb.Transaction) (ret interface{}, e error) {
		start, end := o.dir.FDBRangeKeys()
		tr.ClearRange(fdb.KeyRange{Begin: start, End: end})
		start, end = o.miscDir.FDBRangeKeys()
		tr.ClearRange(fdb.KeyRange{Begin: start, End: end})
		start, end = o.primary.FDBRangeKeys()
		tr.ClearRange(fdb.KeyRange{Begin: start, End: end})
		for _, rel := range o.Relations {
			start, end = rel.hostDir.FDBRangeKeys()
			tr.ClearRange(fdb.KeyRange{Begin: start, End: end})

			start, end = rel.clientDir.FDBRangeKeys()
			tr.ClearRange(fdb.KeyRange{Begin: start, End: end})

			start, end = rel.infoDir.FDBRangeKeys()
			tr.ClearRange(fdb.KeyRange{Begin: start, End: end})
		}
		for _, index := range o.Indexes {
			start, end = index.dir.FDBRangeKeys()
			tr.ClearRange(fdb.KeyRange{Begin: start, End: end})
		}
		return
	})
	return err
}

func (o *Object) sub(key tuple.Tuple) subspace.Subspace {
	return o.primary.Sub(key...)
}

func (o *Object) need(tr fdb.ReadTransaction, sub subspace.Subspace) *needObject {
	needed := needObject{
		object:   o,
		subspace: sub,
	}
	needed.need(tr, sub)
	return &needed
}

// List queries list of items using primary key subspace. Pass no params if fetching all objects
func (o *Object) List(primary ...interface{}) *Query {
	query := Query{object: o}
	return query.List(primary...)
}

// ListAll queries list all items inside. Pass no params if fetching all objects
func (o *Object) ListAll() *Query {
	query := Query{object: o}
	return &query
}

// Use is an index selector for query building
func (o *Object) Use(index string) *Query {
	query := Query{object: o}
	return query.Use(index)
}
