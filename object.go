package stored

import (
	"fmt"
	"reflect"
	"strings"

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
	indexes         map[string]*Index
	counters        map[string]*Counter
	Relations       []*Relation
	keysCount       int
}

func (o *Object) init() {
	var err error
	o.dir, err = o.directory.Subspace.CreateOrOpen(o.db, []string{o.name}, nil)
	if err != nil {
		panic(err)
	}
	o.miscDir, err = o.dir.CreateOrOpen(o.db, []string{"misc"}, nil)
	if err != nil {
		panic(err)
	}
}

func (o *Object) editSlice(sliceObjectPtr interface{}) []*Struct {
	value := reflect.ValueOf(sliceObjectPtr)
	if value.Kind() != reflect.Slice {
		panic("should be slice")
	}

	values := []*Struct{}

	for i := 0; i < value.Len(); i++ {
		row := value.Index(i)
		values = append(values, &Struct{
			value:    row.Elem(),
			editable: true,
		})
	}
	return values
}

func (o *Object) wrapRange(needed []*needObject) *Slice {
	if len(needed) == 0 {
		return &Slice{values: []*Value{}} // empty slice instead of error
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

func (o *Object) panic(text string) {
	panic("Stored error, object " + o.name + ": " + text)
}

func (o *Object) log(text string) {
	fmt.Println("Object «" + o.name + "»: " + text)
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

// getPrimaryField return primary field of an STORED object
func (o *Object) getPrimaryField() *Field {
	if o.primaryKey == "" {
		panic("Object " + o.name + " has no primary key")
	}
	field, ok := o.fields[o.primaryKey]
	if !ok {
		panic("Object " + o.name + " has invalid primary field")
	}
	return field
}

func (o *Object) doWrite(tr fdb.Transaction, sub subspace.Subspace, primaryTuple tuple.Tuple, input, oldObject *Struct, addNew bool) error {
	if addNew {
		for _, ctr := range o.counters {
			ctr.increment(tr, input)
		}
	} else { // remove previous data
		start, end := o.primary.Sub(primaryTuple...).FDBRangeKeys()
		tr.ClearRange(fdb.KeyRange{Begin: start, End: end})
	}

	fieldsWriten := 0
	for _, field := range o.fields {
		if field.UnStored {
			continue
		}
		// primary fields should not be stored inside the object,
		// because could be extraced from the key
		if field.primary {
			continue
		}

		value := input.GetBytes(field)
		tr.Set(field.getKey(sub), value)
		fieldsWriten++
	}
	if fieldsWriten == 0 {
		// write empty field to be shure that object will be written
		tr.Set(sub.FDBKey(), []byte{})
	}

	for _, index := range o.indexes {
		//fmt.Println("WRITE index", primaryTuple, input)
		err := index.Write(tr, primaryTuple, input, oldObject)
		if err != nil {
			fmt.Println("INDEX WRITE ERROR", index.Name, err)
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

func (o *Object) promiseSlice() *PromiseSlice {
	return &PromiseSlice{
		Promise: Promise{
			db: o.db,
		},
		limit: 100,
	}
}

func (o *Object) promiseErr() *PromiseErr {
	return &PromiseErr{
		Promise{
			db: o.db,
		},
	}
}

func (o *Object) promiseValue() *PromiseValue {
	return &PromiseValue{
		Promise{
			db: o.db,
		},
	}
}

func (o *Object) promiseInt64() *Promise {
	return &Promise{
		db: o.db,
	}
}

// Write writes data, only if row already exist
func (o *Object) Write(data interface{}) *PromiseErr {
	input := structAny(data)
	p := o.promiseErr()
	p.do(func() Chain {
		primaryTuple := input.getPrimary(o)

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
			object := structAny(value.Interface())

			err = o.doWrite(p.tr, sub, primaryTuple, input, object, false)
			if err != nil {
				return p.fail(err)
			}
			return p.ok()
		}
	})

	return p
}

// Update writes data, only if row already exist
func (o *Object) Update(data interface{}, callback func() error) *PromiseErr {
	input := structEditable(data)
	p := o.promiseErr()
	p.do(func() Chain {
		//primaryTuple := o.getPrimaryTuple(objOrID)
		//sub := o.primary.Sub(primaryTuple...)
		primaryTuple := input.getPrimary(o)

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
			input.Fill(o, value)
			err = value.Err()
			if err != nil {
				return p.fail(err)
			}
			prevInterface := value.Interface()
			oldObject := structAny(prevInterface)
			err = callback()
			if err != nil {
				return p.fail(err)
			}
			err = o.doWrite(p.tr, sub, primaryTuple, input, oldObject, false)
			if err != nil {
				return p.fail(err)
			}
			return p.ok()
		}
	})
	return p
}

// Set writes data, would return error if primary key is empty
func (o *Object) Set(objectPtr interface{}) *PromiseErr {
	input := structAny(objectPtr)
	p := o.promiseErr()
	p.do(func() Chain {
		primaryTuple := input.getPrimary(o)

		// delete all indexes data
		sub := o.sub(primaryTuple)
		needed := o.need(p.tr, sub)
		//res := needObject(p.tr, sub)
		return func() Chain {
			value, err := needed.fetch()
			addNew := false
			var oldObject *Struct
			if err != ErrNotFound {
				if err != nil {
					return p.fail(err)
				}
				err = value.Err()
				if err != nil {
					return p.fail(err)
				}
				oldObject = structAny(value.Interface())
			} else {
				addNew = true
			}
			err = o.doWrite(p.tr, sub, primaryTuple, input, oldObject, addNew)
			if err != nil {
				p.fail(err)
			}
			return p.ok()
		}
	})

	return p
}

// Subspace return subspace using object or interface
func (o *Object) subspace(objOrID interface{}) subspace.Subspace {
	primaryTuple := o.getPrimaryTuple(objOrID)
	return o.primary.Sub(primaryTuple...)
}

// IncFieldUnsafe increment field  of an object
// does not implement indexes in the moment
// would not increment field of passed object, take care
func (o *Object) IncFieldUnsafe(objOrID interface{}, fieldName string, incVal interface{}) *PromiseErr {
	field := o.field(fieldName)
	//primaryID := o.GetPrimaryField().fromAnyInterface(objOrID)
	primaryTuple := o.getPrimaryTuple(objOrID)
	sub := o.primary.Sub(primaryTuple...)
	p := o.promiseErr()
	p.do(func() Chain {
		incKey := sub.Pack(tuple.Tuple{field.Name})
		val, err := field.ToBytes(incVal)
		if err != nil {
			return p.fail(err)
		}
		p.tr.Add(incKey, val)
		return p.ok()
	})
	return p
}

// IncGetField increment field and return new value
// moved to IncFieldAtomic
func (o *Object) IncGetField(objOrID interface{}, fieldName string, incVal interface{}) *Promise {
	field := o.field(fieldName)

	p := o.promise()
	p.do(func() Chain {
		sub := o.subspace(objOrID)
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
			return p.done(p.getValueField(o, field, bytes))
		}
	})
	return p
}

// UpdateField updates object field via callback with old value
// moved to ChangeField
func (o *Object) UpdateField(objOrID interface{}, fieldName string, callback func(value interface{}) (interface{}, error)) *Promise {
	field := o.field(fieldName)

	p := o.promise()
	p.do(func() Chain {
		sub := o.subspace(objOrID)
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
}

// SetField sets any value to requested field
func (o *Object) SetField(objectPtr interface{}, fieldName string) *PromiseErr {
	input := structAny(objectPtr)
	field := o.field(fieldName)
	p := o.promiseErr()
	p.do(func() Chain {
		bytesValue := input.GetBytes(field)
		//sub := input.getSubspace(o)
		primaryTuple := input.getPrimary(o)
		sub := o.sub(primaryTuple)

		key := sub.Pack(tuple.Tuple{field.Name})

		//isSet := p.tr.GetKey(fdb.FirstGreaterThan(sub))
		needed := o.need(p.tr, sub)

		return func() Chain { // better to get first key
			/*firstKey, err := isSet.Get()
			if err != nil {
				return p.fail(err)
			}
			if !sub.Contains(firstKey) {
				return p.fail(ErrNotFound)
			}*/
			value, err := needed.fetch()
			if err != nil {
				return p.fail(err)
			}
			var oldObject *Struct
			oldObject = structAny(value.Interface())

			p.tr.Set(key, bytesValue)

			for _, index := range o.indexes {
				for _, indexField := range index.fields {
					if indexField == field {
						fmt.Println("set field write index")
						index.Write(p.tr, primaryTuple, input, oldObject)
						break
					}
				}
			}
			return p.ok()
		}
	})
	return p
}

// Add writes data even in primary key is empty, by setting it. Take a look at autoincrement tag
func (o *Object) Add(data interface{}) *PromiseErr {
	input := structEditable(data)
	p := o.promiseErr()
	p.do(func() Chain {
		for _, field := range o.fields {
			if field.AutoIncrement {
				incKey := o.miscDir.Pack(tuple.Tuple{"ai", field.Name})
				p.tr.Add(incKey, field.packed.Plus())
				autoIncrementValue := p.tr.Get(incKey).MustGet()
				input.setField(field, autoIncrementValue)
			} else if field.GenID != 0 {
				input.setField(field, field.GenerateID())
			}
		}

		primaryTuple := input.getPrimary(o)
		sub := o.primary.Sub(primaryTuple...)

		isSet := p.tr.GetKey(fdb.FirstGreaterThan(sub))
		return func() Chain {
			firstKey, err := isSet.Get()
			if err != nil {
				return p.fail(err)
			}
			if sub.Contains(firstKey) {
				fmt.Println("ALREADY EXIST PRIMARY KEY", firstKey)
				return p.fail(ErrAlreadyExist)
			}

			err = o.doWrite(p.tr, sub, primaryTuple, input, nil, true)
			if err != nil {
				return p.fail(err)
			}
			return p.ok()
		}
	})
	return p
}

// Delete removes data
func (o *Object) Delete(objOrID interface{}) *PromiseErr {
	//sub := o.Subspace(objOrID)
	primaryTuple := o.getPrimaryTuple(objOrID)
	sub := o.primary.Sub(primaryTuple...)

	p := o.promiseErr()
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
			object := structAny(value.Interface())

			// remove object key
			start, end := sub.FDBRangeKeys()
			p.tr.ClearRange(fdb.KeyRange{Begin: start, End: end})

			// remove indexes
			for _, index := range o.indexes {
				toDelete := index.getKey(object)
				index.Delete(p.tr, primaryTuple, toDelete)
			}

			for _, ctr := range o.counters {
				ctr.decrement(p.tr, object)
			}

			return p.ok()
		}
	})
	return p
}

// GetBy fetch one row using index bye name or name of the index field
func (o *Object) GetBy(objectPtr interface{}, indexKeys ...string) *PromiseErr {
	input := structEditable(objectPtr)
	indexKey := strings.Join(indexKeys, ",")
	index, ok := o.indexes[indexKey]
	if !ok {
		o.panic("index «" + indexKey + "» is undefined")
	}

	p := o.promiseErr()
	p.doRead(func() Chain {
		sub, err := index.getPrimary(p.readTr, index.getKey(input))
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
			input.Fill(o, &value)
			return p.done(nil)
		}
	})
	return p
}

// MultiGet fetch list of objects using primary id
func (o *Object) MultiGet(sliceObjectPtr interface{}) *PromiseErr {
	inputs := o.editSlice(sliceObjectPtr)
	p := o.promiseErr()
	p.doRead(func() Chain {
		needed := map[int]*needObject{}
		for k, input := range inputs {
			needed[k] = o.need(p.readTr, input.getSubspace(o))
		}
		for k, input := range inputs {
			value, err := needed[k].fetch()
			if err != nil {
				fmt.Println("could not fetch user", err)
				continue
				//return p.fail(err)
			}
			input.Fill(o, value)
		}
		return p.done(nil)
	})
	return p
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
func (o *Object) Get(objectPtr interface{}) *PromiseErr {
	input := structEditable(objectPtr)
	p := o.promiseErr()
	p.doRead(func() Chain {
		needed := o.need(p.readTr, input.getSubspace(o))
		return func() Chain {
			res, err := needed.fetch()
			if err != nil {
				return p.fail(err)
			}
			input.Fill(o, res)
			return p.done(nil)
		}
	})
	return p
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
	primaryField := o.getPrimaryField()
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

// Clear clears all info in object storage
func (o *Object) Clear() error {
	_, err := o.db.Transact(func(tr fdb.Transaction) (ret interface{}, e error) {
		start, end := o.dir.FDBRangeKeys()
		tr.ClearRange(fdb.KeyRange{Begin: start, End: end})
		start, end = o.miscDir.FDBRangeKeys()
		tr.ClearRange(fdb.KeyRange{Begin: start, End: end})
		if o.primary != nil {
			start, end = o.primary.FDBRangeKeys()
			tr.ClearRange(fdb.KeyRange{Begin: start, End: end})
		}
		for _, rel := range o.Relations {
			start, end = rel.hostDir.FDBRangeKeys()
			tr.ClearRange(fdb.KeyRange{Begin: start, End: end})

			start, end = rel.clientDir.FDBRangeKeys()
			tr.ClearRange(fdb.KeyRange{Begin: start, End: end})

			start, end = rel.infoDir.FDBRangeKeys()
			tr.ClearRange(fdb.KeyRange{Begin: start, End: end})
		}
		for _, index := range o.indexes {
			start, end = index.dir.FDBRangeKeys()
			tr.ClearRange(fdb.KeyRange{Begin: start, End: end})
		}
		for _, counter := range o.counters {
			start, end = counter.dir.FDBRangeKeys()
			tr.ClearRange(fdb.KeyRange{Begin: start, End: end})
		}
		return
	})
	return err
}

// ClearAllIndexes clears all indexes data
func (o *Object) ClearAllIndexes() error {
	_, err := o.db.Transact(func(tr fdb.Transaction) (ret interface{}, e error) {
		for _, index := range o.indexes {
			index.doClearAll(tr)
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
func (o *Object) Use(indexFieldNames ...string) *Query {
	query := Query{object: o}
	return query.Use(indexFieldNames...)
}

// Reindex will go around all data and delete add every row
func (o *Object) Reindex() {
	query := o.ListAll().Limit(100)
	num := 0
	stop := false
	for query.Next() {
		query.Slice().Each(func(item interface{}) {
			num++
			if stop {
				return
			}
			err := o.Delete(item).Err()
			if err != nil {
				fmt.Println("DELETEED, err", err)
				stop = true
			} else {
				err = o.Set(item).Err()
				if err != nil {
					fmt.Println("PUSHED BACK, err", err)
					stop = true
				}
			}
		})
	}
}
