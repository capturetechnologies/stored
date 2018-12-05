package stored

/*
import (
	"reflect"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
)

// Database is main representation for inited database
type Database struct {
	// individual subset of objects because in future one database could be build as
	// join of several directories
	cluster *Cluster
	objects map[string]*Object
	types   map[reflect.Type]*Object
}

func (d *Database) init() {
	// checking that eveything set up correctly
	if d.objects == nil {
		panic("Database was not build properly")
	}
	d.types = map[reflect.Type]*Object{}
	for _, obj := range d.objects {
		if obj.primaryKey == "" {
			obj.panic("no primary key setup")
		}
		d.types[obj.reflectType] = obj
	}
}

func (d *Database) edit(objectPtr interface{}) (*Struct, *Object) {
	value := structEditable(objectPtr)
	valType := value.getType()
	obj, ok := d.types[valType]
	if !ok {
		panic("object of type «" + valType.String() + "» not found")
	}
	return value, obj
}

func (d *Database) read(objectPtr interface{}) (*Struct, *Object) {
	value := structAny(objectPtr)
	valType := value.getType()
	obj, ok := d.types[valType]
	if !ok {
		panic("object of type «" + valType.String() + "» not found")
	}
	return value, obj
}

func (d *Database) editSlice(sliceObjectPtr interface{}) ([]*Struct, *Object) {
	value := reflect.ValueOf(sliceObjectPtr)
	if value.Kind() != reflect.Slice {
		panic("should be slice")
	}
	elem := value.Type().Elem()
	subKind := elem.Kind()
	if subKind != reflect.Ptr {
		panic("should be slice of pointers, slice of data passed")
	}
	valType := elem.Elem()
	obj, ok := d.types[valType]
	if !ok {
		panic("object of type «" + valType.String() + "» not found")
	}
	values := []*Struct{}

	for i := 0; i < value.Len(); i++ {
		row := value.Index(i)
		values = append(values, &Struct{
			value:    row.Elem(),
			editable: true,
		})
	}
	return values, obj
}

// Get will fetch object from database filling all the field of passed struct
// Primary fields should be already filled to fetch the object
func (d *Database) Get(objectPtr interface{}) *PromiseErr {
	input, obj := d.edit(objectPtr)

	p := obj.promiseErr()
	p.doRead(func() Chain {
		needed := obj.need(p.readTr, input.getSubspace(obj))
		return func() Chain {
			res, err := needed.fetch()
			if err != nil {
				return p.fail(err)
			}
			input.Fill(obj, res)
			return p.done(nil)
		}
	})
	return p
}

// Set will write data passed to the object
func (d *Database) Set(objectPtr interface{}) *PromiseErr {
	input, obj := d.read(objectPtr)

	p := obj.promiseErr()
	p.do(func() Chain {
		primaryTuple := input.getPrimary(obj)

		// delete all indexes data
		sub := obj.sub(primaryTuple)
		needed := obj.need(p.tr, sub)
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

			err = obj.doWrite(p.tr, sub, primaryTuple, input, oldObject, addNew)
			if err != nil {
				p.fail(err)
			}
			return p.done(nil)
		}
	})
	return p
}

// Add will add object implementing default setup for primary values
func (d *Database) Add(objectPtr interface{}) *PromiseErr {
	input, obj := d.edit(objectPtr)

	p := obj.promiseErr()
	p.do(func() Chain {
		for _, field := range obj.fields {
			if field.AutoIncrement {
				incKey := obj.miscDir.Pack(tuple.Tuple{"ai", field.Name})
				p.tr.Add(incKey, field.packed.Plus())
				autoIncrementValue := p.tr.Get(incKey).MustGet()
				input.setField(field, autoIncrementValue)
			} else if field.GenID != 0 {
				input.setField(field, field.GenerateID())
			}
		}

		primaryTuple := input.getPrimary(obj)
		sub := obj.sub(primaryTuple)

		isSet := p.tr.GetKey(fdb.FirstGreaterThan(sub))
		return func() Chain {
			firstKey, err := isSet.Get()
			if err != nil {
				return p.fail(err)
			}
			if sub.Contains(firstKey) {
				return p.fail(ErrAlreadyExist)
			}

			err = obj.doWrite(p.tr, sub, primaryTuple, input, nil, true)
			if err != nil {
				p.fail(err)
			}
			return p.done(nil)
		}
	})
	return p
}

// Update will set data to the object only if object exist
// return error otherwise
func (d *Database) Update(objectPtr interface{}) *PromiseErr {
	input, obj := d.read(objectPtr)

	p := obj.promiseErr()
	p.do(func() Chain {
		primaryTuple := input.getPrimary(obj)

		// delete all indexes data
		sub := obj.sub(primaryTuple)
		needed := obj.need(p.tr, sub)
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

			err = obj.doWrite(p.tr, sub, primaryTuple, input, object, false)
			if err != nil {
				return p.fail(err)
			}
			return p.done(nil)
		}
	})

	return p
}

// IncFieldUnsafe increment field the fastest way
// this is Unsafe method, you should be sure that object exist
// othewise an abandon field will be created, providing data pollition
// also this method does not implement indexes
func (d *Database) IncFieldUnsafe(objectPtr interface{}, fieldName string, incVal interface{}) *PromiseErr {
	input, obj := d.read(objectPtr)

	p := obj.promiseErr()
	p.do(func() Chain {
		sub := input.getSubspace(obj)
		field := obj.field(fieldName)
		if !field.mutable {
			obj.panic("field " + fieldName + " should be mutable to perform IncFieldUnsafe")
		}
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

// IncField will increment field with all the checks and reindexing
// Only mutable mode done at the moment, also need full mode in case
// if field is not mutable or an custom index applied (means any
// change of the object could tigger) index update
func (d *Database) IncField(objectPtr interface{}, fieldName string, incVal interface{}) *PromiseErr {
	input, obj := d.edit(objectPtr)
	p := obj.promiseErr()
	p.do(func() Chain {
		field := obj.field(fieldName)
		if !field.mutable {
			obj.panic("field " + fieldName + " should be mutable to perform IncFieldUnsafe")
		}

		primaryTuple := input.getPrimary(obj)
		sub := obj.sub(primaryTuple)
		//needed := obj.need(p.tr, sub)
		incKey := p.tr.Get(field.getKey(sub))
		return func() Chain {
			binaryFieldVal, err := incKey.Get()
			if err != nil {
				return p.fail(err)
			}
			if binaryFieldVal == nil {
				return p.fail(ErrNotFound)
			}
			input.setField(field, binaryFieldVal)
			incKey := sub.Pack(tuple.Tuple{field.Name})
			incValBytes, err := field.ToBytes(incVal)
			if err != nil {
				return p.fail(err)
			}
			p.tr.Add(incKey, incValBytes)
			input.incField(field, incVal)
			return p.done(nil)
		}

	})
	return p
}

// ChangeField will fetch field from db, change its content via callback function and write back
// passed object will be updated as well
func (d *Database) ChangeField(objectPtr interface{}, fieldName string, callback func(value interface{}) (interface{}, error)) *PromiseErr {
	input, obj := d.edit(objectPtr)
	field := obj.field(fieldName)

	p := obj.promiseErr()
	p.do(func() Chain {
		sub := input.getSubspace(obj)
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
			input.setField(field, bytesValue)
			return p.done(nil)
		}
	})
	return p
}

// SetField field will write new field data from the object passed
func (d *Database) SetField(objectPtr interface{}, fieldName string) *PromiseErr {
	input, obj := d.read(objectPtr)
	field := obj.field(fieldName)
	p := obj.promiseErr()
	p.do(func() Chain {
		bytesValue := input.GetBytes(field)
		sub := input.getSubspace(obj)
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
}

// Delete will delete the object
func (d *Database) Delete(objectPtr interface{}) *PromiseErr {
	input, obj := d.read(objectPtr)

	primaryTuple := input.getPrimary(obj)
	sub := obj.primary.Sub(primaryTuple...)
	p := obj.promiseErr()
	p.do(func() Chain {
		needed := obj.need(p.tr, sub)
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
			for _, index := range obj.indexes {
				toDelete := index.getKey(object)
				index.Delete(p.tr, primaryTuple, toDelete)
			}

			for _, ctr := range obj.counters {
				ctr.decrement(p.tr, object)
			}

			return p.ok()
		}
	})
	return p
}

// GetBy will perform object fetch using index
func (d *Database) GetBy(objectPtr interface{}, indexKey string) *PromiseErr {
	input, obj := d.edit(objectPtr)

	index, ok := obj.indexes[indexKey]
	if !ok {
		obj.panic("index «" + indexKey + "» is undefined")
	}

	p := obj.promiseErr()
	p.doRead(func() Chain {
		data := input.Get(index.field)
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
				object: obj,
			}
			value.FromKeyValue(sub, rows)
			input.Fill(obj, &value)
			return p.done(nil)
		}
	})
	return p
}

// MultiGet will fill the slice of objects by pointers
func (d *Database) MultiGet(sliceObjectPtr interface{}) *PromiseErr {
	inputs, obj := d.editSlice(sliceObjectPtr)
	p := obj.promiseErr()
	p.doRead(func() Chain {
		needed := map[int]*needObject{}
		for k, input := range inputs {
			needed[k] = obj.need(p.readTr, input.getSubspace(obj))
		}
		for k, input := range inputs {
			value, err := needed[k].fetch()
			if err != nil {
				return p.fail(err)
			}
			input.Fill(obj, value)
		}
		return p.done(nil)
	})
	return p
}

// List will fetch list of rows
func (d *Database) List(objectPtr interface{}, fieldNames ...string) *Query {
	input, obj := d.read(objectPtr)
	var primary tuple.Tuple
	numFields := len(fieldNames)
	if numFields == 0 { // no fields passed, use all
		primary = input.getPrimary(obj)
	} else {
		fields := []*Field{}

		for k, field := range obj.primaryFields {
			if numFields == 0 {
				break
			}
			fieldName := fieldNames[k]
			if fieldName != field.Name {
				obj.panic("field «" + fieldName + "» not primary in that order, should be «" + field.Name + "»")
			}
			fields = append(fields, field)
			numFields--
		}
		primary = input.getTuple(fields)
	}
	query := Query{
		object:  obj,
		primary: primary,
	}
	return &query
}

// ListAll will return wuery with all the objects
func (d *Database) ListAll(objectPtr interface{}) *Query {
	_, obj := d.read(objectPtr)
	query := Query{object: obj}
	return &query
}

// Use is an index selector for query building
func (d *Database) Use(objectPtr interface{}, index string) *Query {
	_, obj := d.read(objectPtr)
	query := Query{object: obj}
	return query.Use(index)
}

// Multi creates reference object for multi requests
func (d *Database) Multi() *MultiChain {
	mc := MultiChain{db: d.cluster.db}
	mc.init()
	return &mc
}

// Clear removes all content inside directory
func (d *Database) Clear() error {
	_, err := d.cluster.db.Transact(func(tr fdb.Transaction) (ret interface{}, e error) {
		for _, obj := range d.objects {
			err := obj.Clear()
			if err != nil {
				return nil, err
			}
		}
		return
	})
	if err != nil {
		return err
	}
	return nil
}*/
