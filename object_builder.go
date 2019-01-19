package stored

import (
	"reflect"
	"strconv"
	"strings"
	"sync"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
)

// ObjectBuilder is main interface to declare objects
type ObjectBuilder struct {
	object   *Object
	waitInit sync.WaitGroup // waiter for main directory
	waitAll  sync.WaitGroup // waiter for all planned async operations
	mux      sync.Mutex
	schema   schemaFull
}

func (ob *ObjectBuilder) panic(text string) {
	panic("Stored error, object «" + ob.object.name + "» declaration: " + text)
}

func (ob *ObjectBuilder) buildSchema(schemaObj interface{}) {
	t := reflect.TypeOf(schemaObj)
	v := reflect.ValueOf(schemaObj)
	if v.Kind() == reflect.Ptr {
		t = t.Elem()
		v = v.Elem()
	}
	o := ob.object
	ob.mux.Lock()
	o.reflectType = t
	numfields := v.NumField()
	o.fields = map[string]*Field{}
	primaryFields := []string{}
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
				primaryFields = append(primaryFields, tag.Name)
				//o.setPrimary(tag.Name)
				//panic("not implemented yet")
			}
			if tag.mutable {
				field.mutable = true
			}
			if tag.UnStored {
				field.UnStored = true
			} else {
				o.keysCount++
			}
		}
	}
	ob.mux.Unlock()
	if len(primaryFields) > 0 {
		ob.Primary(primaryFields...)
	}
	return
}

func (ob *ObjectBuilder) addIndex(indexKey string) *Index {
	o := ob.object
	ob.mux.Lock()
	_, ok := o.indexes[indexKey]
	ob.mux.Unlock()
	if ok {
		ob.panic("already has index «" + indexKey + "»")
	}

	index := Index{
		Name:   indexKey,
		object: o,
	}

	ob.waitAll.Add(1)
	//fmt.Println("all(3) +1")
	go func() {
		ob.waitInit.Wait()
		indexSubspace, err := o.dir.CreateOrOpen(o.db, []string{indexKey}, nil)
		if err != nil {
			panic(err)
		}
		index.dir = indexSubspace
		ob.mux.Lock()
		o.indexes[indexKey] = &index
		ob.mux.Unlock()
		//fmt.Println("all(3) -1")
		ob.waitAll.Done()
	}()

	return &index
}

func (ob *ObjectBuilder) addFieldIndex(fieldKeys []string) *Index {
	ob.mux.Lock()

	fields := make([]*Field, len(fieldKeys))
	for k, keyName := range fieldKeys {
		field, ok := ob.object.fields[keyName]
		if !ok {
			ob.panic("has no key «" + keyName + "» could not set index")
		}
		fields[k] = field
	}
	ob.mux.Unlock()

	index := ob.addIndex(strings.Join(fieldKeys, ","))
	index.fields = fields
	return index
}

func (ob *ObjectBuilder) addGeoIndex(latKey, longKey, indexKey string) *Index {
	ob.mux.Lock()
	latField, ok := ob.object.fields[latKey]
	if !ok {
		ob.panic("has no key «" + latKey + "» could not set index")
	}
	longField, ok := ob.object.fields[longKey]
	if !ok {
		ob.panic("has no key «" + longKey + "» could not set index")
	}
	ob.mux.Unlock()

	index := ob.addIndex(indexKey)
	//index.field = field
	index.fields = []*Field{latField, longField}
	return index
}

func (ob *ObjectBuilder) need() {
	o := ob.object
	o.init()
	res, err := o.db.ReadTransact(func(tr fdb.ReadTransaction) (interface{}, error) {
		schema := schemaFull{}
		schema.load(ob, o.miscDir, tr)
		return schema, nil
	})
	if err != nil {
		ob.panic("could not read schema")
	}
	ob.mux.Lock()
	ob.schema = res.(schemaFull)
	ob.mux.Unlock()
	//fmt.Println("init -1")
	ob.waitInit.Done()
	//fmt.Println("all -1")
	ob.waitAll.Done()
}

// Done will finish the object
func (ob *ObjectBuilder) Done() *Object {
	ob.waitAll.Wait()
	ob.schema.buildCurrent(ob)
	return ob.object
}

// Primary sets primary field in case it wasnot set with annotations
func (ob *ObjectBuilder) Primary(names ...string) *ObjectBuilder {
	ob.mux.Lock()
	for _, name := range names {
		_, ok := ob.object.fields[name]
		if !ok {
			ob.panic("has no key «" + name + "» could not set primar")
		}
	}
	//ob.object.setPrimary(names...)
	o := ob.object
	var name string
	if len(names) == 1 {
		name = names[0]
	}
	if o.primaryKey != "" {
		for k, name := range names {
			if o.primaryFields[k].Name != name {
				o.panic("primary key already set to «" + o.primaryKey + "», could not set to «" + strings.Join(names, ", ") + "»")
			}
		}
		o.panic("primary key already set to «" + o.primaryKey + "», could not set to «" + name + "»")
	}

	if len(names) > 1 {
		o.primaryFields = []*Field{}
		for _, name := range names {
			field := o.fields[name]
			field.primary = true
			o.primaryFields = append(o.primaryFields, field)
		}
		o.primaryKey = names[0]
		o.multiplePrimary = true
	} else {
		o.primaryKey = name
		field := o.fields[name]
		field.primary = true
		o.primaryFields = []*Field{field}
	}

	ob.mux.Unlock()
	ob.waitAll.Add(1)
	//fmt.Println("all +1")
	go func() {
		ob.waitInit.Wait()
		var err error
		o.primary, err = o.dir.CreateOrOpen(o.db, names, nil)
		if err != nil {
			ob.panic(err.Error())
		}
		ob.waitAll.Done()
		//fmt.Println("all -1")
	}()
	return ob
}

// IDDate is unique id generated using date as first part, this approach is usefull
// if date index necessary too
// field type should be int64
func (ob *ObjectBuilder) IDDate(fieldName string) *ObjectBuilder {
	ob.mux.Lock()
	field, ok := ob.object.fields[fieldName]
	if !ok {
		ob.panic("has no key «" + fieldName + "» could not set uuid")
	}
	ob.mux.Unlock()
	field.SetID(GenIDDate)
	return ob
}

// IDRandom is unique id generated using random number, this approach is usefull
// if you whant randomly distribute objects, and you do not whant to unveil data object
func (ob *ObjectBuilder) IDRandom(fieldName string) *ObjectBuilder {
	ob.mux.Lock()
	field, ok := ob.object.fields[fieldName]
	if !ok {
		ob.panic("no key «" + fieldName + "» could not set uuid")
	}
	field.SetID(GenIDRandom)
	ob.mux.Unlock()
	return ob
}

// AutoIncrement make defined field autoincremented before adding new objects
//
func (ob *ObjectBuilder) AutoIncrement(name string) *ObjectBuilder {
	ob.mux.Lock()
	field, ok := ob.object.fields[name]
	if !ok {
		ob.panic("has no key «" + name + "» could not set autoincrement")
	}
	field.SetAutoIncrement()
	ob.mux.Unlock()
	return ob
}

// Unique index: if object with same field value already presented, Set and Add will return an ErrAlreadyExist
func (ob *ObjectBuilder) Unique(names ...string) *ObjectBuilder {
	index := ob.addFieldIndex(names)
	index.Unique = true

	return ob
}

// Index add an simple index for specific key or set of keys
func (ob *ObjectBuilder) Index(names ...string) *ObjectBuilder {
	ob.addFieldIndex(names)
	return ob
}

// IndexOptional is the simple index which will be written only if field is not empty
func (ob *ObjectBuilder) IndexOptional(names ...string) *ObjectBuilder {
	index := ob.addFieldIndex(names)
	index.optional = true
	return ob
}

// FastIndex will set index storing copy of object, performing denormalisation
func (ob *ObjectBuilder) FastIndex(names ...string) *ObjectBuilder {
	ob.mux.Lock()
	for _, name := range names {
		_, ok := ob.object.fields[name]
		if !ok {
			ob.panic("has no key «" + name + "» could not set primar")
		}
	}
	ob.mux.Unlock()
	// init fast index here
	return ob
}

// IndexGeo will add and geohash based index to allow geographicly search objects
// geoPrecision 0 means full precision:
// 10 < 1m, 9 ~ 7.5m, 8 ~ 21m, 7 ~ 228m, 6 ~ 1.8km, 5 ~ 7.2km, 4 ~ 60km, 3 ~ 234km, 2 ~ 1890km, 1 ~ 7500km
func (ob *ObjectBuilder) IndexGeo(latKey string, longKey string, geoPrecision int) *IndexGeo {
	index := ob.addGeoIndex(latKey, longKey, latKey+","+longKey+":"+strconv.Itoa(geoPrecision))
	if geoPrecision < 1 || geoPrecision > 12 {
		geoPrecision = 12
	}
	index.Geo = geoPrecision
	return &IndexGeo{index: index}
}

// IndexCustom add an custom index generated dynamicly using callback function
// custom indexes in an general way to implement any index on top of it
func (ob *ObjectBuilder) IndexCustom(key string, cb func(object interface{}) Key) *Index {
	index := ob.addIndex(key)
	index.handle = cb
	return index
}

// Counter will count all objects with same value of passed fields
func (ob *ObjectBuilder) Counter(fieldNames ...string) *Counter {
	fields := []*Field{}
	ob.mux.Lock()
	for _, fieldName := range fieldNames {
		field, ok := ob.object.fields[fieldName]
		if !ok {
			ob.panic("has no key «" + fieldName + "» could not set counter")
		}
		fields = append(fields, field)
	}
	ob.mux.Unlock()
	return counterNew(ob, fields)
}

// N2N Creates object to object relation between current object and other one.
// Other words it represents relations when unlimited number of host objects connected to unlimited
// amount of client objects
func (ob *ObjectBuilder) N2N(client *ObjectBuilder) *Relation {
	rel := Relation{}
	rel.init(RelationN2N, ob.object, client.object)
	return &rel
}
