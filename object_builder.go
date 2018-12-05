package stored

import "strconv"

// ObjectBuilder is main interface to declare objects
type ObjectBuilder struct {
	object *Object
}

func (ob *ObjectBuilder) panic(text string) {
	panic("Stored error, object «" + ob.object.name + "» declaration: " + text)
}

func (ob *ObjectBuilder) addIndex(indexKey string) *Index {
	o := ob.object
	_, ok := o.indexes[indexKey]
	if ok {
		ob.panic("already has index «" + indexKey + "»")
	}
	indexSubspace, err := o.dir.CreateOrOpen(o.db, []string{indexKey}, nil)
	if err != nil {
		panic(err)
	}
	index := Index{
		Name:   indexKey,
		object: o,
		dir:    indexSubspace,
	}
	o.indexes[indexKey] = &index
	return &index
}

func (ob *ObjectBuilder) addFieldIndex(fieldKey, indexKey string) *Index {
	field, ok := ob.object.fields[fieldKey]
	if !ok {
		ob.panic("has no key «" + fieldKey + "» could not set index")
	}
	index := ob.addIndex(indexKey)
	index.field = field
	return index
}

// Done will finish the object
func (ob *ObjectBuilder) Done() *Object {
	return ob.object
}

// Primary sets primary field in case it wasnot set with annotations
func (ob *ObjectBuilder) Primary(names ...string) *ObjectBuilder {
	for _, name := range names {
		_, ok := ob.object.fields[name]
		if !ok {
			ob.panic("has no key «" + name + "» could not set primar")
		}
	}
	ob.object.setPrimary(names...)
	return ob
}

// IDDate is unique id generated using date as first part, this approach is usefull
// if date index necessary too
// field type should be int64
func (ob *ObjectBuilder) IDDate(fieldName string) *ObjectBuilder {
	field, ok := ob.object.fields[fieldName]
	if !ok {
		ob.panic("has no key «" + fieldName + "» could not set uuid")
	}
	field.SetID(GenIDDate)
	return ob
}

// IDRandom is unique id generated using random number, this approach is usefull
// if you whant randomly distribute objects, and you do not whant to unveil data object
func (ob *ObjectBuilder) IDRandom(fieldName string) *ObjectBuilder {
	field, ok := ob.object.fields[fieldName]
	if !ok {
		ob.panic("no key «" + fieldName + "» could not set uuid")
	}
	field.SetID(GenIDRandom)
	return ob
}

// AutoIncrement make defined field autoincremented before adding new objects
//
func (ob *ObjectBuilder) AutoIncrement(name string) *ObjectBuilder {
	field, ok := ob.object.fields[name]
	if !ok {
		ob.panic("has no key «" + name + "» could not set autoincrement")
	}
	field.SetAutoIncrement()
	return ob
}

// Unique index: if object with same field value already presented, Set and Add will return an ErrAlreadyExist
func (ob *ObjectBuilder) Unique(key string) *ObjectBuilder {
	index := ob.addFieldIndex(key, key)
	index.Unique = true

	return ob
}

// Index add an simple index for specific key
func (ob *ObjectBuilder) Index(key string) *ObjectBuilder {
	ob.addFieldIndex(key, key)
	return ob
}

// IndexGeo will add and geohash based index to allow geographicly search objects
// geoPrecision 0 means full precision:
// 10 < 1m, 9 ~ 7.5m, 8 ~ 21m, 7 ~ 228m, 6 ~ 1.8km, 5 ~ 7.2km, 4 ~ 60km, 3 ~ 234km, 2 ~ 1890km, 1 ~ 7500km
func (ob *ObjectBuilder) IndexGeo(latKey string, longKey string, geoPrecision int) *IndexGeo {
	index := ob.addFieldIndex(latKey, latKey+","+longKey+":"+strconv.Itoa(geoPrecision))
	if geoPrecision < 1 || geoPrecision > 12 {
		geoPrecision = 12
	}
	index.Geo = geoPrecision
	field, ok := ob.object.fields[longKey]
	if !ok {
		ob.panic("has no key «" + longKey + "» could not set index")
	}
	index.secondary = field
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
	for _, fieldName := range fieldNames {
		field, ok := ob.object.fields[fieldName]
		if !ok {
			ob.panic("has no key «" + fieldName + "» could not set counter")
		}
		fields = append(fields, field)
	}
	return counterNew(ob.object, fields)
}

// N2N Creates object to object relation between current object and other one.
// Other words it represents relations when unlimited number of host objects connected to unlimited
// amount of client objects
func (ob *ObjectBuilder) N2N(client *ObjectBuilder) *Relation {
	rel := Relation{}
	rel.init(RelationN2N, ob.object, client.object)
	return &rel
}
