package stored

import (
	"fmt"
	"math/rand"
	"reflect"
	"strings"

	"github.com/capturetechnologies/stored/packed"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/subspace"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
)

var typeOfBytes = reflect.TypeOf([]byte(nil))

// Field is main field structure
type Field struct {
	object        *Object
	Name          string
	Num           int
	Kind          reflect.Kind
	SubKind       reflect.Kind
	Type          reflect.StructField
	Value         reflect.Value
	mutable       bool
	primary       bool
	AutoIncrement bool
	GenID         GenIDType // type of ID autogeneration, IDDate, IDRandom
	packed        *packed.Packed
	UnStored      bool // means this field would not be stored inside main object
}

func (f *Field) init() {
	f.packed = packed.New(f.Value)
}

// Tag is general object for tag parsing
type Tag struct {
	Name          string
	Primary       bool
	mutable       bool
	unique        bool
	AutoIncrement bool
	UnStored      bool // means this field doesn't stored inside main object data

}

// ParseTag converts object stored tag to sturct with options
func (f *Field) ParseTag() *Tag {
	tagStr := f.Type.Tag.Get("stored")
	unstored := false
	if tagStr == "" {
		tagStr = f.Type.Tag.Get("unstored")
		if tagStr == "" {
			return nil
		}
		unstored = true
	}
	tagParts := strings.Split(tagStr, ",")
	tag := Tag{
		Name:     tagParts[0],
		UnStored: unstored,
	}
	if len(tagParts) > 1 {
		for i := 1; i < len(tagParts); i++ {
			part := strings.Trim(tagParts[i], " ")
			switch part {
			case "primary":
				tag.Primary = true
			case "mutable":
				tag.mutable = true
			case "unique":
				tag.unique = true
			case "autoincrement":
				tag.AutoIncrement = true
			default:
				panic("tag «" + tag.Name + "» has unsupported ‘" + part + "’ option")
			}
		}
	}
	return &tag
}

// GetDefault return default value for this field
func (f *Field) GetDefault() interface{} {
	switch f.Kind {
	case reflect.String:
		return ""
	case reflect.Int:
		return 0
	case reflect.Uint:
		return uint(0)
	case reflect.Int32:
		return int32(0)
	case reflect.Uint32:
		return uint32(0)
	case reflect.Int64:
		return int64(0)
	case reflect.Uint64:
		return uint64(0)
	default:
		panic("unsupported type for getdefault " + fmt.Sprintf("%v", f.Kind))
	}
}

// isEmpty checks if element is empty
func (f *Field) isEmpty(value interface{}) bool {
	if value == nil {
		return true
	}
	switch f.Kind { // slices and maps will be nil if not set
	case reflect.String:
		return "" == value
	case reflect.Int:
		return 0 == value
	case reflect.Int32:
		return int32(0) == value
	case reflect.Int64:
		return int64(0) == value
	case reflect.Uint:
		return uint(0) == value
	case reflect.Uint32:
		return uint32(0) == value
	case reflect.Uint64:
		return uint64(0) == value
	case reflect.Float32:
		return float32(0.0) == value
	case reflect.Float64:
		return float64(0.0) == value
	case reflect.Bool:
		return false == value
	default:
		return false
	}
}

// BytesFromObject return bytes using full object instead of field value
func (f *Field) BytesFromObject(objectValue interface{}) ([]byte, error) {
	object := reflect.ValueOf(objectValue)
	kind := object.Kind()
	if kind == reflect.Ptr {
		object = object.Elem()
		kind = object.Kind()
	}
	if kind != reflect.Struct {
		f.panic("object should be struct, not value")
	}
	fieldValue := object.Field(f.Num)
	return f.ToBytes(fieldValue.Interface())
}

// ToBytes packs interface field value to bytes
func (f *Field) ToBytes(val interface{}) ([]byte, error) {
	return f.packed.Encode(val)
}

func (f *Field) tupleElement(val interface{}) tuple.TupleElement {
	if f.Kind == reflect.Uint8 { // byte stored as byte array
		return []byte{val.(byte)}
	}
	return val
}

func (f *Field) setTupleValue(value reflect.Value, interfaceValue interface{}) {
	objField := value.Field(f.Num)
	switch objField.Kind() {
	case reflect.Int: // tuple store int as int64
		interfaceValue = int(interfaceValue.(int64))
	case reflect.Uint8:
		bytes := interfaceValue.([]uint8)
		if len(bytes) != 1 {
			panic("incorrect key tuple")
		}
		interfaceValue = bytes[0]
	case reflect.Uint64:
		interfaceValue = uint64(interfaceValue.(int64))
	}
	objField.Set(reflect.ValueOf(interfaceValue))
}

func (f *Field) getKey(sub subspace.Subspace) fdb.Key {
	return sub.Pack(tuple.Tuple{f.Name})
}

// ToInterface decodes field value
func (f *Field) ToInterface(obj []byte) interface{} {
	val := f.packed.DecodeToInterface(obj)
	return val
}

func (f *Field) panic(text string) {
	panic("field «" + f.Name + "» " + text)
}

// SetAutoIncrement checks if everything ok for autoincrements
func (f *Field) SetAutoIncrement() {
	switch f.Kind {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Uint32, reflect.Uint64:
		f.AutoIncrement = true
	default:
		f.panic("could not be autoincremented")
	}
}

// SetID sets unique id before the add write
func (f *Field) SetID(idType GenIDType) {
	switch f.Kind {
	case reflect.Int64, reflect.Uint64:
		f.GenID = idType
	default:
		f.panic("could not be autoincremented")
	}
}

// GenerateID will return ID bytes for type specified
func (f *Field) GenerateID() []byte {
	switch f.Kind {
	case reflect.Int64, reflect.Uint64:
		var id int64
		switch f.GenID {
		case GenIDDate:
			id = ID64()
			//id = time.Now().UnixNano()
			//id += rand.Int63n(1000000) - 500000
		case GenIDRandom:
			id = rand.Int63()
		default:
			f.panic(fmt.Sprintf("GenID in undefined: %d", f.GenID))
		}
		return Int64(id)
	}
	return []byte{}
}

func fieldsKey(fields []*Field) string {
	str := []string{}
	for _, f := range fields {
		str = append(str, f.Name)
	}
	return strings.Join(str, ",")
}
