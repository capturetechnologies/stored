package stored

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/capturetechnologies/stored/packed"

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
	AutoIncrement bool
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
	Mutable       bool
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
				tag.Mutable = true
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

	/*var buf []byte
	buffer := new(bytes.Buffer)
	var err error
	switch f.Kind {
	case reflect.Int:
		intVal, ok := val.(int)
		if !ok {
			return nil, errors.New("should be int")
		}
		return Int(intVal), nil
	case reflect.Int32:
		intVal, ok := val.(int32)
		if !ok {
			return nil, errors.New("should be int32")
		}
		return Int32(intVal), nil
	case reflect.Int8:
		err = binary.Write(buffer, binary.LittleEndian, int8(val.(int8)))
	case reflect.Int16:
		err = binary.Write(buffer, binary.LittleEndian, int16(val.(int16)))
	case reflect.Int64:
		intVal, ok := val.(int64)
		if !ok {
			return nil, errors.New("should be int64")
		}
		return Int64(intVal), nil
	case reflect.Uint:
		err = binary.Write(buffer, binary.LittleEndian, uint32(val.(uint)))
	case reflect.Uint32:
		err = binary.Write(buffer, binary.LittleEndian, uint32(val.(uint32)))
	case reflect.Uint8:
		err = binary.Write(buffer, binary.LittleEndian, uint8(val.(uint8)))
	case reflect.Uint16:
		err = binary.Write(buffer, binary.LittleEndian, uint16(val.(uint16)))
	case reflect.Uint64:
		err = binary.Write(buffer, binary.LittleEndian, uint64(val.(uint64)))
	default:
		err = binary.Write(buffer, binary.LittleEndian, val)
	}
	if err != nil {
		fmt.Println("GetBytes binary.Write failed:", err)
		return nil, err
	}
	buf = buffer.Bytes()
	return buf, nil*/
}

func (f *Field) tupleElement(val interface{}) tuple.TupleElement {
	if f.Kind == reflect.Uint8 { // byte stored as byte array
		return []byte{val.(byte)}
	} else {
		return val
	}
}

func (f *Field) ToInterface(obj []byte) interface{} {
	val := f.packed.DecodeToInterface(obj)
	return val

	/*if len(obj) == 0 {
		return f.GetDefault()
	}
	switch f.Kind {
	case reflect.String:
		return string(obj)
	case reflect.Int:
		return int(int32(binary.LittleEndian.Uint32(obj))) // forceing to store int as int32
	case reflect.Int32:
		return int32(binary.LittleEndian.Uint32(obj))
	case reflect.Int64:
		return int64(binary.LittleEndian.Uint64(obj))
	case reflect.Slice:
		if f.SubKind == reflect.Uint8 { // []byte
			return obj
		}
	default:
		val := f.Value.Interface()
		buf := bytes.NewReader(obj)
		err := binary.Read(buf, binary.LittleEndian, val)
		if err != nil {
			fmt.Println("binary.Read failed:", err)
		}
		return val
	}
	panic("type of this field not supported")*/
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
