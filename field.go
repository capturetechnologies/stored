package stored

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"reflect"
	"strings"
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
}

// Tag is general object for tag parsing
type Tag struct {
	Name          string
	Primary       bool
	Mutable       bool
	AutoIncrement bool
}

// ParseTag converts object stored tag to sturct with options
func (f *Field) ParseTag() *Tag {
	tagStr := f.Type.Tag.Get("stored")
	if tagStr == "" {
		return nil
	}
	tagParts := strings.Split(tagStr, ",")
	tag := Tag{
		Name: tagParts[0],
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
		panic("unsupported type for getdefault")
	}
}

func (f *Field) ToInterface(obj []byte) interface{} {
	if len(obj) == 0 {
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
	panic("type of this field not supported")
}

func (f *Field) panic(text string) {
	panic("field «" + f.Name + "» " + text)
}

// Get1 return binary representation for "1" for increment
func (f *Field) Get1() []byte {
	switch f.Kind {
	case reflect.Int64, reflect.Uint64:
		return []byte{'\x01', '\x00', '\x00', '\x00', '\x00', '\x00', '\x00', '\x00'}
	case reflect.Int, reflect.Int32, reflect.Uint32:
		return []byte{'\x01', '\x00', '\x00', '\x00'}
	case reflect.Int16, reflect.Uint16:
		return []byte{'\x01', '\x00'}
	case reflect.Int8, reflect.Uint8:
		return []byte{'\x01'}
	default:
		f.panic("do not support autoincrement")
	}
	return []byte{}
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
