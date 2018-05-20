package stored

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"reflect"
	"strings"
)

var typeOfBytes = reflect.TypeOf([]byte(nil))

type Field struct {
	Num     int
	Kind    reflect.Kind
	SubKind reflect.Kind
	Type    reflect.StructField
	Value   reflect.Value
}

// Tag is general object for tag parsing
type Tag struct {
	Name    string
	Primary bool
}

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
			part := tagParts[i]
			if part == "primary" {
				tag.Primary = true
			}
		}
	}
	return &tag
}

func (f *Field) GetBytes(obj interface{}) []byte {
	object := reflect.ValueOf(obj)
	value := object.Field(f.Num)
	var buf []byte
	switch f.Kind {
	case reflect.String:
		buf = []byte(value.String())
	case reflect.Int: // store int as int32
		buffer := new(bytes.Buffer)
		err := binary.Write(buffer, binary.LittleEndian, int32(value.Interface().(int)))
		if err != nil {
			fmt.Println("GetBytes binary.Write failed:", err)
		}
		buf = buffer.Bytes()
	case reflect.Slice:
		if f.SubKind == reflect.Uint8 {
			return value.Bytes()
		} else {
			panic("Other slices doesnt realized")
		}
	default:
		buffer := new(bytes.Buffer)
		err := binary.Write(buffer, binary.LittleEndian, value.Interface())
		if err != nil {
			fmt.Println("GetBytes binary.Write failed:", err)
		}
		buf = buffer.Bytes()
	}
	return buf
}

func (f *Field) GetInterface(obj interface{}) interface{} {
	object := reflect.ValueOf(obj)
	value := object.Field(f.Num)
	return value.Interface()
}

func (f *Field) GetDefault() interface{} {
	switch f.Kind {
	case reflect.String:
		return ""
	case reflect.Int:
		return 0
	case reflect.Int32:
		return int32(0)
	case reflect.Int64:
		return int64(0)
	}
	return nil
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
