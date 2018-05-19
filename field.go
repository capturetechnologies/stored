package stored

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"reflect"
	"strings"
)

type Field struct {
	Num   int
	Kind  reflect.Kind
	Type  reflect.StructField
	Value reflect.Value
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
	if f.Kind == reflect.String {
		buf = []byte(value.String())
	} else {
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

func (f *Field) ToInterface(obj []byte) interface{} {
	if f.Kind == reflect.String {
		return string(obj)
	} else {
		var val interface{}
		buf := bytes.NewReader(obj)
		err := binary.Read(buf, binary.LittleEndian, val)
		if err != nil {
			fmt.Println("binary.Read failed:", err)
		}
		return val
	}
}
