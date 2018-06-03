package stored

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"reflect"
)

// Struct used for work with input structure
type Struct struct {
	object reflect.Value
}

// Set sets field value using bytes
func (s *Struct) Set(field *Field, data []byte) {
	objField := s.object.Field(field.Num)
	interfaceValue := reflect.ValueOf(field.ToInterface(data))
	objField.Set(interfaceValue)
}

// Get return field as interface
func (s *Struct) Get(field *Field) interface{} {
	value := s.object.Field(field.Num)
	return value.Interface()
}

// GetBytes return field as byteSlice
func (s *Struct) GetBytes(field *Field) []byte {
	value := s.object.Field(field.Num)
	var buf []byte
	switch field.Kind {
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
		if field.SubKind == reflect.Uint8 {
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

// StructEditable return Struct object with check for pointer (could be editable)
func StructEditable(data interface{}) *Struct {
	object := reflect.ValueOf(data)
	if object.Kind() != reflect.Ptr {
		panic("you should pass link to the object")
	}
	object = object.Elem()
	input := Struct{
		object: object,
	}
	return &input
}

// StructAny return Struct object from any sruct
func StructAny(data interface{}) *Struct {
	object := reflect.ValueOf(data)
	if object.Kind() == reflect.Ptr {
		object = object.Elem()
	}
	input := Struct{
		object: object,
	}
	return &input
}
