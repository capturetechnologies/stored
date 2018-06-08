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
	if field.Kind == reflect.String {
		return []byte(value.String())
	}
	if field.Kind == reflect.Slice {
		if field.SubKind == reflect.Uint8 {
			return value.Bytes()
		} else {
			panic("Other slices doesnt realized")
		}
	}
	buffer := new(bytes.Buffer)
	var err error
	switch field.Kind {
	case reflect.Int:
		err = binary.Write(buffer, binary.LittleEndian, int32(value.Interface().(int)))
	case reflect.Int32:
		err = binary.Write(buffer, binary.LittleEndian, int32(value.Interface().(int32)))
	case reflect.Int8:
		err = binary.Write(buffer, binary.LittleEndian, int8(value.Interface().(int8)))
	case reflect.Int16:
		err = binary.Write(buffer, binary.LittleEndian, int16(value.Interface().(int16)))
	case reflect.Int64:
		err = binary.Write(buffer, binary.LittleEndian, int64(value.Interface().(int64)))
	case reflect.Uint:
		err = binary.Write(buffer, binary.LittleEndian, uint32(value.Interface().(uint)))
	case reflect.Uint32:
		err = binary.Write(buffer, binary.LittleEndian, uint32(value.Interface().(uint32)))
	case reflect.Uint8:
		err = binary.Write(buffer, binary.LittleEndian, uint8(value.Interface().(uint8)))
	case reflect.Uint16:
		err = binary.Write(buffer, binary.LittleEndian, uint16(value.Interface().(uint16)))
	case reflect.Uint64:
		err = binary.Write(buffer, binary.LittleEndian, uint64(value.Interface().(uint64)))
	default:
		err = binary.Write(buffer, binary.LittleEndian, value.Interface())
	}
	if err != nil {
		fmt.Println("GetBytes binary.Write failed:", err)
	}
	buf = buffer.Bytes()
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
