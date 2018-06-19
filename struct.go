package stored

import (
	"fmt"
	"reflect"

	"github.com/apple/foundationdb/bindings/go/src/fdb/subspace"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
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
	val, err := field.ToBytes(value.Interface())
	if err != nil {
		fmt.Println("field to bytes err", err)
	}
	return val
}

// Primary get primary tuple based on input object
func (s *Struct) Primary(object *Object) tuple.Tuple {
	if object.primaryFields == nil {
		object.panic("primary key is undefined")
	}
	primary := tuple.Tuple{}
	for _, field := range object.primaryFields {
		fieldVal := s.Get(field)
		primary = append(primary, fieldVal)
	}
	return primary
}

// Subspace get subspace with primary keys for parst object
func (s *Struct) Subspace(object *Object) subspace.Subspace {
	primaryTuple := s.Primary(object)
	return object.primary.Sub(primaryTuple...)
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
