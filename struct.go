package stored

import (
	"fmt"
	"reflect"

	"github.com/apple/foundationdb/bindings/go/src/fdb/subspace"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
)

// Struct used for work with input structure
type Struct struct {
	editable bool
	value    reflect.Value
}

// setField sets field value using bytes
func (s *Struct) setField(field *Field, data []byte) {
	objField := s.value.Field(field.Num)
	if objField.Kind() == reflect.Ptr {
		if objField.IsNil() {
			if len(data) == 0 {
				return
			}
			// This code is working in main case
			t := field.Value.Type().Elem()
			value := reflect.New(t)
			objField.Set(value) // creating empty object to fill it below
		}

	}
	err := field.packed.DecodeToValue(data, objField)
	if err != nil {
		fmt.Println("Decode to value failed", field.Name, field.object.name, len(data), err)
	}
}

// incField increment field value using interface value
func (s *Struct) incField(field *Field, toInc interface{}) {
	objField := s.value.Field(field.Num)
	inter := objField.Interface()
	switch field.Kind {
	case reflect.Int:
		s.setFieldValue(objField, inter.(int)+toInc.(int))
	case reflect.Uint:
		s.setFieldValue(objField, inter.(uint)+toInc.(uint))
	case reflect.Int32:
		s.setFieldValue(objField, inter.(int32)+toInc.(int32))
	case reflect.Uint32:
		s.setFieldValue(objField, inter.(uint32)+toInc.(uint32))
	case reflect.Int64:
		s.setFieldValue(objField, inter.(int64)+toInc.(int64))
	case reflect.Uint64:
		s.setFieldValue(objField, inter.(uint64)+toInc.(uint64))
	default:
		panic("field " + field.Name + " is not incrementable")
	}
}

func (s *Struct) setFieldValue(objField reflect.Value, value interface{}) {
	objField.Set(reflect.ValueOf(value))
}

// Fill will use data inside value object to fill struct
func (s *Struct) Fill(o *Object, v *Value) {
	if !s.editable {
		panic("attempt to change readonly struct")
	}
	for fieldName, binaryValue := range v.raw {
		field, ok := o.fields[fieldName]
		if ok {
			s.setField(field, binaryValue)
		} else {
			//o.log("unknown field «" + fieldName + "», skipping")
			//nothing to worry about
		}
	}
	// decoded used to avoid unnecessary decode and encode
	for fieldName, interfaceValue := range v.decoded {
		field, ok := o.fields[fieldName]
		if ok {
			field.setTupleValue(s.value, interfaceValue)
		}
	}
}

// Get return field as interface
func (s *Struct) Get(field *Field) interface{} {
	value := s.value.Field(field.Num)
	return value.Interface()
}

// GetBytes return field as byteSlice
func (s *Struct) GetBytes(field *Field) []byte {
	value := s.value.Field(field.Num)

	res, err := field.packed.Encode(value.Interface())
	if err != nil {
		fmt.Println("encode GetBytes failed", res)
	}
	return res

	/*if field.Kind == reflect.String {
		return []byte(value.String())
	}
	if field.Kind == reflect.Slice {
		if field.SubKind == reflect.Uint8 {
			return value.Bytes()
		} else if field.SubKind == reflect.String {

		} else {
			panic("Other slices doesnt realized")
		}
	}
	val, err := field.ToBytes(value.Interface())
	if err != nil {
		fmt.Println("field to bytes err", err)
	}
	return val*/
}

// getPrimary get primary tuple based on input object
func (s *Struct) getPrimary(object *Object) tuple.Tuple {
	if object.primaryFields == nil {
		object.panic("primary key is undefined")
	}
	return s.getTuple(object.primaryFields)
}

func (s *Struct) getTuple(fields []*Field) tuple.Tuple {
	tuple := tuple.Tuple{}
	for _, field := range fields {
		fieldVal := s.Get(field)
		tuple = append(tuple, field.tupleElement(fieldVal))
	}
	return tuple
}

// getSubspace get subspace with primary keys for parst object
func (s *Struct) getSubspace(object *Object) subspace.Subspace {
	primaryTuple := s.getPrimary(object)
	return object.primary.Sub(primaryTuple...)
}

func (s *Struct) getType() reflect.Type {
	return reflect.Indirect(s.value).Type()
}

// structEditable return Struct object with check for pointer (could be editable)
func structEditable(data interface{}) *Struct {
	value := reflect.ValueOf(data)
	if value.Kind() != reflect.Ptr {
		panic("you should pass link to the object")
	}
	value = value.Elem() // unpointer, interface still
	input := Struct{
		value:    value,
		editable: true,
	}
	return &input
}

// structAny return Struct object from any sruct
func structAny(data interface{}) *Struct {
	value := reflect.ValueOf(data)
	if value.Kind() == reflect.Ptr {
		value = value.Elem()
	}
	input := Struct{
		value: value,
	}
	return &input
}
