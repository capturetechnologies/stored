package stored

import (
	"reflect"
)

// Slice used for iteration over list of values
type Slice struct {
	values []*Value
	//indexData [][]byte
	err error
}

// Append push value inside slice
func (s *Slice) Append(val *Value) {
	s.values = append(s.values, val)
}

// ScanAll fetches all rows from slice
func (s *Slice) ScanAll(slicePointer interface{}) (e error) {
	if s.err != nil {
		return s.err
	}
	valuePtr := reflect.ValueOf(slicePointer)
	value := valuePtr.Elem()

	if value.Kind() != reflect.Slice {
		panic("ScanAll object should be slice")
	}

	pointer := false
	if value.Type().Elem().Kind() == reflect.Ptr {
		pointer = true
	}

	for _, val := range s.values {
		newStruct, err := val.Reflect()
		if err != nil {
			e = err
		}
		if pointer {
			appended := reflect.Append(value, newStruct.Addr())
			value.Set(appended)
		} else {
			appended := reflect.Append(value, newStruct)
			value.Set(appended)
		}
	}
	return
}

// Each will go through all elements in slice
func (s *Slice) Each(cb func(item interface{})) {
	for _, val := range s.values {
		cb(val.Interface())
	}
}

// Len return number of elements in slice
func (s *Slice) Len() int {
	return len(s.values)
}

// GetIndexData return indexData slice of byte array
/*func (s *Slice) GetIndexData() [][]byte {
	return s.indexData
}*/

func (s *Slice) fillFieldData(field *Field, indexData [][]byte) {
	for k, value := range s.values {
		data := indexData[k]
		value.raw[field.Name] = data
	}
}
