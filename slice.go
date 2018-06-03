package stored

import "reflect"

// Slice used for iteration over list of values
type Slice struct {
	values []*Value
	err    error
}

// Append push value inside slice
func (s *Slice) Append(val *Value) {
	s.values = append(s.values, val)
}

func (s *Slice) ScanAll(slicePointer interface{}) (e error) {
	valuePtr := reflect.ValueOf(slicePointer)
	value := valuePtr.Elem()

	if value.Kind() != reflect.Slice {
		panic("ScanAll object should be slice")
	}

	for _, val := range s.values {
		newStruct, err := val.Reflect()
		if err != nil {
			e = err
		}
		appended := reflect.Append(value, newStruct)
		value.Set(appended)
	}
	return
}
