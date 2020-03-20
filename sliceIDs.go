package stored

import (
	"errors"
	"reflect"

	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
)

// SliceIDs is slice that contain  only int64 data
type SliceIDs struct {
	object    *Object
	ids       []tuple.Tuple
	values    [][]byte
	dataField *Field
	err       error
}

func (s *SliceIDs) init(object *Object) {
	s.object = object
	s.ids = []tuple.Tuple{}
	s.values = [][]byte{}
}

func (s *SliceIDs) push(key tuple.Tuple, val []byte) {
	s.ids = append(s.ids, key)
	s.values = append(s.values, val)
}

// Int64 will format response as int64 map if possible
func (s *SliceIDs) Int64() (map[int64][]byte, error) {
	if s.err != nil {
		return nil, s.err
	}
	res := map[int64][]byte{}
	for k, v := range s.ids {
		val, ok := v[0].(int64)
		if !ok {
			return nil, errors.New("Ids is not int64")
		}
		res[val] = s.values[k]
	}
	return res, nil
}

// ScanAll will scan all results for slice pointer
func (s *SliceIDs) ScanAll(slicePointer interface{}) (e error) {
	if s.err != nil {
		return s.err
	}
	itemsPtr := reflect.ValueOf(slicePointer)
	items := itemsPtr.Elem()

	if items.Kind() != reflect.Slice {
		panic("ScanAll object should be slice")
	}

	pointer := false
	if items.Type().Elem().Kind() == reflect.Ptr {
		pointer = true
	}

	for n, key := range s.ids {
		newStruct := reflect.New(s.object.reflectType)
		newStruct = newStruct.Elem()

		if len(key) != len(s.object.primaryFields) {
			s.object.panic("ScanAll of swype ids, primary keys count mismatch")
		}
		for num, field := range s.object.primaryFields {
			keyInterface := key[num]
			objField := newStruct.Field(field.Num)
			if !objField.CanSet() {
				s.object.panic("Could not set object primary key " + field.Name)
			}
			interfaceValue := reflect.ValueOf(keyInterface)
			objField.Set(interfaceValue) // set tuple value to object
		}

		if s.dataField != nil {
			value := s.values[n]
			objField := newStruct.Field(s.dataField.Num)
			if !objField.CanSet() {
				s.object.panic("Could not set object data field " + s.dataField.Name)
			}
			if objField.Kind() == reflect.Ptr && len(value) == 0 {
				// setting an emby object to fix the problem
				t := objField.Type().Elem()
				newFieldObject := reflect.New(t)
				objField.Set(newFieldObject)

			} else {

				keyInterface := s.dataField.ToInterface(value)
				interfaceValue := reflect.ValueOf(keyInterface)
				objField.Set(interfaceValue)
			}
			//HERE
		}

		if pointer {
			appended := reflect.Append(items, newStruct.Addr())
			items.Set(appended)
		} else {
			appended := reflect.Append(items, newStruct)
			items.Set(appended)
		}
	}
	return
}
