package stored

import (
	"reflect"
	"strings"
)

type Storage struct {
	Fields map[string]int
	value  reflect.Value
}

func (s *Storage) Init(obj interface{}) {
	t := reflect.TypeOf(obj)
	s.value = reflect.ValueOf(obj)
	if s.value.Kind() == reflect.Ptr {
		t = t.Elem()
		s.value = s.value.Elem()
	}
	numFields := s.value.NumField()
	for i := 0; i < numFields; i++ {
		field := t.Field(i)
		tag := s.parseTag(field.Tag.Get("stored"))
		if tag != nil {
			s.Fields[tag.Name] = i
		}
	}
}

func (s *Storage) GetField(name string) interface{} {
	fieldNum, ok := s.Fields[name]
	if !ok {
		return nil
	}
	field := s.value.Field(fieldNum)
	/*if field.Kind() == reflect.Ptr {
		field = field.Elem()
	}*/
	return field.Interface()
}

// Tag is general object for tag parsing
type Tag struct {
	Name string
}

func (s *Storage) parseTag(tag string) *Tag {
	if tag == "" {
		return nil
	}
	tagParts := strings.Split(tag, ",")
	res := Tag{
		Name: tagParts[0],
	}
	return &res
}
