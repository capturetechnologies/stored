package main

import (
	"github.com/fatih/structtag"
)

// Field describes field of object in the ast
type Field struct {
	name    string
	tagName string
	tag     string
	Options []string
	num     int
}

func fieldParse(name string, tag string, fieldNum int) (Field, error) {
	field := Field{
		name: name,
		tag:  tag,
		num:  fieldNum,
	}
	if tag[0] == '`' && tag[len(field.tag)-1] == '`' {
		tag = tag[1 : len(field.tag)-1]
	}
	tags, err := structtag.Parse(tag)
	if err != nil {
		return field, err
	}
	storedKey, err := tags.Get("stored")
	if err != nil {
		return field, err
	}
	field.tagName = storedKey.Name
	field.Options = storedKey.Options
	return field, nil
}
