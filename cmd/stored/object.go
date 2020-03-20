package main

import (
	"go/ast"
	"strconv"
	"strings"

	"github.com/fatih/structtag"
)

// Object is an stuct which describes object for the ast
type Object struct {
	name       string
	shortForm  string // an alias using in: func (shortFrom *ObjectName) somefunc
	file       *File
	structType *ast.StructType
	fields     []*Field
}

func (o *Object) parse() {
	o.fields = []*Field{}
	fields := o.structType.Fields.List
	fieldNum := 0
	for _, field := range fields {
		if len(field.Names) > 0 {
			name := field.Names[0]
			tag := field.Tag

			if tag == nil {
				continue
			}
			objField, err := o.fieldParse(name.Name, tag.Value, fieldNum)
			fieldNum++
			objField.parseType(field.Type)

			if err == nil {
				o.fields = append(o.fields, &objField)
			}
		}
	}

	o.shortForm = strings.ToLower(o.name[0:1])
}

func (o *Object) fieldParse(name string, tag string, fieldNum int) (Field, error) {
	field := Field{
		object: o,
		name:   name,
		tag:    tag,
		num:    fieldNum,
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

func (o *Object) genMain() string {
	return "type " + o.name + "Stored {}\n"
}

func (o *Object) generate() string {
	fieldsEncode := ""
	for _, field := range o.fields {
		fieldsEncode += `case ` + strconv.Itoa(field.num) + `:
		` + field.generateEncode() + `
	`
	}
	fieldsEncode += `default:
		panic(errors.New("unknown field encoded"))`
	mainObject := o.genMain()
	return "\n\n" + mainObject + `
func (` + o.shortForm + ` *` + o.name + `) _encodeField(num int, writer io.Writer) (err error) {
	switch(num) {
	` + fieldsEncode + `
	}
	return errors.New("unknown field number")
}
`
}
