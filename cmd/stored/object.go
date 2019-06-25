package main

import (
	"go/ast"
	"strings"
)

// Object is an stuct which describes object for the ast
type Object struct {
	name       string
	file       *File
	structType *ast.StructType
	fields     []Field
}

func (o *Object) parse() {
	o.fields = []Field{}
	fields := o.structType.Fields.List
	fieldNum := 0
	for _, field := range fields {
		if len(field.Names) > 0 {
			name := field.Names[0]
			tag := field.Tag
			if tag == nil {
				continue
			}
			field, err := fieldParse(name.Name, tag.Value, fieldNum)
			fieldNum++

			if err == nil {
				o.fields = append(o.fields, field)
			}
		}
	}
}

func (o *Object) generate() string {
	firstLetter := strings.ToLower(o.name[0:1])
	return `
func (` + firstLetter + ` *` + o.name + `) _encodeField(num int) {
	
}
`
}
