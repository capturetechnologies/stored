package main

import (
	"go/ast"
)

// Field describes field of object in the ast
type Field struct {
	object    *Object
	name      string
	tagName   string
	tag       string
	Options   []string
	fieldType FieldType
	num       int
}

func (f *Field) generateEncode() string {
	return `value := ` + f.object.shortForm + `.` + f.name + `
		` + f.fieldType.generateEncode()
	//return `[]byte{}`
}

func (f *Field) parseType(expr ast.Expr) {
	f.fieldType = FieldType{}
	f.fieldType.parse(expr)
}
