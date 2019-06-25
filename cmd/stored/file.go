package main

import (
	"fmt"
	"go/ast"
	"go/token"
	"strings"
)

// File is an stuct which describes file for the ast
type File struct {
	pack    *Package
	name    string
	fileSet *token.FileSet
	ast     *ast.File
}

func (f *File) parseComments() {
	for _, commGroup := range f.ast.Comments {
		for _, comm := range commGroup.List {
			if !strings.HasPrefix(comm.Text, "//go:generate") {
				continue
			}
			commands := strings.Fields(comm.Text)
			if len(commands) < 2 {
				continue
			}
			if !strings.Contains(commands[1], "stored") {
				continue
			}
			comm.End()
			//f.ast.
			f.pack.gen.setPosition(f, int(comm.End()-f.ast.Pos()))
		}
	}
}

func (f *File) parseNode(node ast.Node) bool {
	switch x := node.(type) {
	case *ast.Comment:
		fmt.Println("comment", x.Text)
	case *ast.CommentGroup:
		fmt.Println("comment here")
	case *ast.BasicLit:
		s := x.Value
		fmt.Println("BasicLit", s)
	case *ast.Ident:
		s := x.Name
		fmt.Println("Ident", s)
	case *ast.CommClause:
		fmt.Println("COMMM FOUND")
	case *ast.DeclStmt:
		fmt.Println("Decl")
	case *ast.EmptyStmt:
		fmt.Println("Empty")
	case *ast.GenDecl:
		fmt.Println("declaration", x.Tok.String())
	default:
		fmt.Println("unknown")
		fmt.Println(node)
	}
	return true
}

// ParseObjects will parse of all objects inside the file
func (f *File) parseObjects() {
	for _, decl := range f.ast.Decls {
		if genDecl, ok := decl.(*ast.GenDecl); ok {

			for _, s := range genDecl.Specs {
				fmt.Println("type", s)
				if ts, ok := s.(*ast.TypeSpec); ok {

					if structType, ok := ts.Type.(*ast.StructType); ok {
						obj := Object{
							name:       ts.Name.Name,
							file:       f,
							structType: structType,
						}
						obj.parse()
						f.pack.objects = append(f.pack.objects, obj)
					}
				}
			}
		}
	}
}

func (f *File) parseFuncs() {

}

// Process will parse file to check
func (f *File) process() {
	// will fill data about all structs in file
	f.parseObjects()
	// will fill data about all comments in file
	f.parseComments()
}
