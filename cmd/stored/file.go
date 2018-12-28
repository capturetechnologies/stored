package main

import (
	"go/ast"
	"go/token"
	"strings"
)

// File is an stuct which describes file for the ast
type File struct {
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
			gen.setPosition(f, int(comm.End()-f.ast.Pos()))
		}
	}
}

func (f *File) parseNode(node ast.Node) bool {
	/*switch x := node.(type) {
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
	}*/
	return true
}

// Process will parse file to check
func (f *File) process() {
	if f.name == "db_new.go" {
		ast.Inspect(f.ast, f.parseNode)
		f.parseComments()
		gen.Generate()
	}
}
