package main

import (
	"fmt"
	"go/build"
	"go/parser"
	"go/token"
	"log"
)

var gen Generator

func parsePackage(directory string, text interface{}) {
	ctx := build.Default
	pkg, err := ctx.ImportDir(directory, 0)
	if err != nil {
		log.Fatalf("cannot process directory %s: %s", directory, err)
	}
	var names []string
	names = append(names, pkg.GoFiles...)
	fs := token.NewFileSet()

	for _, name := range names {
		parsedFile, err := parser.ParseFile(fs, name, text, parser.ParseComments)
		if err != nil {
			log.Fatalf("parsing package: %s: %s", name, err)
		}
		f := File{
			name:    name,
			fileSet: fs,
			ast:     parsedFile,
		}
		f.process()
	}
}

func main() {
	gen = Generator{}
	parsePackage("./", nil)
	fmt.Println(">>>> STORED ignited!!")
}
