package main

import (
	"go/build"
	"go/parser"
	"go/token"
	"log"
)

// Package will return
type Package struct {
	files   []File
	objects map[string]Object
	gen     Generator
}

func (p *Package) init() {
	p.objects = map[string]Object{}
	p.files = []File{}
	p.gen = Generator{}
}

func (p *Package) parse(directory string, text interface{}) {
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
		file := File{
			pack:    p,
			name:    name,
			fileSet: fs,
			ast:     parsedFile,
		}
		p.files = append(p.files, file)
		file.process()
	}
}

func (p *Package) genTopObject() string { // generates top level object with all instances of database
	objs := ""
	for _, obj := range p.objects {
		objs += "	" + obj.name + " " + obj.name + "Stored\n"
	}
	return "var db StoredDB = StoredDB{}\ntype StoredDB struct {\n" + objs + "}"
}

func (p *Package) generate() {
	blocks := []string{}
	blocks = append(blocks, p.genTopObject())
	for _, obj := range p.objects {
		if len(obj.fields) > 0 {
			blocks = append(blocks, obj.generate())
		}
	}
	p.gen.generate(blocks)
}
