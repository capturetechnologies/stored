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
	objects []Object
	gen     Generator
}

func (p *Package) init() {
	p.objects = []Object{}
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

func (p *Package) generate() {
	blocks := []string{}
	for _, obj := range p.objects {
		blocks = append(blocks, obj.generate())
	}
	p.gen.generate(blocks)
}
