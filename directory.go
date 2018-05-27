package stored

import "github.com/apple/foundationdb/bindings/go/src/fdb/directory"

type Directory struct {
	Name       string
	Connection *Connection
	Subspace   directory.DirectorySubspace
}

func (d *Directory) Object(name string, schemaObj interface{}) (ret *Object) {
	ret = &Object{}
	ret.Init(name, &d.Connection.db, d, schemaObj)
	return
}

func (c *Connection) Directory(name string) *Directory {
	subspace, err := directory.CreateOrOpen(c.db, []string{name}, nil)
	if err != nil {
		panic(err)
	}
	return &Directory{
		Name:       name,
		Connection: c,
		Subspace:   subspace,
	}
}
