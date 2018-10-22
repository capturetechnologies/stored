package stored

import (
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
)

// Directory is wrapper around foundation db directories, main entry point for working with STORED
type Directory struct {
	Name     string
	Cluster  *Cluster
	Subspace directory.DirectorySubspace
	objects  map[string]*Object
}

func (d *Directory) init() {
	d.objects = map[string]*Object{}
}

// Object declares new object for document layer
func (d *Directory) Object(name string, schemaObj interface{}) (ret *Object) {
	ret = &Object{}
	ret.init(name, &d.Cluster.db, d, schemaObj)
	d.objects[name] = ret
	return
}

// Clear removes all content inside directory
func (d *Directory) Clear() error {
	_, err := d.Cluster.db.Transact(func(tr fdb.Transaction) (ret interface{}, e error) {
		for _, obj := range d.objects {
			err := obj.Clear()
			if err != nil {
				return nil, err
			}
		}
		return
	})
	if err != nil {
		return err
	}
	return nil
}

// Multi creates reference object for multi requests
func (d *Directory) Multi() *MultiChain {
	mc := MultiChain{db: d.Cluster.db}
	mc.init()
	return &mc
}
