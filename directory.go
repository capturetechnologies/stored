package stored

import (
	"fmt"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
)

// Directory is wrapper around foundation db directories, main entry point for working with STORED
type Directory struct {
	Name       string
	Connection *Connection
	Subspace   directory.DirectorySubspace
	objects    map[string]*Object
}

func (d *Directory) init() {
	d.objects = map[string]*Object{}
}

// Object declares new object for document layer
func (d *Directory) Object(name string, schemaObj interface{}) (ret *Object) {
	ret = &Object{}
	ret.init(name, &d.Connection.db, d, schemaObj)
	d.objects[name] = ret
	return
}

// Clear removes all content inside directory
func (d *Directory) Clear() error {
	_, err := d.Connection.db.Transact(func(tr fdb.Transaction) (ret interface{}, e error) {
		//ret, e = d.Subspace.Remove(tr, []string{})
		if e != nil {
			fmt.Println("remove directory fail", ret, e)
		}
		for _, obj := range d.objects {
			err := obj.Clear()
			if err != nil {
				return nil, err
			}
			/*ret, e = d.Subspace.Remove(tr, []string{path})
			if e != nil {
				fmt.Println("remove directory fail", ret, e)
			}*/
		}
		return
	})
	if err != nil {
		return err
	}
	return nil
}

// Creates reference object for multi requests
func (d *Directory) Multi() *MultiChain {
	mc := MultiChain{db: d.Connection.db}
	mc.init()
	return &mc
}
