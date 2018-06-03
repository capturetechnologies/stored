package stored

import (
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
)

// Directory is wrapper around foundation db directories, main entry point for working with STORED
type Directory struct {
	Name       string
	Connection *Connection
	Subspace   directory.DirectorySubspace
}

// Object declares new object for document layer
func (d *Directory) Object(name string, schemaObj interface{}) (ret *Object) {
	ret = &Object{}
	ret.init(name, &d.Connection.db, d, schemaObj)
	return
}

// Clear removes all content inside directory
func (d *Directory) Clear() error {
	_, err := d.Connection.db.Transact(func(tr fdb.Transaction) (ret interface{}, e error) {
		ret, e = d.Subspace.Remove(tr, []string{})
		return
	})
	if err != nil {
		return err
	}
	return nil
}
