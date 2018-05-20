package stored

import (
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
)

// Connection is the main  struct for handling work with fdb
type Connection struct {
	db fdb.Database
}

// Key is main type for
type Key = tuple.Tuple

// Connect is main constructor for creating connections
func Connect(cluster, dbname string) *Connection {
	fdb.MustAPIVersion(510)
	conn := Connection{
		db: fdb.MustOpen(cluster, []byte(dbname)),
	}
	return &conn
}

func (c *Connection) Object(name string, schemaObj interface{}) (ret *Object) {
	ret = &Object{}
	ret.Init(name, &c.db, schemaObj)
	return
}
