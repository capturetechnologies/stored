package stored

import (
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
)

// Connection is the main  struct for handling work with fdb
type Connection struct {
	db fdb.Database
}

// Connect is main constructor for creating connections
func Connect(cluster string) *Connection {
	fdb.MustAPIVersion(510)
	conn := Connection{
		db: fdb.MustOpen(cluster, []byte("DB")),
	}
	return &conn
}

// Directory created an directury that could be used to work with stored
func (c *Connection) Directory(name string) *Directory {
	subspace, err := directory.CreateOrOpen(c.db, []string{name}, nil)
	if err != nil {
		panic(err)
	}
	dir := Directory{
		Name:       name,
		Connection: c,
		Subspace:   subspace,
	}
	dir.init()
	return &dir
}
