package stored

import (
	"errors"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
	"github.com/apple/foundationdb/bindings/go/src/fdb/subspace"
)

// Connection is the main  struct for handling work with fdb
type Connection struct {
	db fdb.Database
}

// ErrNotFound is an error returned when no rows was found
var ErrNotFound = errors.New("Document not found")

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
	return &Directory{
		Name:       name,
		Connection: c,
		Subspace:   subspace,
	}
}

// NeedRange return promise for objects data by primary key
func NeedRange(tr fdb.ReadTransaction, key subspace.Subspace) fdb.RangeResult {
	start, end := key.FDBRangeKeys()
	r := fdb.KeyRange{Begin: start, End: end}
	return tr.GetRange(r, fdb.RangeOptions{Mode: fdb.StreamingModeWantAll})
}
