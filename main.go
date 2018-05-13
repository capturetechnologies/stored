package stored

import (
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
	"github.com/vmihailenco/msgpack"
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

func (c *Connection) SetPack(key Key, obj interface{}) (e error) {
	data, err := msgpack.Marshal(obj)
	if err != nil {
		return err
	}
	_, err = c.db.Transact(func(tr fdb.Transaction) (ret interface{}, e error) {
		tr.Set(key, data)
    return
	})
  if err != nil {
		return err
	}
  return
}

func (c *Connection) GetPack(key Key) *Pack {
	data, err := c.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		return tr.Get(key).MustGet(), nil
	})
  if err != nil {
		return &Pack{
			err: err,
		}
  }
	return &Pack{
		binary: data.([]byte),
	}
}

func (c *Connection) Object(name string, schemaObj interface{}) (ret *Object) {
	ret = &Object{}
	ret.Init(name, &c.db)
	return
}
