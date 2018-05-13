package stored

import (
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
	"github.com/vmihailenco/msgpack"
)

type Object struct {
	name    string
	db      *fdb.Database
	data    Storage
	primary string
	key     tuple.Tuple
}

func (o *Object) Init(name string, db *fdb.Database) {
	o.name = name
	o.db = db
	o.key = tuple.Tuple{name}
}

func (o *Object) getPrimary() tuple.TupleElement {
	if o.primary == "" {
		panic("Object " + o.name + " has no primary key")
	}
	return o.data.GetField(o.primary)
}

func (o *Object) Set(data interface{}) error {
	binary, err := msgpack.Marshal(data)
	if err != nil {
		return err
	}
	_, err = o.db.Transact(func(tr fdb.Transaction) (ret interface{}, e error) {
		tr.Set(append(o.key, o.getPrimary()), binary)
		return
	})
	if err != nil {
		return err
	}
	return nil
}

func (o *Object) Primary(key string) *Object {
	o.primary = key
	return o
}
