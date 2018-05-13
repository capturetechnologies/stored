package stored

import (
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
	"github.com/vmihailenco/msgpack"
)

type Object struct {
	name    string
	db      *fdb.Database
	storage Storage
	primary string
	key     tuple.Tuple
}

func (o *Object) Init(name string, db *fdb.Database, schemaObj interface{}) {
	o.name = name
	o.db = db
	o.key = tuple.Tuple{name}
	o.storage = Storage{}
	o.storage.Init(schemaObj)
}

func (o *Object) getPrimary() tuple.TupleElement {
	if o.primary == "" {
		panic("Object " + o.name + " has no primary key")
	}
	return o.storage.GetField(o.primary)
}

func (o *Object) Set(data interface{}) error {
	/*binary, err := msgpack.Marshal(data)
	if err != nil {
		return err
	}*/
	primary := o.getPrimary()
	if primary == nil {
		panic("Object " + o.name + ", primary key «" + o.primary + "» is undefined")
	}
	mainKey := append(o.key, primary)

	_, err := o.db.Transact(func(tr fdb.Transaction) (ret interface{}, e error) {
		for fieldName := range o.storage.Fields {
			value := o.storage.GetField(fieldName)
			binary, err := msgpack.Marshal(value)
			tr.Set(append(o.key, fieldName), binary)
		}
		//tr.Set(append(o.key, o.getPrimary()), binary)
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
