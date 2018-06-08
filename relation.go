package stored

import (
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
)

var RelationN2N = 1

type Relation struct {
	Host   *Object
	Client *Object
	dir    *directory.DirectorySubspace
	Kind   int
}

func (r *Relation) init(kind int, host *Object, client *Object) {
	r.Kind = kind
	r.Host = host
	r.Client = client
	r.dir, err = dir.Subspace.CreateOrOpen(db, []string{"rel", host.name, client.name}, nil)
	if err != nil {
		panic(err)
	}
	host.Relations = append(host.Relations, r)
}

func (r *Relation) Add(hostObject interface{}, clientObject interface{}) {
	_, err := r.Host.db.Transact(func(tr fdb.Transaction) (ret interface{}, e error) {
		r.dir.W
		e = o.Write(tr, key, []byte{}])
		return
	})
}
