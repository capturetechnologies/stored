package stored

import (
	"bytes"
	"hash/fnv"
	"math/rand"
	"net"
	"sync"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
)

// Directory is wrapper around foundation db directories, main entry point for working with STORED
type Directory struct {
	Name     string
	Cluster  *Cluster
	Subspace directory.DirectorySubspace
	objects  map[string]*Object
	mux      sync.Mutex
}

// init require name and cluster properties to be set
func (d *Directory) init() {
	subspace, err := directory.CreateOrOpen(d.Cluster.db, []string{"dir", d.Name}, nil)
	if err != nil {
		panic(err)
	}
	d.Subspace = subspace
	d.objects = map[string]*Object{}

	// randomising
	// To Generate seed number we will use unix nano timestamp, plus hash from system amc adress
	// in most cases mac addresses will be roughly same in docker containers, but still small source
	// of entropy

	// Todo: best way is to add increment counter in foundationdb itself as part of first connect
	// transaction. This way each client instance will have unique id
	seed := time.Now().UnixNano()

	interfaces, err := net.Interfaces()
	if err == nil {
		for _, i := range interfaces {
			if i.Flags&net.FlagUp != 0 && bytes.Compare(i.HardwareAddr, nil) != 0 {
				h := fnv.New64()
				h.Write([]byte(i.HardwareAddr.String()))
				seed += int64(h.Sum64() / 1000000)
				break
			}
		}
	}

	rand.Seed(seed)
}

// Object declares new object for document layer
func (d *Directory) Object(name string, schemaObj interface{}) *ObjectBuilder {
	object := &Object{}
	object.init(name, &d.Cluster.db, d, schemaObj)
	d.mux.Lock()
	d.objects[name] = object
	d.mux.Unlock()
	return &ObjectBuilder{
		object: object,
	}
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

// Build will returen db object ready to operate
/*func (d *Directory) Build() *Database {
	db := Database{
		objects: d.objects,
		cluster: d.Cluster,
	}
	db.init()
	return &db
}*/

// Read will run callback in read transaction
func (d *Directory) Read(callback func(*Transaction)) *Transaction {
	db := &d.Cluster.db
	t := Transaction{db: db}
	_, err := db.ReadTransact(func(tr fdb.ReadTransaction) (interface{}, error) {
		t.initRead(tr)
		callback(&t)
		return nil, t.Err()
	})
	t.err = err
	return &t
}

// Write will run callback in write transaction
func (d *Directory) Write(callback func(*Transaction)) *Transaction {
	db := &d.Cluster.db
	t := Transaction{db: db}
	_, err := db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		t.initWrite(tr)
		callback(&t)
		return nil, t.Err()
	})
	t.err = err
	return &t
}

// Parallel will create read transaction to perform multi gets
func (d *Directory) Parallel(tasks ...PromiseAny) *Transaction {
	db := &d.Cluster.db
	t := Transaction{
		Promises: []*Promise{},
		db:       db,
	}
	for _, task := range tasks {
		t.Promises = append(t.Promises, task.self())
	}
	return &t
}
