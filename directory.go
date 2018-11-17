package stored

import (
	"bytes"
	"hash/fnv"
	"math/rand"
	"net"
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
func (d *Directory) Object(name string, schemaObj interface{}) (ret *Object) {
	ret = &Object{}
	ret.init(name, &d.Cluster.db, d, schemaObj)
	d.objects[name] = ret
	return
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
