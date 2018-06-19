package stored

import (
	"fmt"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
	"github.com/apple/foundationdb/bindings/go/src/fdb/subspace"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
)

var RelationN2N = 1

// list of service keys for shorter
var keyRelHostCount = "a"
var keyRelClientCount = "b"

type Relation struct {
	host      *Object
	client    *Object
	hostDir   directory.DirectorySubspace
	clientDir directory.DirectorySubspace
	infoDir   directory.DirectorySubspace
	kind      int
	counter   bool
}

func (r *Relation) init(kind int, host *Object, client *Object) {
	r.kind = kind
	r.host = host
	r.client = client
	var err error
	dir := host.directory.Subspace
	db := host.db
	r.infoDir, err = dir.CreateOrOpen(db, []string{"rel", host.name, client.name}, nil)
	if err != nil {
		panic(err)
	}
	r.hostDir, err = r.infoDir.CreateOrOpen(db, []string{"host"}, nil)
	if err != nil {
		panic(err)
	}
	r.clientDir, err = r.infoDir.CreateOrOpen(db, []string{"client"}, nil)
	if err != nil {
		panic(err)
	}
	host.Relations = append(host.Relations, r)
}

func (r *Relation) panic(text string) {
	panic("relation between «" + r.host.name + "» and «" + r.client.name + "»: " + text)
}

func (r *Relation) Counter(on bool) {
	r.counter = on
}

// return primary values, no masser object wass passed or primary value
func (r *Relation) getPrimary(hostObject interface{}, clientObject interface{}) (tuple.Tuple, tuple.Tuple) {
	//hostPrimary := r.host.GetPrimaryField().fromAnyInterface(hostObject)
	hostPrimary := r.host.getPrimaryTuple(hostObject)
	//clientPrimary := r.client.GetPrimaryField().fromAnyInterface(clientObject)
	clientPrimary := r.client.getPrimaryTuple(clientObject)
	return hostPrimary, clientPrimary
}

// Set writes new relation beween objects, you could use objects or values with same types as primary key
func (r *Relation) Set(hostOrID interface{}, clientOrID interface{}) error {
	return r.SetData(hostOrID, clientOrID, []byte{}, []byte{})
}

// SetData sets additional data to relation
func (r *Relation) SetData(hostOrID interface{}, clientOrID interface{}, hostVal []byte, clientVal []byte) error {
	hostPrimary, clientPrimary := r.getPrimary(hostOrID, clientOrID)
	_, err := r.host.db.Transact(func(tr fdb.Transaction) (ret interface{}, e error) {
		if r.counter { // increment if not exists
			val, err := tr.Get(r.hostDir.Pack(tuple.Tuple{hostPrimary, clientPrimary})).Get()
			if err != nil {
				return nil, err
			}
			if val == nil { // not exists increment here
				tr.Add(r.infoDir.Sub(keyRelHostCount).Pack(hostPrimary), countInc)
				tr.Add(r.infoDir.Sub(keyRelClientCount).Pack(clientPrimary), countInc)
			}
		}
		tr.Set(r.hostDir.Sub(hostPrimary...).Pack(clientPrimary), hostVal)
		tr.Set(r.clientDir.Sub(clientPrimary...).Pack(hostPrimary), clientVal)
		return
	})
	return err
}

// Delete removes relation between objects
func (r *Relation) Delete(hostOrID interface{}, clientOrID interface{}) error {
	hostPrimary, clientPrimary := r.getPrimary(hostOrID, clientOrID)
	_, err := r.host.db.Transact(func(tr fdb.Transaction) (ret interface{}, e error) {
		if r.counter { // increment if not exists
			val, err := tr.Get(r.hostDir.Sub(hostPrimary...).Pack(clientPrimary)).Get()
			if err != nil {
				return nil, err
			}
			if val != nil { // exists decrement here
				tr.Add(r.infoDir.Sub(keyRelHostCount).Pack(hostPrimary), countDec)
				tr.Add(r.infoDir.Sub(keyRelClientCount).Pack(clientPrimary), countDec)
			}
		}
		tr.Clear(r.hostDir.Sub(hostPrimary...).Pack(clientPrimary))
		tr.Clear(r.clientDir.Sub(clientPrimary...).Pack(hostPrimary))
		return
	})
	return err
}

// GetHostsCount fetches counter
func (r *Relation) GetHostsCount(clientOrID interface{}) (int64, error) {
	if !r.counter {
		r.panic("counter is off, use rel.Counter(true)")
	}
	//hostPrimary := r.host.GetPrimaryField().fromAnyInterface(hostOrID)
	clientPrimary := r.host.getPrimaryTuple(clientOrID)
	row, err := r.host.db.Transact(func(tr fdb.Transaction) (ret interface{}, e error) {
		row, err := tr.Get(r.infoDir.Sub(keyRelClientCount).Pack(clientPrimary)).Get()
		if row == nil {
			return nil, ErrNotFound
		}
		return ToInt64(row), err
	})
	if err != nil {
		return int64(0), err
	}
	return row.(int64), nil
}

// GetClientsCount fetches counter
func (r *Relation) GetClientsCount(hostOrID interface{}) (int64, error) {
	if !r.counter {
		r.panic("counter is off, use rel.Counter(true)")
	}
	//clientPrimary := r.client.GetPrimaryField().fromAnyInterface(clientOrID)
	hostPrimary := r.client.getPrimaryTuple(hostOrID)
	row, err := r.host.db.Transact(func(tr fdb.Transaction) (ret interface{}, e error) {
		row, err := tr.Get(r.infoDir.Sub(keyRelHostCount).Pack(hostPrimary)).Get()
		if row == nil {
			return nil, ErrNotFound
		}
		return ToInt64(row), err
	})
	if err != nil {
		return int64(0), err
	}
	return row.(int64), err
}

// Getclients fetch slice of client objects using host
func (r *Relation) GetClients(objOrID interface{}, from interface{}, limit int) *Slice {
	dataKeys := []subspace.Subspace{}
	//primary := r.host.GetPrimaryField()
	//hostPrimary := primary.fromAnyInterface(objOrID)
	hostPrimary := r.host.getPrimaryTuple(objOrID)
	//hostSub := r.host.Subspace(objOrID)
	key := r.hostDir.Sub(hostPrimary...)
	resp, err := r.host.db.Transact(func(tr fdb.Transaction) (ret interface{}, e error) {
		start, end := key.FDBRangeKeys()
		if from != nil {
			start = key.Pack(r.client.getPrimaryTuple(from)) // add the last key fetched
		}
		iterator := tr.GetRange(fdb.KeyRange{Begin: start, End: end}, fdb.RangeOptions{
			Limit: limit,
		}).Iterator()
		needed := []fdb.RangeResult{}
		for iterator.Advance() {
			kv, err := iterator.Get()
			if err != nil {
				fmt.Printf("Unable to read next value: %v\n", err)
				return nil, err
			}
			keyTuple, err := key.Unpack(kv.Key)
			if err != nil {
				fmt.Printf("Unable to unpack index key: %v\n", err)
				return nil, err
			}
			l := len(keyTuple)
			if l < 1 {
				panic("empty key")
			}
			id := keyTuple[l-1]
			key := r.client.primary.Sub(id)
			needed = append(needed, NeedRange(tr, key))
			dataKeys = append(dataKeys, key)
		}
		return FetchRange(tr, needed)
	})
	return r.client.wrapRange(resp, err, dataKeys)
}

func (r *Relation) GetClientIDs(objOrID interface{}, from interface{}, limit int) *SliceIDs {
	//primary := r.host.GetPrimaryField()
	//hostPrimary := primary.fromAnyInterface(objOrID)
	hostPrimary := r.host.getPrimaryTuple(objOrID)
	sub := r.hostDir.Sub(hostPrimary...)

	resp, err := r.host.db.Transact(func(tr fdb.Transaction) (ret interface{}, e error) {
		start, end := sub.FDBRangeKeys()
		if from != nil {
			start = sub.Pack(r.client.getPrimaryTuple(from)) // add the last key fetched
		}
		iterator := tr.GetRange(fdb.KeyRange{Begin: start, End: end}, fdb.RangeOptions{
			Limit: limit,
		}).Iterator()
		slice := SliceIDs{}
		slice.init()
		for iterator.Advance() {
			kv, err := iterator.Get()
			if err != nil {
				fmt.Printf("Unable to read next value: %v\n", err)
				return nil, err
			}
			keyTuple, err := sub.Unpack(kv.Key)
			if err != nil {
				fmt.Printf("Unable to unpack index key: %v\n", err)
				return nil, err
			}
			slice.push(keyTuple, kv.Value)
		}
		return &slice, nil
	})
	if err != nil {
		return &SliceIDs{
			err: err,
		}
	}
	return resp.(*SliceIDs)
}

// Gethosts fetch slice of client objects using host
func (r *Relation) GetHosts(objOrID interface{}, from interface{}, limit int) *Slice {
	dataKeys := []subspace.Subspace{}
	//primary := r.client.GetPrimaryField()

	hostPrimary := r.client.getPrimaryTuple(objOrID)
	key := r.clientDir.Sub(hostPrimary...)
	resp, err := r.host.db.Transact(func(tr fdb.Transaction) (ret interface{}, e error) {
		start, end := key.FDBRangeKeys()
		if from != nil {
			start = key.Pack(r.host.getPrimaryTuple(from)) // add the last key fetched
		}
		iterator := tr.GetRange(fdb.KeyRange{Begin: start, End: end}, fdb.RangeOptions{
			Limit: limit,
		}).Iterator()
		needed := []fdb.RangeResult{}
		for iterator.Advance() {
			kv, err := iterator.Get()
			if err != nil {
				fmt.Printf("Unable to read next value: %v\n", err)
				return nil, err
			}
			keyTuple, err := key.Unpack(kv.Key)
			if err != nil {
				fmt.Printf("Unable to unpack index key: %v\n", err)
				return nil, err
			}
			l := len(keyTuple)
			if l < 1 {
				panic("empty key")
			}
			key := r.host.primary.Sub(keyTuple...)
			needed = append(needed, NeedRange(tr, key))
			dataKeys = append(dataKeys, key)
		}
		return FetchRange(tr, needed)
	})
	return r.host.wrapRange(resp, err, dataKeys)
}

func (r *Relation) UpdateHostData(hostOrID interface{}, clientOrID interface{}, hostVal []byte) error {
	hostPrimary, clientPrimary := r.getPrimary(hostOrID, clientOrID)
	_, err := r.host.db.Transact(func(tr fdb.Transaction) (ret interface{}, e error) {
		row, err := tr.Get(r.hostDir.Sub(hostPrimary...).Pack(clientPrimary)).Get()
		if err != nil {
			return nil, err
		}
		if row == nil {
			return nil, ErrNotFound
		}
		tr.Set(r.hostDir.Sub(hostPrimary...).Pack(clientPrimary), hostVal)
		return
	})
	return err
}
