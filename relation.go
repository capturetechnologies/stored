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
	host            *Object
	client          *Object
	hostDir         directory.DirectorySubspace
	clientDir       directory.DirectorySubspace
	infoDir         directory.DirectorySubspace
	kind            int
	counter         bool
	hostDataField   *Field
	clientDataField *Field
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

// getHostData fetches data from object
func (r *Relation) getHostDataBytes(hostObj interface{}) (hostVal []byte, err error) {
	if r.hostDataField != nil {
		hostVal, err = r.hostDataField.BytesFromObject(hostObj)
		if err != nil {
			return
		}
	} else {
		hostVal = []byte{}
	}
	return
}

// getClientData fetches data from object
func (r *Relation) getClientDataBytes(clientObj interface{}) (clientVal []byte, err error) {
	if r.clientDataField != nil {
		clientVal, err = r.clientDataField.BytesFromObject(clientObj)
		if err != nil {
			return
		}
	} else {
		clientVal = []byte{}
	}
	return
}

// getData fetches data from object
func (r *Relation) getData(hostObj interface{}, clientObj interface{}) (hostVal, clientVal []byte, err error) {
	hostVal, err = r.getHostDataBytes(hostObj)
	if err != nil {
		return
	}
	clientVal, err = r.getClientDataBytes(clientObj)
	if err != nil {
		return
	}
	return
}

func (r *Relation) HostData(fieldName string) {
	field := r.host.field(fieldName)
	r.hostDataField = field
}

func (r *Relation) ClientData(fieldName string) {
	field := r.client.field(fieldName)
	r.clientDataField = field
}

// Add writes new relation beween objects (return ErrAlreadyExist if exists)
func (r *Relation) Add(hostOrID interface{}, clientOrID interface{}) error {
	hostPrimary, clientPrimary := r.getPrimary(hostOrID, clientOrID)
	_, err := r.host.db.Transact(func(tr fdb.Transaction) (ret interface{}, e error) {
		val, err := tr.Get(r.hostDir.Sub(hostPrimary...).Pack(clientPrimary)).Get()
		if err != nil {
			return nil, err
		}
		if val != nil { // already exists
			return nil, ErrAlreadyExist
		}
		if r.counter { // increment if not exists
			tr.Add(r.infoDir.Sub(keyRelHostCount).Pack(hostPrimary), countInc)
			tr.Add(r.infoDir.Sub(keyRelClientCount).Pack(clientPrimary), countInc)
		}

		// getting data to store inside relation kv
		hostVal, clientVal, dataErr := r.getData(hostOrID, clientOrID)
		if dataErr != nil {
			return nil, dataErr
		}

		tr.Set(r.hostDir.Sub(hostPrimary...).Pack(clientPrimary), clientVal)
		tr.Set(r.clientDir.Sub(clientPrimary...).Pack(hostPrimary), hostVal)
		return
	})
	return err
}

// Set writes new relation beween objects, you could use objects or values with same types as primary key
func (r *Relation) Set(hostOrID interface{}, clientOrID interface{}) error {
	hostPrimary, clientPrimary := r.getPrimary(hostOrID, clientOrID)
	_, err := r.host.db.Transact(func(tr fdb.Transaction) (ret interface{}, e error) {
		if r.counter { // increment if not exists
			val, err := tr.Get(r.hostDir.Sub(hostPrimary...).Pack(clientPrimary)).Get()
			if err != nil {
				return nil, err
			}
			if val == nil { // not exists increment here
				tr.Add(r.infoDir.Sub(keyRelHostCount).Pack(hostPrimary), countInc)
				tr.Add(r.infoDir.Sub(keyRelClientCount).Pack(clientPrimary), countInc)
			}
		}

		// getting data to store inside relation kv
		hostVal, clientVal, dataErr := r.getData(hostOrID, clientOrID)
		if dataErr != nil {
			return nil, dataErr
		}

		tr.Set(r.hostDir.Sub(hostPrimary...).Pack(clientPrimary), clientVal)
		tr.Set(r.clientDir.Sub(clientPrimary...).Pack(hostPrimary), hostVal)
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

// GetClients fetch slice of client objects using host
func (r *Relation) GetClients(objOrID interface{}, from interface{}, limit int) *Slice {
	hostPrimary := r.host.getPrimaryTuple(objOrID)

	key := r.hostDir.Sub(hostPrimary...)
	resp, err := r.host.db.Transact(func(tr fdb.Transaction) (ret interface{}, e error) {
		indexData := [][]byte{}
		start, end := key.FDBRangeKeys()
		if from != nil {
			start = key.Pack(r.client.getPrimaryTuple(from)) // add the last key fetched
		}
		iterator := tr.GetRange(fdb.KeyRange{Begin: start, End: end}, fdb.RangeOptions{
			Limit: limit,
		}).Iterator()
		//needed := []fdb.RangeResult{}
		needed := []*needObject{}
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
			//key := r.client.primary.Sub(id)
			needed = append(needed, r.client.need(tr, r.client.primary.Sub(id)))
			//dataKeys = append(dataKeys, key)
			indexData = append(indexData, kv.Value)
		}
		slice := r.client.wrapRange(needed)
		if r.clientDataField != nil {
			slice.fillFieldData(r.clientDataField, indexData)
		}
		//slice.indexData = indexData
		/*kv, err := FetchRange(tr, needed)
		if err != nil {
			fmt.Printf("Fetch range error: %v\n", err)
			return nil, err
		}
		slice := r.client.wrapRange(kv, dataKeys)
		slice.indexData = indexData*/
		return slice, nil
	})
	if err != nil {
		return &Slice{err: err}
	}

	return resp.(*Slice)
}

func (r *Relation) getSliceIDs(objFrom *Object, objRet *Object, dataField *Field, sub subspace.Subspace, from interface{}, limit int) *SliceIDs {
	resp, err := objFrom.db.Transact(func(tr fdb.Transaction) (ret interface{}, e error) {
		start, end := sub.FDBRangeKeys()
		if from != nil {
			start = sub.Pack(objRet.getPrimaryTuple(from)) // add the last key fetched
		}
		iterator := tr.GetRange(fdb.KeyRange{Begin: start, End: end}, fdb.RangeOptions{
			Limit: limit,
		}).Iterator()
		slice := SliceIDs{}
		slice.init(objRet)
		slice.dataField = dataField
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
			object: objRet,
			err:    err,
		}
	}
	return resp.(*SliceIDs)
}

func (r *Relation) GetClientIDs(objOrID interface{}, from interface{}, limit int) *SliceIDs {
	hostPrimary := r.host.getPrimaryTuple(objOrID)
	sub := r.hostDir.Sub(hostPrimary...)
	return r.getSliceIDs(r.host, r.client, r.clientDataField, sub, from, limit)
}

func (r *Relation) GetHostIDs(objOrID interface{}, from interface{}, limit int) *SliceIDs {
	clientPrimary := r.client.getPrimaryTuple(objOrID)
	sub := r.clientDir.Sub(clientPrimary...)
	return r.getSliceIDs(r.client, r.host, r.hostDataField, sub, from, limit)
}

// GetHosts fetch slice of client objects using host
func (r *Relation) GetHosts(objOrID interface{}, from interface{}, limit int) *Slice {
	//dataKeys := []subspace.Subspace{}
	//primary := r.client.GetPrimaryField()

	hostPrimary := r.client.getPrimaryTuple(objOrID)
	key := r.clientDir.Sub(hostPrimary...)
	resp, err := r.host.db.Transact(func(tr fdb.Transaction) (ret interface{}, e error) {
		indexData := [][]byte{}
		start, end := key.FDBRangeKeys()
		if from != nil {
			start = key.Pack(r.host.getPrimaryTuple(from)) // add the last key fetched
		}
		iterator := tr.GetRange(fdb.KeyRange{Begin: start, End: end}, fdb.RangeOptions{
			Limit: limit,
		}).Iterator()
		//needed := []fdb.RangeResult{}
		needed := []*needObject{}
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
			r.host.need(tr, r.host.sub(keyTuple))
			//key := r.host.primary.Sub(keyTuple...)
			needed = append(needed, r.host.need(tr, r.host.sub(keyTuple)))
			//dataKeys = append(dataKeys, key)
			indexData = append(indexData, kv.Value)
		}
		slice := r.host.wrapRange(needed)
		if r.hostDataField != nil {
			slice.fillFieldData(r.hostDataField, indexData)
		}
		return slice, nil
		/*kv, err := FetchRange(tr, needed)
		if err != nil {
			return nil, err
		}
		return r.host.wrapRange(kv, dataKeys), nil*/
	})
	if err != nil {
		return &Slice{err: err}
	}
	return resp.(*Slice)
}

// UpdateHostData writed new data from host object (host data could be )
func (r *Relation) UpdateHostData(hostObj interface{}, clientOrID interface{}) error {
	hostPrimary, clientPrimary := r.getPrimary(hostObj, clientOrID)
	_, err := r.host.db.Transact(func(tr fdb.Transaction) (ret interface{}, e error) {
		row, err := tr.Get(r.clientDir.Sub(clientPrimary...).Pack(hostPrimary)).Get()
		if err != nil {
			return nil, err
		}
		if row == nil {
			return nil, ErrNotFound
		}

		// getting data to store inside relation kv
		hostVal, dataErr := r.getHostDataBytes(hostObj)
		if dataErr != nil {
			return nil, dataErr
		}

		tr.Set(r.clientDir.Sub(clientPrimary...).Pack(hostPrimary), hostVal)
		return
	})
	return err
}

// UpdateClientData writed new data from host object (host data could be )
func (r *Relation) UpdateClientData(hostOrID interface{}, clientObj interface{}) error {
	hostPrimary, clientPrimary := r.getPrimary(hostOrID, clientObj)
	_, err := r.host.db.Transact(func(tr fdb.Transaction) (ret interface{}, e error) {
		row, err := tr.Get(r.hostDir.Sub(hostPrimary...).Pack(clientPrimary)).Get()
		if err != nil {
			return nil, err
		}
		if row == nil {
			fmt.Println("Catched error")
			return nil, ErrNotFound
		}

		// getting data to store inside relation kv
		clientVal, dataErr := r.getClientDataBytes(clientObj)
		if dataErr != nil {
			return nil, dataErr
		}
		fmt.Println("[CLIENT DATA]", hostPrimary, clientPrimary, "=>", clientVal)

		tr.Set(r.hostDir.Sub(hostPrimary...).Pack(clientPrimary), clientVal)
		return
	})
	return err
}

// Check return true if relation is set (false) if not set
func (r *Relation) Check(hostOrID interface{}, clientOrID interface{}) (bool, error) {
	hostPrimary, clientPrimary := r.getPrimary(hostOrID, clientOrID)
	val, err := r.host.db.Transact(func(tr fdb.Transaction) (ret interface{}, e error) {
		val, err := tr.Get(r.hostDir.Sub(hostPrimary...).Pack(clientPrimary)).Get()
		if err != nil {
			return false, err
		}
		if val == nil { // not exists increment here
			return false, nil
		}
		return true, nil
	})
	return val.(bool), err
}

// GetClientDataIDs returns client data bytes
func (r *Relation) GetClientDataIDs(hostOrID interface{}, clientOrID interface{}) ([]byte, error) {
	hostPrimary, clientPrimary := r.getPrimary(hostOrID, clientOrID)
	val, err := r.host.db.Transact(func(tr fdb.Transaction) (ret interface{}, e error) {
		val, err := tr.Get(r.hostDir.Sub(hostPrimary...).Pack(clientPrimary)).Get()
		if err != nil {
			return Nan, err
		}
		if val == nil { // not exists increment here
			return Nan, ErrNotFound
		}
		return val, nil
	})
	return val.([]byte), err
}

// GetClientData fetch client data
func (r *Relation) GetClientData(hostOrID interface{}, clientOrID interface{}) *Promise {
	hostPrimary, clientPrimary := r.getPrimary(hostOrID, clientOrID)
	p := r.host.promise()
	p.do(func() Chain {
		fetching := p.tr.Get(r.hostDir.Sub(hostPrimary...).Pack(clientPrimary))
		return func() Chain {
			val, err := fetching.Get()
			if err != nil {
				return p.fail(err)
			}
			fmt.Println("CLIENT DATA", hostPrimary, clientPrimary, "=>", val)
			if val == nil { // not exists increment here
				return p.fail(ErrNotFound)
			}
			data := map[string]interface{}{}
			valueInterface := r.clientDataField.ToInterface(val)
			data[r.clientDataField.Name] = valueInterface
			value := Value{
				object: r.client,
				data:   data,
			}
			p.value = &value
			return p.done(nil)
		}
	})
	return p
}

// GetHostData fetch client data
func (r *Relation) GetHostData(hostOrID interface{}, clientOrID interface{}) *Promise {
	hostPrimary, clientPrimary := r.getPrimary(hostOrID, clientOrID)
	p := r.host.promise()
	p.do(func() Chain {
		val, err := p.tr.Get(r.clientDir.Sub(clientPrimary...).Pack(hostPrimary)).Get()
		if err != nil {
			return p.fail(err)
		}
		if val == nil { // not exists increment here
			return p.fail(ErrNotFound)
		}
		return func() Chain {
			data := map[string]interface{}{}
			valueInterface := r.hostDataField.ToInterface(val)
			data[r.hostDataField.Name] = valueInterface
			val := Value{
				object: r.host,
				data:   data,
			}
			p.value = &val
			return p.done(nil)
		}
	})
	return p
}
