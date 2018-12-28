package stored

import (
	"fmt"
	"reflect"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
	"github.com/apple/foundationdb/bindings/go/src/fdb/subspace"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
)

// RelationN2N is main type of relation
var RelationN2N = 1

// list of service keys for shorter
var keyRelHostCount = "a"
var keyRelClientCount = "b"

// Relation is main struct to represent Relation
type Relation struct {
	host            *Object
	client          *Object
	hostDir         directory.DirectorySubspace
	clientDir       directory.DirectorySubspace
	infoDir         directory.DirectorySubspace
	kind            int
	counter         bool
	counterClient   *Field
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
	if host == client { // if same object use same directory
		r.clientDir = r.hostDir
	} else {
		r.clientDir, err = r.infoDir.CreateOrOpen(db, []string{"client"}, nil)
		if err != nil {
			panic(err)
		}
	}
	host.Relations = append(host.Relations, r)
}

func (r *Relation) panic(text string) {
	panic("relation between «" + r.host.name + "» and «" + r.client.name + "»: " + text)
}

// Counter will start count objects count within relation
func (r *Relation) Counter(on bool) {
	r.counter = on
}

// CounterClient will set external field
func (r *Relation) CounterClient(object *ObjectBuilder, fieldName string) {
	field := object.object.field(fieldName)
	if field.Kind != reflect.Int64 {
		r.panic("field «" + fieldName + "» should be int64 to work as counter")
	}
	r.counterClient = field
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

// HostData will set host data field name
func (r *Relation) HostData(fieldName string) {
	field := r.host.field(fieldName)
	r.hostDataField = field
}

// ClientData will set cliet data field name
func (r *Relation) ClientData(fieldName string) {
	field := r.client.field(fieldName)
	r.clientDataField = field
}

func (r *Relation) incClientCounter(clientPrimary tuple.Tuple, tr fdb.Transaction, incVal []byte) {
	if r.counterClient != nil {
		sub := r.counterClient.object.sub(clientPrimary)
		tr.Add(r.counterClient.getKey(sub), incVal)
	} else {
		tr.Add(r.infoDir.Sub(keyRelClientCount).Pack(clientPrimary), incVal)
	}
}

func (r *Relation) getClientCounter(clientPrimary tuple.Tuple, tr fdb.ReadTransaction) fdb.FutureByteSlice {
	if r.counterClient != nil {
		sub := r.counterClient.object.sub(clientPrimary)
		return tr.Get(r.counterClient.getKey(sub))
	}
	return tr.Get(r.infoDir.Sub(keyRelClientCount).Pack(clientPrimary))
}

// Add writes new relation beween objects (return ErrAlreadyExist if exists)
func (r *Relation) Add(hostOrID interface{}, clientOrID interface{}) *PromiseErr {
	hostPrimary, clientPrimary := r.getPrimary(hostOrID, clientOrID)
	p := r.host.promiseErr()
	p.do(func() Chain {
		val, err := p.tr.Get(r.hostDir.Sub(hostPrimary...).Pack(clientPrimary)).Get()
		if err != nil {
			p.fail(err)
		}
		if val != nil { // already exists
			p.fail(ErrAlreadyExist)
		}
		if r.counter { // increment if not exists
			p.tr.Add(r.infoDir.Sub(keyRelHostCount).Pack(hostPrimary), countInc)
			r.incClientCounter(clientPrimary, p.tr, countInc)
		}

		// getting data to store inside relation kv
		hostVal, clientVal, dataErr := r.getData(hostOrID, clientOrID)
		if dataErr != nil {
			p.fail(dataErr)
		}

		p.tr.Set(r.hostDir.Sub(hostPrimary...).Pack(clientPrimary), clientVal)
		p.tr.Set(r.clientDir.Sub(clientPrimary...).Pack(hostPrimary), hostVal)
		return p.ok()
	})
	return p
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
				//tr.Add(r.infoDir.Sub(keyRelClientCount).Pack(clientPrimary), countInc)
				r.incClientCounter(clientPrimary, tr, countInc)
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
func (r *Relation) Delete(hostOrID interface{}, clientOrID interface{}) *PromiseErr {
	hostPrimary, clientPrimary := r.getPrimary(hostOrID, clientOrID)
	p := r.host.promiseErr()
	p.do(func() Chain {
		if r.counter { // increment if not exists
			val, err := p.tr.Get(r.hostDir.Sub(hostPrimary...).Pack(clientPrimary)).Get()
			if err != nil {
				return p.fail(err)
			}
			if val != nil { // exists decrement here
				p.tr.Add(r.infoDir.Sub(keyRelHostCount).Pack(hostPrimary), countDec)
				//tr.Add(r.infoDir.Sub(keyRelClientCount).Pack(clientPrimary), countDec)
				r.incClientCounter(clientPrimary, p.tr, countDec)
			}
		}
		p.tr.Clear(r.hostDir.Sub(hostPrimary...).Pack(clientPrimary))
		p.tr.Clear(r.clientDir.Sub(clientPrimary...).Pack(hostPrimary))
		return p.ok()
	})
	return p
}

// GetHostsCount fetches counter
func (r *Relation) GetHostsCount(clientOrID interface{}) *Promise {
	if !r.counter {
		r.panic("counter is off, use rel.Counter(true)")
	}

	p := r.host.promiseInt64()
	p.doRead(func() Chain {
		clientPrimary := r.host.getPrimaryTuple(clientOrID)
		//row, err := r.host.db.Transact(func(tr fdb.Transaction) (ret interface{}, e error) {
		//row, err := p.readTr.Get(r.infoDir.Sub(keyRelClientCount).Pack(clientPrimary)).Get()
		row, err := r.getClientCounter(clientPrimary, p.readTr).Get()

		if row == nil {
			return p.fail(ErrNotFound)
			//return nil, ErrNotFound
		}
		if err != nil {
			return p.fail(err)
		}
		return p.done(ToInt64(row))
	})
	return p
}

// SetHostsCounterUnsafe set hosts counter unsafely. User with care
func (r *Relation) SetHostsCounterUnsafe(clientObject interface{}, count int64) error {
	clientPrimary := r.client.getPrimaryTuple(clientObject)
	_, err := r.client.db.Transact(func(tr fdb.Transaction) (ret interface{}, e error) {
		if r.counterClient != nil {
			sub := r.counterClient.object.sub(clientPrimary)
			tr.Set(r.counterClient.getKey(sub), Int64(count))
		} else {
			tr.Set(r.infoDir.Sub(keyRelClientCount).Pack(clientPrimary), Int64(count))
		}
		return
	})
	return err
}

// GetClientsCount fetches counter
func (r *Relation) GetClientsCount(hostOrID interface{}) *Promise {
	if !r.counter {
		r.panic("counter is off, use rel.Counter(true)")
	}
	p := r.host.promiseInt64()
	p.doRead(func() Chain {
		hostPrimary := r.client.getPrimaryTuple(hostOrID)
		//row, err := r.host.db.Transact(func(tr fdb.Transaction) (ret interface{}, e error) {
		row, err := p.readTr.Get(r.infoDir.Sub(keyRelHostCount).Pack(hostPrimary)).Get()
		if row == nil {
			return p.fail(ErrNotFound)
			//return nil, ErrNotFound
		}
		if err != nil {
			return p.fail(err)
		}
		return p.done(ToInt64(row))
	})
	return p
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

// GetClientIDs will fetch only primary values of client objects
func (r *Relation) GetClientIDs(objOrID interface{}, from interface{}, limit int) *SliceIDs {
	hostPrimary := r.host.getPrimaryTuple(objOrID)
	sub := r.hostDir.Sub(hostPrimary...)
	return r.getSliceIDs(r.host, r.client, r.clientDataField, sub, from, limit)
}

// GetHostIDs will fetch only primary values of host objects
func (r *Relation) GetHostIDs(objOrID interface{}, from interface{}, limit int) *SliceIDs {
	clientPrimary := r.client.getPrimaryTuple(objOrID)
	sub := r.clientDir.Sub(clientPrimary...)
	return r.getSliceIDs(r.client, r.host, r.hostDataField, sub, from, limit)
}

// GetHosts fetch slice of client objects using host
func (r *Relation) GetHosts(objOrID interface{}, from interface{}, limit int) *Slice {
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
func (r *Relation) UpdateHostData(hostObj interface{}, clientOrID interface{}) *Promise {
	hostPrimary, clientPrimary := r.getPrimary(hostObj, clientOrID)
	p := r.client.promise()
	p.do(func() Chain {
		//_, err := r.host.db.Transact(func(tr fdb.Transaction) (ret interface{}, e error) {
		dataGet := p.tr.Get(r.clientDir.Sub(clientPrimary...).Pack(hostPrimary))
		row, err := dataGet.Get()
		if err != nil {
			return p.fail(err)
		}
		if row == nil {
			return p.fail(ErrNotFound)
		}

		// getting data to store inside relation kv
		hostVal, dataErr := r.getHostDataBytes(hostObj)
		if dataErr != nil {
			return p.fail(dataErr)
		}

		p.tr.Set(r.clientDir.Sub(clientPrimary...).Pack(hostPrimary), hostVal)
		return p.done(nil)
	})
	return p
}

// UpdateClientData writed new data from host object (host data could be )
func (r *Relation) UpdateClientData(hostOrID interface{}, clientObj interface{}) *Promise {
	hostPrimary, clientPrimary := r.getPrimary(hostOrID, clientObj)
	p := r.client.promise()
	p.do(func() Chain {
		dataGet := p.tr.Get(r.hostDir.Sub(hostPrimary...).Pack(clientPrimary))
		row, err := dataGet.Get()
		if err != nil {
			return p.fail(err)
		}
		if row == nil {
			fmt.Println("Catched error")
			return p.fail(ErrNotFound)
		}

		// getting data to store inside relation kv
		clientVal, dataErr := r.getClientDataBytes(clientObj)
		if dataErr != nil {
			return p.fail(dataErr)
		}
		//fmt.Println("[CLIENT DATA]", hostPrimary, clientPrimary, "=>", clientVal)

		p.tr.Set(r.hostDir.Sub(hostPrimary...).Pack(clientPrimary), clientVal)
		return p.done(nil)
	})
	return p
}

// UpdateData will atomicly update host and client index storage data using callback
func (r *Relation) UpdateData(hostObj interface{}, clientObj interface{}, callback func()) *Promise {
	hostEditable := structEditable(hostObj)
	clientEditable := structEditable(clientObj)
	hostPrimary := hostEditable.getPrimary(r.host)
	clientPrimary := clientEditable.getPrimary(r.client)

	//hostPrimary, clientPrimary := r.getPrimary(hostObj, clientObj)
	p := r.client.promise()
	p.do(func() Chain {
		hostDataGet := p.tr.Get(r.clientDir.Sub(clientPrimary...).Pack(hostPrimary))
		clientDataGet := p.tr.Get(r.hostDir.Sub(hostPrimary...).Pack(clientPrimary))
		return func() Chain {
			hostData, hostErr := hostDataGet.Get()
			if hostErr != nil {
				return p.fail(hostErr)
			}
			clientData, clientErr := clientDataGet.Get()
			if clientErr != nil {
				return p.fail(clientErr)
			}
			if hostData == nil || clientData == nil {
				return p.fail(ErrNotFound)
			}

			//var hostVal, clientVal []byte
			//var err error

			hostEditable.setField(r.hostDataField, hostData)
			clientEditable.setField(r.clientDataField, clientData)
			callback()
			hostVal := hostEditable.GetBytes(r.hostDataField)
			clientVal := clientEditable.GetBytes(r.clientDataField)
			/*hostVal, err = r.hostDataField.ToBytes(hostI)
			if err != nil {
				return p.fail(err)
			}
			clientVal, err = r.clientDataField.ToBytes(clientI)
			if err != nil {
				return p.fail(err)
			}*/

			p.tr.Set(r.clientDir.Sub(clientPrimary...).Pack(hostPrimary), hostVal)
			p.tr.Set(r.hostDir.Sub(hostPrimary...).Pack(clientPrimary), clientVal)
			return p.done(nil)
		}
	})
	return p
}

// SetData writed new data for both host and client index storages, return fail if object nor exist
func (r *Relation) SetData(hostObj interface{}, clientObj interface{}) *Promise {
	hostPrimary, clientPrimary := r.getPrimary(hostObj, clientObj)
	p := r.client.promise()
	p.do(func() Chain {
		dataGet := p.tr.Get(r.clientDir.Sub(clientPrimary...).Pack(hostPrimary))
		return func() Chain {
			row, err := dataGet.Get()
			if err != nil {
				return p.fail(err)
			}
			if row == nil {
				return p.fail(ErrNotFound)
			}

			// getting data to store inside relation kv
			hostVal, dataErr := r.getHostDataBytes(hostObj)
			if dataErr != nil {
				return p.fail(dataErr)
			}

			p.tr.Set(r.clientDir.Sub(clientPrimary...).Pack(hostPrimary), hostVal)

			clientVal, dataErr := r.getClientDataBytes(clientObj)
			if dataErr != nil {
				return p.fail(dataErr)
			}
			p.tr.Set(r.hostDir.Sub(hostPrimary...).Pack(clientPrimary), clientVal)
			return p.done(nil)
		}
	})
	return p
}

// Check return true if relation is set (false) if not set
func (r *Relation) Check(hostOrID interface{}, clientOrID interface{}) *Promise {
	hostPrimary, clientPrimary := r.getPrimary(hostOrID, clientOrID)
	p := r.host.promise()
	p.doRead(func() Chain {
		checkGet := p.readTr.Get(r.hostDir.Sub(hostPrimary...).Pack(clientPrimary))
		return func() Chain {
			val, err := checkGet.Get()
			if err != nil {
				return p.fail(err)
			}
			if val == nil { // not exists increment here
				return p.done(false)
			}
			return p.done(true)
		}
	})
	return p
	/*val, err := r.host.db.Transact(func(tr fdb.Transaction) (ret interface{}, e error) {
		val, err := tr.Get(r.hostDir.Sub(hostPrimary...).Pack(clientPrimary)).Get()
		if err != nil {
			return false, err
		}
		if val == nil { // not exists increment here
			return false, nil
		}
		return true, nil
	})
	return val.(bool), err*/
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
func (r *Relation) GetClientData(hostOrID interface{}, clientOrID interface{}) *PromiseValue {
	hostPrimary, clientPrimary := r.getPrimary(hostOrID, clientOrID)
	p := r.host.promiseValue()
	p.doRead(func() Chain {
		fetching := p.readTr.Get(r.hostDir.Sub(hostPrimary...).Pack(clientPrimary))
		return func() Chain {
			val, err := fetching.Get()
			if err != nil {
				return p.fail(err)
			}
			//fmt.Println("CLIENT DATA", hostPrimary, clientPrimary, "=>", val)
			if val == nil { // not exists increment here
				return p.fail(ErrNotFound)
			}
			raw := valueRaw{}
			//data := map[string]interface{}{}
			//valueInterface := r.clientDataField.ToInterface(val)
			//data[r.clientDataField.Name] = valueInterface
			raw[r.clientDataField.Name] = val

			value := Value{
				object: r.client,
				raw:    raw,
			}
			return p.done(&value)
		}
	})
	return p
}

// GetHostData fetch client data
func (r *Relation) GetHostData(hostOrID interface{}, clientOrID interface{}) *PromiseValue {
	hostPrimary, clientPrimary := r.getPrimary(hostOrID, clientOrID)
	p := r.host.promiseValue()
	p.doRead(func() Chain {
		val, err := p.readTr.Get(r.clientDir.Sub(clientPrimary...).Pack(hostPrimary)).Get()
		if err != nil {
			return p.fail(err)
		}
		if val == nil { // not exists increment here
			return p.fail(ErrNotFound)
		}
		return func() Chain {
			return p.done(p.getValueField(r.host, r.hostDataField, val))
		}
	})
	return p
}
