package stored

import (
	"errors"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/subspace"
)

type multiNeed struct {
	multi    *MultiChain
	object   *Object
	subspace subspace.Subspace
	res      *Value
}

// MultiChain allow to select multiple objects simultaniusly
type MultiChain struct {
	db          fdb.Database
	needed      map[string]multiNeed
	unprocessed int
}

func (m *MultiChain) init() {
	m.needed = map[string]multiNeed{}
}

func (m *MultiChain) execute() {
	m.db.ReadTransact(func(tr fdb.ReadTransaction) (interface{}, error) {
		tr = tr.Snapshot()
		results := map[string]*needObject{}
		for k, needObj := range m.needed {
			results[k] = needObj.object.need(tr, needObj.subspace)
		}
		for k, needObj := range m.needed {
			value, err := results[k].fetch()
			if err != nil {
				needObj.res.err = err
			} else {
				needObj.res.object = value.object
				needObj.res.raw = value.raw
			}
		}
		return nil, nil
	})
	m.unprocessed = 0
}

// Need means this object should be fetched
func (m *MultiChain) Need(o *Object, objOrID interface{}) *Value {
	sub := o.subspace(objOrID)
	needed := multiNeed{
		multi:    m,
		object:   o,
		subspace: sub,
	}
	m.unprocessed++
	val := Value{
		fetch: func() {
			if m.unprocessed > 0 {
				m.execute()
			}
		},
	}
	needed.res = &val
	m.needed[string(sub.Bytes())] = needed
	return &val
}

// Get wiil get all objects with this object
func (m *MultiChain) Get(o *Object, objOrID interface{}) *Value {
	sub := o.subspace(objOrID)
	needed, ok := m.needed[string(sub.Bytes())]
	if !ok {
		return &Value{err: errors.New("multiget object was not needed")}
	}
	return needed.res
}
