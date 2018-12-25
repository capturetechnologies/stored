package stored

import (
	"errors"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
)

// Chain is the recursive functions chain
type Chain func() Chain

// Promise is an basic promise object
type Promise struct {
	db       *fdb.Database
	readTr   fdb.ReadTransaction
	tr       fdb.Transaction
	chain    Chain
	err      error
	readOnly bool
	resp     interface{}
}

func (p *Promise) do(chain Chain) {
	p.chain = chain
}

func (p *Promise) doRead(chain Chain) {
	p.readOnly = true
	p.chain = chain
}

func (p *Promise) fail(err error) Chain {
	p.err = err
	return nil
}

func (p *Promise) done(resp interface{}) Chain {
	p.resp = resp
	return nil
}

func (p *Promise) ok() Chain {
	return nil
}

func (p *Promise) getValueField(o *Object, field *Field, bytes []byte) *Value {
	raw := valueRaw{}
	//data := map[string]interface{}{}
	raw[field.Name] = bytes
	val := Value{
		object: o,
		raw:    raw,
	}
	return &val
}

func (p *Promise) execute() (interface{}, error) {
	next := p.chain()
	for next != nil {
		next = next()
	}
	return p.resp, p.err
}

func (p *Promise) clear() {
	p.err = nil
	p.resp = nil
}

func (p *Promise) transact() (interface{}, error) {
	if p.readTr != nil {
		p.clear()
		return p.execute()
	}
	if p.readOnly {
		return p.db.ReadTransact(func(tr fdb.ReadTransaction) (interface{}, error) {
			p.clear() // since transaction could be repeated - should clear everything
			p.readTr = tr.Snapshot()
			return p.execute()
		})
	}
	return p.db.Transact(func(tr fdb.Transaction) (ret interface{}, err error) {
		p.clear() // clear tmp data in case if transaction resended
		p.tr = tr
		p.readTr = tr
		return p.execute()
	})
}

// Err will execute the promise and return error
func (p *Promise) Err() error {
	_, err := p.transact()
	return err
}

// Bool return bool value if promise contins true or false
func (p *Promise) Bool() (bool, error) {
	data, err := p.transact()
	var res bool
	if err != nil {
		return res, err
	}
	if data == nil {
		panic("promise does not contain any value, use Scan")
	}
	res, ok := data.(bool)
	if !ok {
		return res, errors.New("promise value is not bool")
	}
	return res, nil
}

// Int64 return Int64 value if promise contin int64 data
func (p *Promise) Int64() (int64, error) {
	data, err := p.transact()
	var res int64
	if err != nil {
		return res, err
	}
	if data == nil {
		panic("promise does not contain any value, use Scan")
	}
	res, ok := data.(int64)
	if !ok {
		return res, errors.New("promise value is not int64")
	}
	return res, nil
}

// Join allow easely join two promises into the parallel Transaction
// FoundationDB garantees that ether both of requests will complete or none
func (p *Promise) Join(sidePromise *Promise) *Transaction {
	return &Transaction{
		Promises: []*Promise{p, sidePromise},
		db:       p.db,
	}
}

// JoinDo is the same as join, but its accept function instad which returns
// promise. Usefull if you whant to add some logic prior execution. Transaction
// will be started before logic execution.
// at the moment of do function execution, first step if parent promise will already
// be executed
func (p *Promise) JoinDo(do func() *Promise) *Transaction {
	return &Transaction{
		Promises: []*Promise{p},
		ghosts:   []func() *Promise{do},
		db:       p.db,
	}
}

// Check will perform promise in parallel with other promises whithin transaction
// without returning the result. But if Promise will return error full transaction
// will be cancelled and error will be returned
func (p *Promise) Check(t *Transaction) {
	t.Promises = append(t.Promises, p)
}

// Do will attach promise to transaction, so promise will be called within passed transaction
// Promise should be inside an transaction callback, because transaction could be resent
func (p *Promise) Do(t *Transaction) *Promise {
	if !t.started {
		panic("transaction not started, could not use in Promise")
	}
	p.tr = t.tr
	p.readTr = t.readTr
	return p
}
