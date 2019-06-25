package stored

import (
	"errors"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
)

// Chain is the recursive functions chain
type Chain func() Chain

// Promise is an basic promise object
type Promise struct {
	db        *fdb.Database
	readTr    fdb.ReadTransaction
	tr        fdb.Transaction
	chain     Chain
	after     func() PromiseAny
	err       error
	readOnly  bool
	resp      interface{}
	confirmed bool
}

// PromiseAny describes any type of promise
type PromiseAny interface {
	self() *Promise
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
	if p.after != nil {
		after := p.after().self()
		after.tr = p.tr
		after.readTr = p.readTr
		after.execute()
	}
	return p.resp, p.err
}

func (p *Promise) clear() {
	p.err = nil
	p.resp = nil
}

func (p *Promise) transact() (resp interface{}, err error) {
	if p.readTr != nil {
		p.clear()
		resp, err = p.execute()
		p.confirmed = true
		return
	}
	if p.readOnly {
		resp, err = p.db.ReadTransact(func(tr fdb.ReadTransaction) (interface{}, error) {
			p.clear() // since transaction could be repeated - should clear everything
			p.readTr = tr.Snapshot()
			return p.execute()
		})
		p.confirmed = true
		return

	}
	resp, err = p.db.Transact(func(tr fdb.Transaction) (ret interface{}, err error) {
		p.clear() // clear tmp data in case if transaction resended
		p.tr = tr
		p.readTr = tr
		return p.execute()
	})
	p.confirmed = true
	return
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

// After will perform an additional promise right after current one will be finised
// This works in transactions as well as in standalone promises, child promise will
// be executed in same transaction as parent
func (p *Promise) After(do func() PromiseAny) *Promise {
	p.after = do
	return p
}

// Check will perform promise in parallel with other promises whithin transaction
// without returning the result. But if Promise will return error full transaction
// will be cancelled and error will be returned
func (p *Promise) Check(t *Transaction) {
	t.tasks = append(t.tasks, transactionTask{
		promise: p,
		check:   true,
	})
}

// Try will perform promise in parallel with other promises within transaction
// without returning the result. But if Promise will return error, transaction will
// be performed as everythig is ok, error will be ignored
func (p *Promise) Try(t *Transaction) {
	t.tasks = append(t.tasks, transactionTask{
		promise: p,
		check:   false,
	})
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

// Promise will return promise from any type of promise
func (p *Promise) self() *Promise {
	return p
}
