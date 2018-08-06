package stored

import "github.com/apple/foundationdb/bindings/go/src/fdb"

// Chain is the recursive functions chain
type Chain func() Chain

// Promise is an basic promise object
type Promise struct {
	db       *fdb.Database
	tr       fdb.Transaction
	chain    Chain
	err      error
	readOnly bool
	value    *Value
	resp     interface{}
}

func (p *Promise) do(chain Chain) {
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

func (p *Promise) transact() (interface{}, error) {
	return p.db.Transact(func(tr fdb.Transaction) (ret interface{}, err error) {
		p.tr = tr
		next := p.chain()
		for next != nil {
			next = next()
		}
		return p.resp, p.err
	})
}

func (p *Promise) Scan(obj interface{}) error {
	_, err := p.transact()
	if err != nil {
		return err
	}
	if p.value == nil {
		panic("Scan couldn't be triggered because promise has no Value")
	}
	return p.value.Scan(obj)
}

// Err will execute the promise and return error
func (p *Promise) Err() error {
	_, err := p.transact()
	return err
}
