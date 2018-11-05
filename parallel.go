package stored

import "github.com/apple/foundationdb/bindings/go/src/fdb"

// Parallel allow you to join several Promises into one transaction
// also parallel executes promises effectively trying to parallel where its possible
type Parallel struct {
	Promises []*Promise
	ghosts   []func() *Promise
	db       *fdb.Database
	readTr   fdb.ReadTransaction
	tr       fdb.Transaction
}

func (p *Parallel) isReadOnly() bool {
	for _, promise := range p.Promises {
		if !promise.readOnly {
			return false
		}
	}
	return true
}

func (p *Parallel) clear() {
	for _, promise := range p.Promises {
		promise.clear()
	}
}

func (p *Parallel) setTr(promise *Promise) {
	promise.tr = p.tr
	promise.readTr = p.readTr
}

func (p *Parallel) transact() (interface{}, error) {
	db := p.db
	if p.isReadOnly() {
		return db.ReadTransact(func(tr fdb.ReadTransaction) (interface{}, error) {
			p.clear()
			p.readTr = tr.Snapshot()
			return p.execute()
		})
	}
	return db.Transact(func(tr fdb.Transaction) (ret interface{}, err error) {
		p.clear()
		p.tr = tr
		p.readTr = tr
		return p.execute()
	})
}

func (p *Parallel) execute() (ret interface{}, err error) {
	chains := make([]Chain, len(p.Promises))
	for i, promise := range p.Promises {
		p.setTr(promise)
		chains[i] = promise.chain
	}
	next := true
	for next {
		next = false
		// go through all chain events
		for i, chain := range chains {
			if chain != nil {
				chains[i] = chain()
				// once error happened at any promise - transaction is failed
				if p.Promises[i].err != nil {
					err = p.Promises[i].err
					return
				}
				next = true
			}
		}
		// going through ghost promises to create real promises from functions
		if len(p.ghosts) != 0 {
			for _, ghost := range p.ghosts {
				ghostPromise := ghost()
				p.Promises = append(p.Promises, ghostPromise)
				p.setTr(ghostPromise)
				chains = append(chains, ghostPromise.chain())
				if ghostPromise.err != nil {
					err = ghostPromise.err
					return
				}
				next = true
			}
			p.ghosts = []func() *Promise{}
		}
	}
	return
}

// Err will perform all promises and return err if any of them failed
func (p *Parallel) Err() error {
	_, err := p.transact()
	return err
}
