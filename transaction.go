package stored

import "github.com/apple/foundationdb/bindings/go/src/fdb"

// Transaction allow you to join several Promises into one transaction
// also parallel executes promises effectively trying to parallel where its possible
type Transaction struct {
	Promises []*Promise
	ghosts   []func() *Promise
	db       *fdb.Database
	writable bool
	readTr   fdb.ReadTransaction
	tr       fdb.Transaction
	started  bool
	finish   bool
	err      error
}

func (t *Transaction) isReadOnly() bool {
	for _, promise := range t.Promises {
		if !promise.readOnly {
			return false
		}
	}
	return true
}

func (t *Transaction) clear() {
	for _, promise := range t.Promises {
		promise.clear()
	}
}

func (t *Transaction) initRead(tr fdb.ReadTransaction) {
	t.Promises = []*Promise{}
	t.ghosts = []func() *Promise{}
	t.readTr = tr
	t.started = true
}

func (t *Transaction) initWrite(tr fdb.Transaction) {
	t.Promises = []*Promise{}
	t.ghosts = []func() *Promise{}
	t.readTr = tr
	t.tr = tr
	t.writable = true
	t.started = true
}

func (t *Transaction) setTr(promise *Promise) {
	promise.tr = t.tr
	promise.readTr = t.readTr
}

func (t *Transaction) transact() {
	if t.finish { // transaction already executed
		return
	}
	if t.started {
		_, t.err = t.execute()
	}
	t.started = true

	db := t.db
	if t.isReadOnly() {
		_, t.err = db.ReadTransact(func(tr fdb.ReadTransaction) (interface{}, error) {
			t.clear()
			t.readTr = tr.Snapshot()
			return t.execute()
		})
	} else {
		_, t.err = db.Transact(func(tr fdb.Transaction) (ret interface{}, err error) {
			t.clear()
			t.tr = tr
			t.readTr = tr
			return t.execute()
		})
	}
}

func (t *Transaction) execute() (ret interface{}, err error) {
	chains := make([]Chain, len(t.Promises))
	for i, promise := range t.Promises {
		t.setTr(promise)
		chains[i] = promise.chain
	}
	t.finish = true
	next := true
	for next {
		next = false
		// go through all chain events
		for i, chain := range chains {
			if chain != nil {
				chains[i] = chain()
				// once error happened at any promise - transaction is failed
				if t.Promises[i].err != nil {
					err = t.Promises[i].err
					return
				}
				next = true
			}
		}
		// going through ghost promises to create real promises from functions
		if len(t.ghosts) != 0 {
			for _, ghost := range t.ghosts {
				ghostPromise := ghost()
				t.Promises = append(t.Promises, ghostPromise)
				t.setTr(ghostPromise)
				chains = append(chains, ghostPromise.chain())
				if ghostPromise.err != nil {
					err = ghostPromise.err
					return
				}
				next = true
			}
			t.ghosts = []func() *Promise{}
		}
	}
	return
}

// Err will perform all promises and return err if any of them failed
func (t *Transaction) Err() error {
	t.transact()
	return t.err
}

// Fail will set the transaction error, so
func (t *Transaction) Fail(err error) {
	t.err = err
	if t.writable {
		t.tr.Cancel()
	}
}
