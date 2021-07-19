package stored

import (
	"fmt"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
)

type transactionTask struct {
	promise *Promise
	onDone  func(err error) error
	check   bool
}

// Transaction allow you to join several Promises into one transaction
// also parallel executes promises effectively trying to parallel where its possible
type Transaction struct {
	tasks    []transactionTask
	db       *fdb.Database
	writable bool
	readTr   fdb.ReadTransaction
	tr       fdb.Transaction
	started  bool
	finish   bool
	err      error
}

func (t *Transaction) isReadOnly() bool {
	for _, task := range t.tasks {
		if !task.promise.readOnly {
			return false
		}
	}
	return true
}

func (t *Transaction) clear() {
	for _, task := range t.tasks {
		task.promise.clear()
	}
}

func (t *Transaction) initRead(tr fdb.ReadTransaction) {
	t.tasks = []transactionTask{}
	t.readTr = tr
	t.started = true
}

func (t *Transaction) initWrite(tr fdb.Transaction) {
	t.tasks = []transactionTask{}
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
		_, err := t.execute()
		if t.err == nil {
			t.err = err
		}
		return
	}
	t.started = true

	db := t.db
	var err error
	if t.isReadOnly() {
		_, err = db.ReadTransact(func(tr fdb.ReadTransaction) (interface{}, error) {
			t.clear()
			t.readTr = tr.Snapshot()
			return t.execute()
		})
		t.confirm()
	} else {
		_, err = db.Transact(func(tr fdb.Transaction) (ret interface{}, err error) {
			t.clear()
			t.tr = tr
			t.readTr = tr
			return t.execute()
		})
		t.confirm()
	}
	if t.err == nil {
		t.err = err
	}
}

// will set all promises as confirmed
func (t *Transaction) confirm() {
	for _, task := range t.tasks {
		task.promise.confirmed = true
	}
}

func (t *Transaction) execute() (ret interface{}, err error) {
	chains := make([]Chain, len(t.tasks))
	for i, task := range t.tasks {
		t.setTr(task.promise)
		chains[i] = task.promise.chain
	}
	t.finish = true
	next := true
	for next {
		next = false
		// go through all chain events
		for i, chain := range chains {
			task := t.tasks[i]
			promise := task.promise
			if chain != nil {
				chains[i] = chain()
				// once error happened at any promise - transaction is failed
				if promise.err != nil && promise.err != ErrSkip {
					promise.after = nil // no after in that case
					if task.check {
						err = promise.err
						fmt.Println("PROMISE ERERRR", err)
						return
					}
				}
				next = true
			} else { // if promise is done we chan check for postponed relative promises
				if promise.after != nil {
					after := promise.after().self()
					promise.after = nil
					t.setTr(after)
					t.tasks = append(t.tasks, transactionTask{promise: after})
					chains = append(chains, after.chain)
					next = true
				}
				if task.onDone != nil {
					promise.err = task.onDone(promise.err)
					if promise.err != nil {
						return // cancel the transaction
					}

				}
			}
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
