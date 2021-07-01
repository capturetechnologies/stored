package stored

// PromiseSlice is implements everything promise implements but more
type PromiseSlice struct {
	Promise
	limit   int
	reverse bool
	onDone  func() // function which will be called once the promise is finished
}

// CheckAll will perform promise in parallel with other promises whithin transaction
// without returning the result. But if Promise will return error full transaction
// will be cancelled and error will be returned.
// Only when promise will be finished – slicePointer will be filled with data.
func (p *PromiseSlice) CheckAll(t *Transaction, slicePointer interface{}) {
	t.tasks = append(t.tasks, transactionTask{
		promise: &p.Promise,
		check:   true,
	})
	p.onDone = func() {
		slice := p.resp.(*Slice)
		slice.ScanAll(slicePointer)
	}
}

// TryAll will perform promise in parallel with other promises within transaction
// without returning the result. But if Promise will return error, transaction will
// be performed as everythig is ok, error will be ignored.
// Only when promise will be finished – slicePointer will be filled with data.
func (p *PromiseSlice) TryAll(t *Transaction, slicePointer interface{}) {
	t.tasks = append(t.tasks, transactionTask{
		promise: &p.Promise,
	})
	p.onDone = func() {
		slice := p.resp.(*Slice)
		slice.ScanAll(slicePointer)
	}
}

// ScanAll values inside promise
func (p *PromiseSlice) ScanAll(slicePointer interface{}) error {
	if !p.confirmed {
		p.transact()
	}
	if p.err != nil {
		return p.err
	}
	slice := p.resp.(*Slice)
	/*if !ok {
		sliceOrig := p.resp.(Slice)
		slice = &sliceOrig
	}*/
	return slice.ScanAll(slicePointer)
}

// Slice will return slice pointer
func (p *PromiseSlice) Slice() *Slice {
	if !p.confirmed {
		p.transact()
	}
	if p.err != nil {
		return &Slice{err: p.err}
	}
	slice := p.resp.(*Slice)
	return slice
}

// Do will attach promise to transaction, so promise will be called within passed transaction
// Promise should be inside an transaction callback, because transaction could be resent
func (p *PromiseSlice) Do(t *Transaction) *PromiseSlice {
	if !t.started {
		panic("transaction not started, could not use in Promise")
	}
	p.tr = t.tr
	p.readTr = t.readTr
	return p
}

// done will call original promise done, but also will call onDone handler
func (p *PromiseSlice) done(resp interface{}) Chain {
	r := p.Promise.done(resp)
	if p.onDone != nil {
		p.onDone()
	}
	return r
}

// Limit is meant to set limit of the query this
func (p *PromiseSlice) Limit(limit int) *PromiseSlice {
	p.limit = limit
	return p
}

// Reverse allow to reverse value of slice query if querying function support this
func (p *PromiseSlice) Reverse(reverse bool) *PromiseSlice {
	p.reverse = reverse
	return p
}
