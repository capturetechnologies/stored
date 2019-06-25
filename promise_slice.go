package stored

// PromiseSlice is implements everything promise implements but more
type PromiseSlice struct {
	Promise
	limit   int
	reverse bool
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
