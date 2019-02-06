package stored

// PromiseSlice is implements everything promise implements but more
type PromiseSlice struct {
	Promise
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
