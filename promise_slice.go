package stored

// PromiseSlice is implements everything promise implements but more
type PromiseSlice struct {
	Promise
}

// ScanAll values inside promise
func (p *PromiseSlice) ScanAll(slicePointer interface{}) error {
	res, err := p.transact()
	if err != nil {
		return err
	}
	slice := res.(*Slice)
	return slice.ScanAll(slicePointer)
}
