package stored

// PromiseValue is implements everything promise implements but also values
type PromiseValue struct {
	Promise
}

// Do will attach promise to transaction, so promise will be called within passed transaction
// Promise should be inside an transaction callback, because transaction could be resent
func (p *PromiseValue) Do(t *Transaction) *PromiseValue {
	if !t.started {
		panic("transaction not started, could not use in Promise")
	}
	p.tr = t.tr
	p.readTr = t.readTr
	return p
}

// Scan appened passed object with fetched fields
func (p *PromiseValue) Scan(obj interface{}) error {
	res, err := p.transact()
	if err != nil {
		return err
	}
	value, ok := res.(*Value)
	if !ok {
		panic("Scan couldn't be triggered because promise has no Value")
	}
	return value.Scan(obj)
}
