package stored

// PromiseErr is implements everything promise implements but more
type PromiseErr struct {
	Promise
}

// Do will attach promise to transaction, so promise will be called within passed transaction
// Promise should be inside an transaction callback, because transaction could be resent
func (p *PromiseErr) Do(t *Transaction) *PromiseErr {
	if !t.started {
		panic("transaction not started, could not use in Promise")
	}
	p.tr = t.tr
	p.readTr = t.readTr
	return p
}

// Err values inside promise
func (p *PromiseErr) Err() error {
	_, err := p.transact()
	if err != nil {
		return err
	}
	return nil
}
