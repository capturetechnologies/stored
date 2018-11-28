package stored

// PromiseErr is implements everything promise implements but more
type PromiseErr struct {
	Promise
}

// Err values inside promise
func (p *PromiseErr) Err() error {
	_, err := p.transact()
	if err != nil {
		return err
	}
	return nil
}
