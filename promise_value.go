package stored

// PromiseValue is implements everything promise implements but also values
type PromiseValue struct {
	Promise
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
