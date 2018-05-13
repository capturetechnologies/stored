package stored

import "github.com/vmihailenco/msgpack"

type Pack struct {
	err    error
	binary []byte
}

func (p *Pack) String() (string, error) {
	res := ""
	if p.err != nil {
		return res, p.err
	}
	err := msgpack.Unmarshal(p.binary, &res)
	if err != nil {
		return res, err
	}
	return res, nil
}

func (p *Pack) Int() (int, error) {
	var res int
	if p.err != nil {
		return res, p.err
	}
	err := msgpack.Unmarshal(p.binary, &res)
	if err != nil {
		return res, err
	}
	return res, nil
}

func (p *Pack) Int64() (int64, error) {
	var res int64
	if p.err != nil {
		return res, p.err
	}
	err := msgpack.Unmarshal(p.binary, &res)
	if err != nil {
		return res, err
	}
	return res, nil
}

func (p *Pack) Scan(obj interface{}) error {
	if p.err != nil {
		return p.err
	}
	err := msgpack.Unmarshal(p.binary, obj)
	if err != nil {
		return err
	}
	return nil
}
