package stored

import (
	"github.com/vmihailenco/msgpack"
)

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

// UnpackKeyIndex takes original key tuple and packed key with sub index, checks the key and return only subindex
/*func UnpackKeyIndex(origin tuple.Tuple, key []byte) (tuple.TupleElement, error) {
	unpacked, err := tuple.Unpack(key)
	if err != nil {
		return nil, err
	}
	if len(unpacked)-len(origin) != 1 {
		return nil, errors.New("no sub index found")
	}
	for k, v := range origin {
		if unpacked[k] != v {
			return nil, errors.New("no sub index found")
		}
	}
	return unpacked[len(unpacked)-1], nil
}*/
