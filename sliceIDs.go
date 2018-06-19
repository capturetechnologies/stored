package stored

import (
	"errors"

	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
)

type SliceIDs struct {
	ids  []tuple.Tuple
	vals [][]byte
	err  error
}

func (s *SliceIDs) init() {
	s.ids = []tuple.Tuple{}
	s.vals = [][]byte{}
}

func (s *SliceIDs) push(key tuple.Tuple, val []byte) {
	s.ids = append(s.ids, key)
	s.vals = append(s.vals, val)
}

func (s *SliceIDs) Int64() (map[int64][]byte, error) {
	if s.err != nil {
		return nil, s.err
	}
	res := map[int64][]byte{}
	for k, v := range s.ids {
		val, ok := v[0].(int64)
		if !ok {
			return nil, errors.New("Ids is not int64")
		}
		res[val] = s.vals[k]
	}
	return res, nil
}
