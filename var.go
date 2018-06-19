package stored

import "errors"

type Var struct {
	data interface{}
	err  error
}

func (v *Var) Int64() (int64, error) {
	var res int64
	if v.err != nil {
		return res, v.err
	}
	res, ok := v.data.(int64)
	if !ok {
		return res, errors.New("value is not int64")
	}
	return res, nil
}
