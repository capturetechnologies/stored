package stored

import (
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/subspace"
)

type needObject struct {
	object      *Object
	rangeResult fdb.RangeResult
	subspace    subspace.Subspace
}

func (n *needObject) need(tr fdb.ReadTransaction, sub subspace.Subspace) {

	//fmt.Println("need sub", sub)
	//start, end := sub.FDBRangeKeys()
	//r := fdb.KeyRange{Begin: start, End: end}

	start := sub.FDBKey()
	end := append(start, uint8(255))
	r := fdb.KeyRange{Begin: start, End: end}
	//fmt.Println("fetching range", start, end)
	n.rangeResult = tr.GetRange(r, fdb.RangeOptions{Mode: fdb.StreamingModeWantAll})
}

func (n *needObject) fetch() (*Value, error) {
	rows, err := n.rangeResult.GetSliceWithError()
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		// problem is here so shouldbe checked
		return nil, ErrNotFound
	}
	value := Value{object: n.object}
	value.FromKeyValue(n.subspace, rows)
	return &value, nil
}
