package stored

import "github.com/apple/foundationdb/bindings/go/src/fdb/tuple"

type SelectOptions struct {
	Primary tuple.Tuple
	From    tuple.Tuple
	To      tuple.Tuple
	Limit   int
	Reverse bool
}
