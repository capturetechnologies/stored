package stored

import "github.com/apple/foundationdb/bindings/go/src/fdb/tuple"

type SelectOptions struct {
	Primary tuple.Tuple
	Limit   int
}
