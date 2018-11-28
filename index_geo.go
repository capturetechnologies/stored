package stored

import (
	"fmt"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/mmcloughlin/geohash"
)

// IndexGeo does all the Index does but also geo
type IndexGeo struct {
	index *Index
}

// GetGeo will return elements by geo index
func (ig *IndexGeo) GetGeo(lat float64, long float64, limit int) *PromiseSlice {
	i := ig.index
	// Precisions
	// #   km
	// 1   ± 2500
	// 2   ± 630
	// 3   ± 78
	// 4   ± 20
	// 5   ± 2.4
	// 6   ± 0.61
	// 7   ± 0.076
	// 8   ± 0.019
	// 9   ± 0.0024
	// 10  ± 0.00060
	// 11  ± 0.000074
	hash := geohash.Encode(lat, long)
	if i.Geo > 0 && i.Geo < len(hash) {
		hash = hash[0:i.Geo] // cut hash len to match percision of index
	}
	neighbors := geohash.Neighbors(hash)
	search := append(neighbors, hash)
	fmt.Println("search scope hashes:", search)
	p := i.object.promiseSlice()
	p.doRead(func() Chain {
		rangeResults := map[string]fdb.RangeResult{}
		for _, geohash := range search {
			sub := i.dir.Sub(geohash)
			start, end := sub.FDBRangeKeys()
			r := fdb.KeyRange{Begin: start, End: end}
			rangeResults[geohash] = p.readTr.GetRange(r, fdb.RangeOptions{
				Limit:   limit,
				Reverse: true,
			})
		}
		need := []*needObject{}
		slice := Slice{}
		return func() Chain {
			for geohash, res := range rangeResults {
				sub := i.dir.Sub(geohash)
				rows, err := res.GetSliceWithError()
				if err != nil {
					return p.fail(err)
				}
				if len(rows) == 0 {
					continue
				}
				fmt.Println("[A] hash got:", sub.Bytes(), "len:", len(rows))
				for _, row := range rows {
					primaryTuple, err := sub.Unpack(row.Key)
					if err != nil {
						return p.fail(err)
					}
					fmt.Println("[A] primary:", primaryTuple)
					// need object here
					need = append(need, i.object.need(p.readTr, i.object.sub(primaryTuple)))
				}
			}
			return func() Chain {
				fmt.Println("len need", len(need))
				for _, n := range need {
					val, err := n.fetch()
					if err != nil {
						fmt.Println("val failed", err)
						continue
						//return p.fail(err)
					}
					slice.Append(val)
				}
				return p.done(&slice) // return frinish slice here
			}
		}
	})
	return p
}
