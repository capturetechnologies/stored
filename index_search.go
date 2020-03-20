package stored

import (
	"errors"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
)

// IndexSearch provide  substring search index for strings. That means you can search by part of the word
// this index is fast but could leave significant memory footpring on your database
type IndexSearch struct {
	index *Index
}

// Search is main function to search using search index
func (is *IndexSearch) Search(name string) *PromiseSlice {
	words := searchSplit(name)

	//word := words[0]
	i := is.index
	p := i.object.promiseSlice()
	wordsLen := len(words)
	p.doRead(func() Chain {
		if wordsLen < 1 {
			return p.fail(errors.New("No words found on the string"))
		}

		limit := 1000
		if wordsLen == 1 {
			limit = p.limit
		}
		rangeResults := map[string]fdb.RangeResult{}
		for _, word := range words {
			partword := i.dir.Pack(tuple.Tuple{word})
			start := partword[:len(partword)-1]
			end := make(fdb.Key, len(start))
			copy(end, start)

			start = append(start, 0)
			end = append(end, 255)

			r := fdb.KeyRange{Begin: start, End: end}
			rangeResults[word] = p.readTr.GetRange(r, fdb.RangeOptions{
				Limit:   limit,
				Reverse: p.reverse,
			})
		}
		need := []*needObject{}
		slice := Slice{}
		return func() Chain {
			found := map[string]int{}
			for _, word := range words {
				rows, err := rangeResults[word].GetSliceWithError()
				if err != nil {
					return p.fail(err)
				}
				for _, row := range rows {
					fullTuple, err := i.dir.Unpack(row.Key)
					if err != nil {
						return p.fail(err)
					}
					if len(fullTuple) < 2 {
						continue
					}
					primaryTuple := fullTuple[1:]
					primaryTupleKey := string(primaryTuple.Pack())
					_, ok := found[primaryTupleKey]
					if ok {
						found[primaryTupleKey]++
					} else {
						found[primaryTupleKey] = 1
					}
					if found[primaryTupleKey] == wordsLen {
						need = append(need, i.object.need(p.readTr, i.object.sub(primaryTuple)))
					}
				}
			}
			return func() Chain {
				for _, n := range need {
					val, err := n.fetch()
					if err != nil {
						continue
					}
					slice.Append(val)
				}
				return p.done(&slice) // return frinish slice here
			}
		}
	})
	return p
}

// ClearAll will remove index data
func (is *IndexSearch) ClearAll() error {
	return is.index.ClearAll()
}

// Reindex will reindex index data
func (is *IndexSearch) Reindex() {
	is.index.Reindex()
}
