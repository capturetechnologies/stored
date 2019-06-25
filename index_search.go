package stored

import (
	"errors"
	"fmt"

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

	word := words[0]
	i := is.index
	p := i.object.promiseSlice()
	p.doRead(func() Chain {
		if len(words) < 1 {
			return p.fail(errors.New("No words dound on the string"))
		}
		//sub := i.dir.Sub(word)
		partword := i.dir.Pack(tuple.Tuple{word})
		//key := fdb.Key{}
		//partword = partword[:len(partword)-1]
		start := partword[:len(partword)-1]
		end := make(fdb.Key, len(start))
		copy(end, start)

		start = append(start, 0)
		end = append(end, 255)

		fmt.Println("word", word, "start!", start, "end", end)

		r := fdb.KeyRange{Begin: start, End: end}
		rangeResult := p.readTr.GetRange(r, fdb.RangeOptions{
			Limit:   p.limit,
			Reverse: p.reverse,
		})
		need := []*needObject{}
		slice := Slice{}
		return func() Chain {
			rows, err := rangeResult.GetSliceWithError()
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

				need = append(need, i.object.need(p.readTr, i.object.sub(primaryTuple)))
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
