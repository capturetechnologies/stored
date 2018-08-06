package stored

import (
	"bytes"
	"encoding/binary"
	"errors"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
)

// ErrNotFound is an error returned when no rows was found
var ErrNotFound = errors.New("Document not found")
var ErrDataCorrupt = errors.New("Data corrupt")
var ErrAlreadyExist = errors.New("This object already exist")

// needObject return promise for objects data by primary key
/*func needObject(tr fdb.ReadTransaction, sub subspace.Subspace) fdb.RangeResult {
	start, end := sub.FDBRangeKeys()
	r := fdb.KeyRange{Begin: start, End: end}
	return tr.GetRange(r, fdb.RangeOptions{Mode: fdb.StreamingModeWantAll})
}*/

// fetchObject returns the kv results of an object
/*func fetchObject(tr fdb.ReadTransaction, needed fdb.RangeResult) ([]fdb.KeyValue, error) {
	res, err := v.GetSliceWithError()
	if err != nil {
		return nil, err
	}
	if len(res) == 0 {
		return nil, ErrNotFound
	}
	return res
}*/

func FetchRange(tr fdb.ReadTransaction, needed []fdb.RangeResult) ([][]fdb.KeyValue, error) {
	results := make([][]fdb.KeyValue, len(needed))
	for k, v := range needed {
		res, err := v.GetSliceWithError()
		if err != nil {
			return nil, err
		}
		results[k] = res
	}
	return results, nil
}

// Int convert int to byte array
func Int(i int) []byte {
	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.LittleEndian, int32(i))
	return buffer.Bytes()
}

// Int32 convert int32 to byte array
func Int32(i int32) []byte {
	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.LittleEndian, int32(i))
	return buffer.Bytes()
}

// Int64 convert int64 to byte array
func Int64(i int64) []byte {
	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.LittleEndian, int64(i))
	return buffer.Bytes()
}

// ToInt64 converts byte array to int64
func ToInt64(b []byte) int64 {
	return int64(binary.LittleEndian.Uint64(b))
}

// ToInt32 converts byte array to int64
func ToInt32(b []byte) int32 {
	return int32(binary.LittleEndian.Uint32(b))
}

// ToInt converts byte array to int64
func ToInt(b []byte) int {
	return int(binary.LittleEndian.Uint32(b))
}

// Nan means no data presented
var Nan = []byte{}
