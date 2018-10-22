package stored

import (
	"bytes"
	"encoding/binary"
	"errors"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
)

// ErrNotFound is an error returned when no rows was found
var ErrNotFound = errors.New("Document not found")

// ErrDataCorrupt incorrect data sent
var ErrDataCorrupt = errors.New("Data corrupt")

// ErrAlreadyExist Object with this primary index or one of unique indexes already
var ErrAlreadyExist = errors.New("This object already exist")

// GenIDType is type for ID generators
type GenIDType int

const (
	// GenIDNone is no generateID options set
	GenIDNone GenIDType = iota
	// GenIDDate is option for generating unique id using unix timestamp and random combined,
	GenIDDate
	// GenIDRandom if you do not whant unix timestamp in your ids
	GenIDRandom
)

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
	binary.Write(buffer, binary.LittleEndian, i)
	return buffer.Bytes()
}

// Complex128 convert complex128 to byte array
func Complex128(i complex128) []byte {
	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.LittleEndian, i)
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

func IncrementTuple(t tuple.Tuple) tuple.Tuple {
	index := len(t) - 1
	if index < 0 {
		return t
	}
	switch k := t[index].(type) {
	case int:
		t[index] = k + 1
	case int16:
		t[index] = k + 1
	case int8:
		t[index] = k + 1
	case int32:
		t[index] = k + 1
	case int64:
		t[index] = k + 1
	case uint:
		t[index] = k + 1
	case uint8:
		t[index] = k + 1
	case uint16:
		t[index] = k + 1
	case uint32:
		t[index] = k + 1
	case uint64:
		t[index] = k + 1
	case []byte:
		t[index] = append(k, '\x01')
	case string:
		t[index] = k + "\x01"
	}
	return t
}

// Nan means no data presented
var Nan = []byte{}
