package stored

import (
	"bytes"
	"encoding/binary"
	"errors"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
	"github.com/apple/foundationdb/bindings/go/src/fdb/subspace"
)

// Connection is the main  struct for handling work with fdb
type Connection struct {
	db fdb.Database
}

// ErrNotFound is an error returned when no rows was found
var ErrNotFound = errors.New("Document not found")
var ErrDataCorrupt = errors.New("Data corrupt")

// Connect is main constructor for creating connections
func Connect(cluster string) *Connection {
	fdb.MustAPIVersion(510)
	conn := Connection{
		db: fdb.MustOpen(cluster, []byte("DB")),
	}
	return &conn
}

// Directory created an directury that could be used to work with stored
func (c *Connection) Directory(name string) *Directory {
	subspace, err := directory.CreateOrOpen(c.db, []string{name}, nil)
	if err != nil {
		panic(err)
	}
	return &Directory{
		Name:       name,
		Connection: c,
		Subspace:   subspace,
	}
}

// NeedRange return promise for objects data by primary key
func NeedRange(tr fdb.ReadTransaction, key subspace.Subspace) fdb.RangeResult {
	start, end := key.FDBRangeKeys()
	r := fdb.KeyRange{Begin: start, End: end}
	return tr.GetRange(r, fdb.RangeOptions{Mode: fdb.StreamingModeWantAll})
}

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
func Nan() []byte {
	return []byte{}
}
