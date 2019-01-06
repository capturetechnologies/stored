package stored

import (
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
)

// STORED support up to 255 versions of each object
// when you change object (for eaxmple change type of an field) new version is created
// once there will be 100 versions ahead STORED will perform transactional database update
// updating all outdated records to the newest version

// note: any large field (> than 100kb should be marked as mutable for the performance)

type schemaFull struct { // stored separately, an key for each version
	versions map[uint8]schemaVersion
	latest   uint8
	current  schemaVersion
}

type schemaVersion struct {
	PrimaryFields []schemaField `json:"primary"`  // Fields stored inside primary part of key
	PackedFields  []schemaField `json:"packed"`   // Fields stored at one-key packed body
	MutableFields []schemaField `json:"mutable"`  // Fields stored as separate keys (mutable keys)
	Created       int64         `json:"timestamp` // Time the schema was created
}

// schemaField is needed to match data in diferent position of the value with fields of object
// since annotated name of field or real name could be changes STORED will match field if one
// of those preserved
type schemaField struct {
	Name       string `json:"name"`
	ObjectName string `json:"obj_name"`
	Type       string `json:"type"`
}

func (sf *schemaFull) load(ob *ObjectBuilder, dir directory.DirectorySubspace, tr fdb.ReadTransaction) error {
	sub := dir.Sub("schema")

	start, end := sub.FDBRangeKeys()
	r := fdb.KeyRange{Begin: start, End: end}

	rangeGet := tr.GetRange(r, fdb.RangeOptions{
		Mode: fdb.StreamingModeWantAll,
	})
	rows, err := rangeGet.GetSliceWithError()
	if err != nil {
		return err
	}
	sf.versions = map[uint8]schemaVersion{}
	for _, kv := range rows {
		sch := schemaVersion{}
		err := json.Unmarshal(kv.Value, sch)
		if err != nil {
			fmt.Println("schema corrupted", err)
			ob.panic("schema corrupted")
		}
		var tuple tuple.Tuple
		tuple, err = sub.Unpack(kv.Key)
		if err != nil {
			fmt.Println("schema corrupted", err)
			ob.panic("schema corrupted")
		}
		version := tuple[0].(uint8)
		sf.versions[version] = sch
	}
	sf.setLatest()
	return nil
}

func (sf *schemaFull) setLatest() *schemaVersion {
	latestTime := int64(0)
	latestVer := uint8(0)
	for ver, schema := range sf.versions {
		if schema.Created > latestTime {
			latestVer = ver
			latestTime = schema.Created
		}
	}
	if latestVer != 0 {
		v := sf.versions[latestVer]
		return &v
	}
	return nil
}

func (sf *schemaFull) buildCurrent(ob *ObjectBuilder) {
	sf.current = schemaVersion{
		PrimaryFields: []schemaField{},
		PackedFields:  []schemaField{},
		MutableFields: []schemaField{},
	}
	for _, field := range ob.object.primaryFields {
		sf.current.PrimaryFields = append(sf.current.PrimaryFields, sf.current.wrapField(field))
	}
	for _, field := range ob.object.fields {
		if field.primary {
			continue
		}
		if field.mutable {
			sf.current.MutableFields = append(sf.current.MutableFields, sf.current.wrapField(field))
		} else {
			sf.current.PackedFields = append(sf.current.PackedFields, sf.current.wrapField(field))
		}
	}
}

// compare returns true if new version should be stored
func (sf *schemaFull) compare(new *schemaVersion, old *schemaVersion) bool {
	if new.Created < old.Created {
		return false // just in case comparing outdated version
	}
	if len(new.PrimaryFields) != len(old.PrimaryFields) {
		return true // new version should be set up
	}
	if !reflect.DeepEqual(new.PrimaryFields, old.PrimaryFields) {
		return true
	}
	if len(new.PackedFields) != len(old.PackedFields) {
		return true
	}
	for k, newField := range new.PackedFields {
		oldField := old.PackedFields[k]
		if newField.Name != oldField.Name && newField.ObjectName != oldField.ObjectName {
			return true
		}
		if newField.Type != oldField.Type {
			return true
		}
	}
	if len(new.MutableFields) != len(old.MutableFields) {
		return true
	}
	for k, newField := range new.MutableFields {
		oldField := old.MutableFields[k]
		if newField.Name != oldField.Name && newField.ObjectName != oldField.ObjectName {
			return true
		}
		if newField.Type != oldField.Type {
			return true
		}
	}
	return false
}

func (sv *schemaVersion) wrapField(field *Field) schemaField {
	return schemaField{
		Name:       field.Name,
		ObjectName: field.Type.Name,
		Type:       field.Type.Type.Name(),
	}
}
