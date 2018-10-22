package packed

import (
	"bytes"
	"encoding"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"reflect"
)

var endian = binary.LittleEndian

type Packed struct {
	Kind  reflect.Kind
	Value reflect.Value
}

// New creates new packed instance, could be passed interface or reflectValue as packing schema
func New(typeInput interface{}) *Packed {
	value, ok := typeInput.(reflect.Value)
	if !ok {
		value = reflect.Indirect(reflect.ValueOf(typeInput))
	}
	kind := value.Kind()
	return &Packed{
		Kind:  kind,
		Value: value,
	}
}

type byteReader struct {
	io.Reader
}

func (b *byteReader) ReadByte() (byte, error) {
	var buf [1]byte
	if _, err := io.ReadFull(b, buf[:]); err != nil {
		return 0, err
	}
	return buf[0], nil
}

func vInt(writer io.Writer, v int) error {
	buf8 := make([]byte, 8)
	l := binary.PutUvarint(buf8, uint64(v))
	_, err := writer.Write(buf8[:l])
	return err
}

// Encode will pack passed data to byte array
func (p *Packed) Encode(data interface{}) ([]byte, error) {
	writer := &bytes.Buffer{}
	err := p.doEncode(writer, data, 0)
	if err != nil {
		return nil, err
	}
	return writer.Bytes(), nil
}

func (p *Packed) doEncode(writer io.Writer, data interface{}, level int) (err error) {
	level++
	switch ref := data.(type) {
	case encoding.BinaryMarshaler:
		buf, err := ref.MarshalBinary()
		if err != nil {
			return err
		}
		if err = vInt(writer, len(buf)); err != nil {
			return err
		}
		_, err = writer.Write(buf)
	case []byte:
		if err = vInt(writer, len(ref)); err != nil {
			return
		}
		_, err = writer.Write(ref)
	default:
		value := reflect.ValueOf(data)

		t := value.Type()
		kind := t.Kind()
		//fmt.Println("VALUE TYE", t, "KIND", kind)
		if kind == reflect.Ptr {
			if value.IsNil() {
				if level == 1 {
					return
				} else {
					panic("nil inside struct pack")
				}
			}
			value = reflect.Indirect(value)
			t = value.Type()
			kind = t.Kind()
			//fmt.Println("[ fixed ]  VALUE TYE", t, "KIND", kind)
		}

		switch kind {
		case reflect.Array:
			length := t.Len()
			for i := 0; i < length; i++ {
				if err = p.doEncode(writer, value.Index(i).Addr().Interface(), level); err != nil {
					return
				}
			}

		case reflect.Slice:
			l := value.Len()
			if err = vInt(writer, l); err != nil {
				return
			}
			for i := 0; i < l; i++ {
				if err = p.doEncode(writer, value.Index(i).Addr().Interface(), level); err != nil {
					return
				}
			}

		case reflect.Struct:
			l := value.NumField()
			for i := 0; i < l; i++ {
				if v := value.Field(i); t.Field(i).Name != "_" {
					if err = p.doEncode(writer, v.Interface(), level); err != nil {
						return
					}
				}
			}

		case reflect.Map:
			l := value.Len()
			if err = vInt(writer, l); err != nil {
				return
			}
			for _, key := range value.MapKeys() {
				value := value.MapIndex(key)
				if err = p.doEncode(writer, key.Interface(), level); err != nil {
					return err
				}
				if err = p.doEncode(writer, value.Interface(), level); err != nil {
					return err
				}
			}

		case reflect.String:
			if err = vInt(writer, value.Len()); err != nil {
				return
			}
			_, err = writer.Write([]byte(value.String()))

		case reflect.Bool:
			var out byte
			if value.Bool() {
				out = 1
			}
			err = binary.Write(writer, endian, out)

		case reflect.Int:
			err = binary.Write(writer, endian, int64(value.Int()))

		case reflect.Uint:
			err = binary.Write(writer, endian, int64(value.Uint()))

		case reflect.Int8, reflect.Uint8, reflect.Int16, reflect.Uint16,
			reflect.Int32, reflect.Uint32, reflect.Int64, reflect.Uint64,
			reflect.Float32, reflect.Float64,
			reflect.Complex64, reflect.Complex128:
			err = binary.Write(writer, endian, data)

		default:
			return errors.New("unsupported type " + t.String())
		}
	}
	return
}

func (p *Packed) doDecode(reader *byteReader, value reflect.Value) (err error) {
	/*if i, ok := input.(encoding.BinaryUnmarshaler); ok {
		var l uint64
		if l, err = binary.ReadUvarint(reader); err != nil {
			return
		}
		buf := make([]byte, l)
		_, err = reader.Read(buf)
		return i.UnmarshalBinary(buf)
	}*/

	if !value.CanAddr() {
		fmt.Println("this pointer could not decoded")
		return errors.New("this pointer could not decoded")
	}
	t := value.Type()

	switch t.Kind() {
	case reflect.Array:
		len := t.Len()
		for i := 0; i < int(len); i++ {
			if err = p.doDecode(reader, value.Index(i)); err != nil {
				return
			}
		}

	case reflect.Slice:
		var l uint64
		if l, err = binary.ReadUvarint(reader); err != nil {
			return
		}
		if t.Kind() == reflect.Slice {
			value.Set(reflect.MakeSlice(t, int(l), int(l)))
		} else if int(l) != t.Len() {
			return fmt.Errorf("slice size is incorrect, encoded = %d, expected = %d", l, t.Len())
		}
		for i := 0; i < int(l); i++ {
			if err = p.doDecode(reader, value.Index(i)); err != nil {
				return
			}
		}

	case reflect.Struct:
		l := value.NumField()
		for i := 0; i < l; i++ {
			if v := value.Field(i); v.CanSet() && t.Field(i).Name != "_" {
				if err = p.doDecode(reader, v); err != nil {
					return
				}
			}
		}

	case reflect.Map:
		var l uint64
		if l, err = binary.ReadUvarint(reader); err != nil {
			return
		}
		kt := t.Key()
		vt := t.Elem()
		value.Set(reflect.MakeMap(t))
		for i := 0; i < int(l); i++ {
			kv := reflect.Indirect(reflect.New(kt))
			if err = p.doDecode(reader, kv); err != nil {
				return
			}
			vv := reflect.Indirect(reflect.New(vt))
			if err = p.doDecode(reader, vv); err != nil {
				return
			}
			value.SetMapIndex(kv, vv)
		}

	case reflect.String:
		var l uint64
		if l, err = binary.ReadUvarint(reader); err != nil {
			return
		}
		buf := make([]byte, l)
		_, err = reader.Read(buf)
		value.SetString(string(buf))

	case reflect.Bool:
		var out byte
		err = binary.Read(reader, endian, &out)
		value.SetBool(out != 0)

	case reflect.Int:
		var out int64
		err = binary.Read(reader, endian, &out)
		value.SetInt(out)

	case reflect.Uint:
		var out uint64
		err = binary.Read(reader, endian, &out)
		value.SetUint(out)

	case reflect.Int8, reflect.Uint8, reflect.Int16, reflect.Uint16,
		reflect.Int32, reflect.Uint32, reflect.Int64, reflect.Uint64,
		reflect.Float32, reflect.Float64, reflect.Complex64, reflect.Complex128:
		err = binary.Read(reader, endian, value.Addr().Interface())

	default:
		return errors.New("unsupported type " + t.String())
	}
	return
}

func (p *Packed) DecodeToValue(data []byte, value reflect.Value) error {
	//input := p.Value.Interface()
	reader := &byteReader{bytes.NewReader(data)}
	value = reflect.Indirect(value)
	return p.doDecode(reader, value)
}

// DecodeToInterface will decode field to interface
func (p *Packed) DecodeToInterface(data []byte) interface{} {
	t := p.Value.Type()
	var isPointer bool
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
		isPointer = true
	}
	value := reflect.New(t)
	value = reflect.Indirect(value)
	p.DecodeToValue(data, value)

	if isPointer {
		value = value.Addr()
	}
	return value.Interface()
}

func (p *Packed) Decode(data []byte, input interface{}) error {
	value := reflect.ValueOf(input)
	return p.DecodeToValue(data, value)
}

// Plus returns 1 in byte representation of packed type
func (p *Packed) Plus() []byte {
	switch p.Kind {
	case reflect.Int, reflect.Int64, reflect.Uint64:
		return []byte{'\x01', '\x00', '\x00', '\x00', '\x00', '\x00', '\x00', '\x00'}
	case reflect.Int32, reflect.Uint32:
		return []byte{'\x01', '\x00', '\x00', '\x00'}
	case reflect.Int16, reflect.Uint16:
		return []byte{'\x01', '\x00'}
	case reflect.Int8, reflect.Uint8:
		return []byte{'\x01'}
	}
	panic("type not supported for plus operation")
}

// Minus returns -1 in byte representation of packed type
func (p *Packed) Minus() []byte {
	switch p.Kind {
	case reflect.Int, reflect.Int64, reflect.Uint64:
		return []byte{'\xff', '\xff', '\xff', '\xff', '\xff', '\xff', '\xff', '\xff'}
	case reflect.Int32, reflect.Uint32:
		return []byte{'\xff', '\xff', '\xff', '\xff'}
	case reflect.Int16, reflect.Uint16:
		return []byte{'\xff', '\xff'}
	case reflect.Int8, reflect.Uint8:
		return []byte{'\xff'}
	}
	panic("type not supported for minus operation")
}
