// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package schema

import (
	"fmt"
	"io"
	"math"
	"reflect"
	"time"

	"blockwatch.cc/knoxdb/encoding/decimal"
	"blockwatch.cc/knoxdb/vec"
)

type Field struct {
	// schema values for CREATE TABLE
	name     string           // field name from struct tag or variable name
	id       uint16           // unique lifetime id of the field
	typ      FieldType        // schema field type from struct tag or Go type
	flags    FieldFlags       // schema flags from struct tag
	compress FieldCompression // data compression from struct tag
	index    IndexKind        // index type: none, hash, int, bloom
	fixed    uint16           // 0..65535 fixed size array/bytes/string length
	scale    uint8            // 0..255 fixed point scale, bloom error probability 1/x (1..4)

	// encoder values for INSERT, UPDATE, QUERY
	isArray  bool       // field is a fixed size array
	path     []int      // reflect struct nested positions
	offset   uintptr    // struct field offset from reflect
	dataSize uint16     // struct field size in bytes
	wireSize uint16     // wire encoding field size in bytes, min size for []byte & string
	iface    IfaceFlags // Go encoder default interfaces
}

// ExportedField is a performance improved version of Field
// containing exported fields for direct access in other packages
type ExportedField struct {
	Name      string
	Id        uint16
	Type      FieldType
	Flags     FieldFlags
	Compress  FieldCompression
	Index     IndexKind
	Fixed     uint16
	Scale     uint8
	Offset    uintptr
	Iface     IfaceFlags
	IsVisible bool
	IsArray   bool
	path      []int
}

func NewField(typ FieldType) Field {
	return Field{
		typ:      typ,
		dataSize: uint16(typ.Size()),
		wireSize: uint16(typ.Size()),
	}
}

func (f *Field) Name() string {
	return f.name
}

func (f *Field) Id() uint16 {
	return f.id
}

func (f *Field) Type() FieldType {
	return f.typ
}

func (f *Field) Path() []int {
	return f.path
}

func (f *Field) Is(v FieldFlags) bool {
	return f.flags.Is(v)
}

func (f *Field) Can(v IfaceFlags) bool {
	return f.iface.Is(v)
}

func (f *Field) Compress() FieldCompression {
	return f.compress
}

func (f *Field) Index() IndexKind {
	return f.index
}

func (f *Field) Offset() uintptr {
	return f.offset
}

func (f *Field) Scale() uint8 {
	return f.scale
}

func (f *Field) Fixed() uint16 {
	return f.fixed
}

func (f *Field) IsValid() bool {
	return len(f.name) > 0 && f.typ.IsValid()
}

func (f *Field) IsVisible() bool {
	return f.flags&(FieldFlagDeleted|FieldFlagInternal) == 0
}

func (f *Field) IsFixedSize() bool {
	switch f.typ {
	case FieldTypeString, FieldTypeBytes:
		return f.fixed > 0
	default:
		return true
	}
}

func (f *Field) IsInterface() bool {
	return f.iface != 0
}

func (f *Field) IsArray() bool {
	return f.isArray
}

func (f *Field) WireSize() int {
	switch f.typ {
	case FieldTypeString, FieldTypeBytes:
		if f.fixed > 0 {
			return int(f.fixed)
		}
	}
	return f.typ.Size()
}

// WithXXX methods do not use pointer receivers and always return a
// changed copy of the field.
func (f Field) WithName(n string) Field {
	f.name = n
	return f
}

func (f Field) WithFlags(v FieldFlags) Field {
	f.flags = v
	return f
}

func (f Field) WithCompression(c FieldCompression) Field {
	f.compress = c
	return f
}

func (f Field) WithFixed(n int) Field {
	f.fixed = uint16(n)
	return f
}

func (f Field) WithScale(n int) Field {
	f.scale = uint8(n)
	return f
}

func (f Field) WithIndex(kind IndexKind) Field {
	f.index = kind
	if kind != IndexKindNone {
		f.flags |= FieldFlagIndexed
	} else {
		f.flags &^= FieldFlagIndexed
	}
	return f
}

func (f Field) WithGoType(typ reflect.Type, path []int, ofs uintptr) Field {
	var iface IfaceFlags
	// detect marshaler types
	if typ.Implements(binaryMarshalerType) {
		iface |= IfaceBinaryMarshaler
	}
	if reflect.PointerTo(typ).Implements(binaryUnmarshalerType) {
		iface |= IfaceBinaryUnmarshaler
	}
	if typ.Implements(textMarshalerType) {
		iface |= IfaceTextMarshaler
	}
	if reflect.PointerTo(typ).Implements(textUnmarshalerType) {
		iface |= IfaceTextUnmarshaler
	}
	if typ.Implements(stringerType) {
		iface |= IfaceStringer
	}
	f.dataSize = uint16(typ.Size())
	f.wireSize = uint16(typ.Size())
	if typ.Kind() == reflect.Array && typ.Elem().Kind() == reflect.Uint8 {
		f.isArray = true
		f.dataSize = uint16(typ.Len())
		f.wireSize = uint16(typ.Len())
	}
	f.path = path
	f.offset = ofs
	f.iface = iface
	return f
}

func (f *Field) Validate() error {
	// require scale on decimal fields only
	if f.scale != 0 {
		minScale, maxScale := 0, 0
		switch f.typ {
		case FieldTypeDecimal32:
			maxScale = decimal.MaxDecimal32Precision
		case FieldTypeDecimal64:
			maxScale = decimal.MaxDecimal64Precision
		case FieldTypeDecimal128:
			maxScale = decimal.MaxDecimal128Precision
		case FieldTypeDecimal256:
			maxScale = decimal.MaxDecimal256Precision
		default:
			if f.index == IndexKindBloom {
				minScale, maxScale = 1, 4
			} else {
				return fmt.Errorf("scale unsupported on type %s", f.typ)
			}
		}
		if _, err := validateInt("scale", int(f.scale), minScale, maxScale); err != nil {
			return err
		}
	}

	// require fixed on string/byte fields only
	if f.fixed != 0 {
		if _, err := validateInt("fixed", int(f.fixed), 1, int(MAX_FIXED)); err != nil {
			return err
		}
		switch f.typ {
		case FieldTypeBytes, FieldTypeString:
			// ok
		default:
			return fmt.Errorf("fixed unsupported on type %s", f.typ)
		}
	}

	// require index kind in range
	if f.index < 0 || f.index > IndexKindBloom {
		return fmt.Errorf("invalid index kind %d", f.index)
	}

	// require index flag when index is != none
	if f.index > 0 && f.flags&FieldFlagIndexed == 0 {
		return fmt.Errorf("missing indexed flag with index kind set")
	}

	// require integer index on int fields only
	if f.index == IndexKindInt {
		switch f.typ {
		case FieldTypeInt64, FieldTypeInt32, FieldTypeInt16, FieldTypeInt8,
			FieldTypeUint64, FieldTypeUint32, FieldTypeUint16, FieldTypeUint8:
		default:
			return fmt.Errorf("unsupported integer index on type %s", f.typ)
		}
	}

	// require bloom scale 1..4
	if f.index == IndexKindBloom {
		if _, err := validateInt("bloom factor", int(f.scale), 1, 4); err != nil {
			return err
		}
	}

	return nil
}

func (f *Field) Codec() OpCode {
	switch f.typ {
	case FieldTypeDatetime:
		return OpCodeDateTime

	case FieldTypeInt64:
		return OpCodeInt64

	case FieldTypeInt32:
		return OpCodeInt32

	case FieldTypeInt16:
		return OpCodeInt16

	case FieldTypeInt8:
		return OpCodeInt8

	case FieldTypeUint64:
		return OpCodeUint64

	case FieldTypeUint32:
		return OpCodeUint32

	case FieldTypeUint16:
		return OpCodeUint16

	case FieldTypeUint8:
		return OpCodeUint8

	case FieldTypeFloat64:
		return OpCodeFloat64

	case FieldTypeFloat32:
		return OpCodeFloat32

	case FieldTypeBoolean:
		return OpCodeBool

	case FieldTypeString:
		switch {
		case f.fixed > 0:
			return OpCodeFixedString
		case f.Can(IfaceTextMarshaler):
			return OpCodeMarshalText
		case f.Can(IfaceStringer):
			return OpCodeStringer
		default:
			return OpCodeString
		}

	case FieldTypeBytes:
		switch {
		case f.Can(IfaceBinaryMarshaler):
			return OpCodeMarshalBinary
		case f.isArray:
			return OpCodeFixedArray
		case f.fixed > 0:
			return OpCodeFixedBytes
		default:
			return OpCodeBytes
		}

	case FieldTypeInt256:
		return OpCodeInt256

	case FieldTypeInt128:
		return OpCodeInt128

	case FieldTypeDecimal256:
		return OpCodeDecimal256

	case FieldTypeDecimal128:
		return OpCodeDecimal128

	case FieldTypeDecimal64:
		return OpCodeDecimal64

	case FieldTypeDecimal32:
		return OpCodeDecimal32

	default:
		return OpCodeInvalid
	}
}

// Simple per field encoder used to wire-encode individual typed values
// found in query conditions.
func (f *Field) Encode(w io.Writer, val any) (err error) {
	if val == nil {
		return ErrNilValue
	}

	// init error, will be overwritten by write branches below
	err = ErrInvalidValueType

	switch code := f.Codec(); code {
	default:
		err = encodeInt(w, code, val)

	case OpCodeFixedArray,
		OpCodeFixedString,
		OpCodeFixedBytes,
		OpCodeString,
		OpCodeStringer,
		OpCodeBytes,
		OpCodeMarshalBinary,
		OpCodeMarshalText:

		err = encodeBytes(w, val, f.fixed)

	case OpCodeBool:
		b, ok := val.(bool)
		if ok {
			if b {
				_, err = w.Write([]byte{1})
			} else {
				_, err = w.Write([]byte{0})
			}
		}

	case OpCodeDateTime:
		tv, ok := val.(time.Time)
		if ok {
			_, err = w.Write(Uint64Bytes(uint64(tv.UnixNano())))
		}

	case OpCodeFloat32:
		switch v := val.(type) {
		case float32:
			_, err = w.Write(Uint32Bytes(math.Float32bits(v)))
		case float64:
			_, err = w.Write(Uint32Bytes(math.Float32bits(float32(v))))
		}

	case OpCodeFloat64:
		switch v := val.(type) {
		case float32:
			_, err = w.Write(Uint64Bytes(math.Float64bits(float64(v))))
		case float64:
			_, err = w.Write(Uint64Bytes(math.Float64bits(v)))
		}

	case OpCodeInt128:
		v, ok := val.(vec.Int128)
		if ok {
			_, err = w.Write(v.Bytes())
		}

	case OpCodeInt256:
		v, ok := val.(vec.Int256)
		if ok {
			_, err = w.Write(v.Bytes())
		}

	case OpCodeDecimal32:
		v, ok := val.(decimal.Decimal32)
		if ok {
			_, err = w.Write(Uint32Bytes(uint32(v.Int32())))
		}

	case OpCodeDecimal64:
		v, ok := val.(decimal.Decimal64)
		if ok {
			_, err = w.Write(Uint64Bytes(uint64(v.Int64())))
		}

	case OpCodeDecimal128:
		v, ok := val.(decimal.Decimal128)
		if ok {
			_, err = w.Write(v.Int128().Bytes())
		}

	case OpCodeDecimal256:
		v, ok := val.(decimal.Decimal256)
		if ok {
			_, err = w.Write(v.Int256().Bytes())
		}
	}
	return
}

// Simple per field decoder used to wire-decode individual typed values
// found in query conditions.
func (f *Field) Decode(r io.Reader) (val any, err error) {
	var (
		buf [32]byte
		n   int
	)
	switch f.typ {
	case FieldTypeDatetime:
		_, err = r.Read(buf[:8])
		i64, _ := ReadInt64(buf[:8])
		val = time.Unix(0, i64).UTC()

	case FieldTypeInt64:
		_, err = r.Read(buf[:8])
		i64, _ := ReadInt64(buf[:8])
		val = i64

	case FieldTypeInt32:
		_, err = r.Read(buf[:4])
		i32, _ := ReadInt32(buf[:4])
		val = i32

	case FieldTypeInt16:
		_, err = r.Read(buf[:2])
		i16, _ := ReadInt32(buf[:2])
		val = i16

	case FieldTypeInt8:
		_, err = r.Read(buf[:1])
		i8, _ := ReadInt8(buf[:1])
		val = i8

	case FieldTypeUint64:
		_, err = r.Read(buf[:8])
		u64, _ := ReadUint64(buf[:8])
		val = u64

	case FieldTypeUint32:
		_, err = r.Read(buf[:4])
		u32, _ := ReadUint32(buf[:4])
		val = u32

	case FieldTypeUint16:
		_, err = r.Read(buf[:2])
		i16, _ := ReadInt32(buf[:2])
		val = i16

	case FieldTypeUint8:
		_, err = r.Read(buf[:1])
		i8, _ := ReadInt8(buf[:1])
		val = i8

	case FieldTypeFloat64:
		_, err = r.Read(buf[:8])
		u64, _ := ReadUint64(buf[:8])
		val = math.Float64frombits(u64)

	case FieldTypeFloat32:
		_, err = r.Read(buf[:4])
		u32, _ := ReadUint32(buf[:4])
		val = math.Float32frombits(u32)

	case FieldTypeBoolean:
		_, err = r.Read(buf[:1])
		b := buf[0] > 0
		val = b

	case FieldTypeString:
		if f.fixed > 0 {
			b := make([]byte, f.fixed)
			n, err = r.Read(b)
			if n < int(f.fixed) {
				return nil, ErrShortBuffer
			}
			val = string(b[:n])
		} else {
			_, err = r.Read(buf[:4])
			if err != nil {
				return
			}
			u32, _ := ReadUint32(buf[:4])
			b := make([]byte, int(u32))
			n, err = r.Read(b)
			val = string(b[:n])
		}

	case FieldTypeBytes:
		if f.fixed > 0 {
			b := make([]byte, f.fixed)
			n, err = r.Read(b)
			if n < int(f.fixed) {
				return nil, ErrShortBuffer
			}
			val = string(b[:n])
		} else {
			_, err = r.Read(buf[:4])
			if err != nil {
				return
			}
			u32, _ := ReadUint32(buf[:4])
			b := make([]byte, int(u32))
			n, err = r.Read(b)
			val = b[:n]
		}

	case FieldTypeInt256:
		_, err = r.Read(buf[:32])
		i256 := vec.Int256FromBytes(buf[:32])
		val = i256

	case FieldTypeInt128:
		_, err = r.Read(buf[:16])
		i128 := vec.Int128FromBytes(buf[:16])
		val = i128

	case FieldTypeDecimal256:
		_, err = r.Read(buf[:32])
		d256 := decimal.NewDecimal256(vec.Int256FromBytes(buf[:32]), int(f.scale))
		val = d256

	case FieldTypeDecimal128:
		_, err = r.Read(buf[:16])
		d128 := decimal.NewDecimal128(vec.Int128FromBytes(buf[:16]), int(f.scale))
		val = d128

	case FieldTypeDecimal64:
		_, err = r.Read(buf[:8])
		i64, _ := ReadInt64(buf[:8])
		d64 := decimal.NewDecimal64(i64, int(f.scale))
		val = d64

	case FieldTypeDecimal32:
		_, err = r.Read(buf[:4])
		i32, _ := ReadInt32(buf[:4])
		d32 := decimal.NewDecimal32(i32, int(f.scale))
		val = d32

	default:
		err = ErrInvalidField
	}
	return
}

// StructValue resolves a struct field from a struct. When the field
// is a pointer it allocates the target type and dereferences it
// so that the return value can consistently be used for interface calls.
func (f *Field) StructValue(rval reflect.Value) reflect.Value {
	dst := rval.FieldByIndex(f.path)
	if dst.Kind() == reflect.Ptr {
		if dst.IsNil() && dst.CanSet() {
			dst.Set(reflect.New(dst.Type().Elem()))
		}
		dst = dst.Elem()
	}
	return dst
}

func (f *ExportedField) StructValue(rval reflect.Value) reflect.Value {
	dst := rval.FieldByIndex(f.path)
	if dst.Kind() == reflect.Ptr {
		if dst.IsNil() && dst.CanSet() {
			dst.Set(reflect.New(dst.Type().Elem()))
		}
		dst = dst.Elem()
	}
	return dst
}
