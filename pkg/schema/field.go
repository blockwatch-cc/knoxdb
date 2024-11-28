// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package schema

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"reflect"
	"time"

	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/num"
)

type Field struct {
	// schema values for CREATE TABLE
	name     string                 // field name from struct tag or variable name
	id       uint16                 // unique lifetime id of the field
	typ      types.FieldType        // schema field type from struct tag or Go type
	flags    types.FieldFlags       // schema flags from struct tag
	compress types.FieldCompression // data compression from struct tag
	index    types.IndexType        // index type: none, hash, int, bloom
	fixed    uint16                 // 0..65535 fixed size array/bytes/string length
	scale    uint8                  // 0..255 fixed point scale, bloom error probability 1/x (1..4)

	// encoder values for INSERT, UPDATE, QUERY
	isArray  bool             // field is a fixed size array
	path     []int            // reflect struct nested positions
	offset   uintptr          // struct field offset from reflect
	wireSize uint16           // wire encoding field size in bytes, min size for []byte & string
	iface    types.IfaceFlags // Go encoder default interfaces
	enum     *EnumDictionary  // dynamic enum data
}

// ExportedField is a performance improved version of Field
// containing exported fields for direct access in other packages
type ExportedField struct {
	Name       string
	Id         uint16
	Type       types.FieldType
	Flags      types.FieldFlags
	Compress   types.FieldCompression
	Index      types.IndexType
	IsVisible  bool
	IsInternal bool
	IsArray    bool
	Iface      types.IfaceFlags
	Scale      uint8
	Fixed      uint16
	Offset     uintptr
	Enum       *EnumDictionary
	path       []int
}

func NewField(typ types.FieldType) Field {
	return Field{
		typ:      typ,
		wireSize: uint16(typ.Size()),
	}
}

func (f *Field) Name() string {
	return f.name
}

func (f *Field) Id() uint16 {
	return f.id
}

func (f *Field) Type() types.FieldType {
	return f.typ
}

func (f *Field) Path() []int {
	return f.path
}

func (f *Field) Flags() types.FieldFlags {
	return f.flags
}

func (f *Field) Is(v types.FieldFlags) bool {
	return f.flags.Is(v)
}

func (f *Field) Can(v types.IfaceFlags) bool {
	return f.iface.Is(v)
}

func (f *Field) Compress() types.FieldCompression {
	return f.compress
}

func (f *Field) Index() types.IndexType {
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

func (f *Field) Enum() *EnumDictionary {
	return f.enum
}

func (f *Field) IsValid() bool {
	return len(f.name) > 0 && f.typ.IsValid()
}

func (f *Field) IsVisible() bool {
	return f.flags&(types.FieldFlagDeleted|types.FieldFlagInternal) == 0
}

func (f *Field) IsActive() bool {
	return !f.flags.Is(types.FieldFlagDeleted)
}

func (f *Field) IsInternal() bool {
	return f.flags.Is(types.FieldFlagInternal)
}

func (f *Field) IsPrimary() bool {
	return f.flags.Is(types.FieldFlagPrimary)
}

func (f *Field) IsIndexed() bool {
	return f.flags.Is(types.FieldFlagIndexed)
}

func (f *Field) IsEnum() bool {
	return f.flags.Is(types.FieldFlagEnum)
}

func (f *Field) IsFixedSize() bool {
	switch f.typ {
	case types.FieldTypeString, types.FieldTypeBytes:
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
	// switch f.typ {
	// case types.FieldTypeString, types.FieldTypeBytes:
	if f.fixed > 0 {
		return int(f.fixed)
	}
	// }
	return f.typ.Size()
}

// WithXXX methods do not use pointer receivers and always return a
// changed copy of the field.
func (f Field) WithName(n string) Field {
	f.name = n
	return f
}

func (f Field) WithFlags(v types.FieldFlags) Field {
	f.flags = v
	return f
}

func (f Field) WithCompression(c types.FieldCompression) Field {
	f.compress = c
	return f
}

func (f Field) WithFixed(n uint16) Field {
	f.fixed = n
	return f
}

func (f Field) WithScale(n uint8) Field {
	f.scale = n
	return f
}

func (f Field) WithEnum(d *EnumDictionary) Field {
	f.enum = d
	return f
}

func (f Field) WithIndex(kind types.IndexType) Field {
	f.index = kind
	if kind != types.IndexTypeNone {
		f.flags |= types.FieldFlagIndexed
	} else {
		f.flags &^= types.FieldFlagIndexed
	}
	return f
}

// @deprecated
// func (f Field) WithGoType(typ reflect.Type, path []int, ofs uintptr) Field {
// 	var iface types.IfaceFlags
// 	// detect marshaler types
// 	if typ.Implements(binaryMarshalerType) {
// 		iface |= types.IfaceBinaryMarshaler
// 	}
// 	if reflect.PointerTo(typ).Implements(binaryUnmarshalerType) {
// 		iface |= types.IfaceBinaryUnmarshaler
// 	}
// 	if typ.Implements(textMarshalerType) {
// 		iface |= types.IfaceTextMarshaler
// 	}
// 	if reflect.PointerTo(typ).Implements(textUnmarshalerType) {
// 		iface |= types.IfaceTextUnmarshaler
// 	}
// 	if typ.Implements(stringerType) {
// 		iface |= types.IfaceStringer
// 	}
// 	f.wireSize = uint16(typ.Size())
// 	if typ.Kind() == reflect.Array && typ.Elem().Kind() == reflect.Uint8 {
// 		f.isArray = true
// 		f.wireSize = uint16(typ.Len())
// 	}
// 	if f.flags.Is(types.FieldFlagEnum) {
// 		f.wireSize = 2
// 	}
// 	f.path = path
// 	f.offset = ofs
// 	f.iface = iface
// 	return f
// }

func (f *Field) Validate() error {
	// require scale on decimal fields only
	if f.scale != 0 {
		var minScale, maxScale uint8
		switch f.typ {
		case types.FieldTypeDecimal32:
			maxScale = num.MaxDecimal32Precision
		case types.FieldTypeDecimal64:
			maxScale = num.MaxDecimal64Precision
		case types.FieldTypeDecimal128:
			maxScale = num.MaxDecimal128Precision
		case types.FieldTypeDecimal256:
			maxScale = num.MaxDecimal256Precision
		default:
			if f.index == types.IndexTypeBloom {
				minScale, maxScale = 1, 4
			} else {
				return fmt.Errorf("scale unsupported on type %s", f.typ)
			}
		}
		if _, err := validateInt("scale", int(f.scale), int(minScale), int(maxScale)); err != nil {
			return err
		}
	}

	// require fixed on string/byte fields only
	if f.fixed != 0 {
		if _, err := validateInt("fixed", int(f.fixed), 1, int(MAX_FIXED)); err != nil {
			return err
		}
		switch f.typ {
		case types.FieldTypeBytes, types.FieldTypeString:
			// ok
		default:
			return fmt.Errorf("fixed unsupported on type %s", f.typ)
		}
	}

	// require index kind in range
	if f.index < 0 || f.index > types.IndexTypeBloom {
		return fmt.Errorf("invalid index kind %d", f.index)
	}

	// require index flag when index is != none
	if f.index > 0 && f.flags&types.FieldFlagIndexed == 0 {
		return fmt.Errorf("missing indexed flag with index kind set")
	}

	// require integer index on int fields only
	if f.index == types.IndexTypeInt {
		switch f.typ {
		case types.FieldTypeInt64, types.FieldTypeInt32,
			types.FieldTypeInt16, types.FieldTypeInt8,
			types.FieldTypeUint64, types.FieldTypeUint32,
			types.FieldTypeUint16, types.FieldTypeUint8:
		default:
			return fmt.Errorf("unsupported integer index on type %s", f.typ)
		}
	}

	// require bloom scale 1..4
	if f.index == types.IndexTypeBloom {
		if _, err := validateInt("bloom factor", int(f.scale), 1, 4); err != nil {
			return err
		}
	}

	// require uint16 for enum types
	if f.flags.Is(types.FieldFlagEnum) && f.typ != types.FieldTypeUint16 {
		return fmt.Errorf("invalid type %s for enum, requires uint16", f.typ)
	}

	return nil
}

func (f *Field) Codec() OpCode {
	if !f.IsVisible() {
		return OpCodeSkip
	}

	switch f.typ {
	case types.FieldTypeDatetime:
		return OpCodeDateTime

	case types.FieldTypeInt64:
		return OpCodeInt64

	case types.FieldTypeInt32:
		return OpCodeInt32

	case types.FieldTypeInt16:
		return OpCodeInt16

	case types.FieldTypeInt8:
		return OpCodeInt8

	case types.FieldTypeUint64:
		return OpCodeUint64

	case types.FieldTypeUint32:
		return OpCodeUint32

	case types.FieldTypeUint16:
		if f.flags.Is(types.FieldFlagEnum) {
			return OpCodeEnum
		}
		return OpCodeUint16

	case types.FieldTypeUint8:
		return OpCodeUint8

	case types.FieldTypeFloat64:
		return OpCodeFloat64

	case types.FieldTypeFloat32:
		return OpCodeFloat32

	case types.FieldTypeBoolean:
		return OpCodeBool

	case types.FieldTypeString:
		switch {
		case f.fixed > 0:
			return OpCodeFixedString
		case f.Can(types.IfaceTextMarshaler):
			return OpCodeMarshalText
		case f.Can(types.IfaceStringer):
			return OpCodeStringer
		default:
			return OpCodeString
		}

	case types.FieldTypeBytes:
		switch {
		case f.Can(types.IfaceBinaryMarshaler):
			return OpCodeMarshalBinary
		case f.isArray:
			return OpCodeFixedArray
		case f.fixed > 0:
			return OpCodeFixedBytes
		default:
			return OpCodeBytes
		}

	case types.FieldTypeInt256:
		return OpCodeInt256

	case types.FieldTypeInt128:
		return OpCodeInt128

	case types.FieldTypeDecimal256:
		return OpCodeDecimal256

	case types.FieldTypeDecimal128:
		return OpCodeDecimal128

	case types.FieldTypeDecimal64:
		return OpCodeDecimal64

	case types.FieldTypeDecimal32:
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
		err = EncodeInt(w, code, val)

	case OpCodeFixedArray,
		OpCodeFixedString,
		OpCodeFixedBytes,
		OpCodeString,
		OpCodeStringer,
		OpCodeBytes,
		OpCodeMarshalBinary,
		OpCodeMarshalText:

		err = EncodeBytes(w, val, f.fixed)

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
		v, ok := val.(num.Int128)
		if ok {
			_, err = w.Write(v.Bytes())
		}

	case OpCodeInt256:
		v, ok := val.(num.Int256)
		if ok {
			_, err = w.Write(v.Bytes())
		}

	case OpCodeDecimal32:
		v, ok := val.(num.Decimal32)
		if ok {
			_, err = w.Write(Uint32Bytes(uint32(v.Int32())))
		}

	case OpCodeDecimal64:
		v, ok := val.(num.Decimal64)
		if ok {
			_, err = w.Write(Uint64Bytes(uint64(v.Int64())))
		}

	case OpCodeDecimal128:
		v, ok := val.(num.Decimal128)
		if ok {
			_, err = w.Write(v.Int128().Bytes())
		}

	case OpCodeDecimal256:
		v, ok := val.(num.Decimal256)
		if ok {
			_, err = w.Write(v.Int256().Bytes())
		}

	case OpCodeEnum:
		v, ok := val.(string)
		if !ok {
			err = ErrInvalidValueType
			return
		}
		if f.enum == nil {
			return ErrEnumUndefined
		}
		code, ok := f.enum.Code(v)
		if !ok {
			return ErrInvalidValue
		}
		err = EncodeInt(w, OpCodeUint16, code)
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
	case types.FieldTypeDatetime:
		_, err = r.Read(buf[:8])
		i64, _ := ReadInt64(buf[:8])
		val = time.Unix(0, i64).UTC()

	case types.FieldTypeInt64:
		_, err = r.Read(buf[:8])
		i64, _ := ReadInt64(buf[:8])
		val = i64

	case types.FieldTypeInt32:
		_, err = r.Read(buf[:4])
		i32, _ := ReadInt32(buf[:4])
		val = i32

	case types.FieldTypeInt16:
		_, err = r.Read(buf[:2])
		i16, _ := ReadInt16(buf[:2])
		val = i16

	case types.FieldTypeInt8:
		_, err = r.Read(buf[:1])
		i8, _ := ReadInt8(buf[:1])
		val = i8

	case types.FieldTypeUint64:
		_, err = r.Read(buf[:8])
		u64, _ := ReadUint64(buf[:8])
		val = u64

	case types.FieldTypeUint32:
		_, err = r.Read(buf[:4])
		u32, _ := ReadUint32(buf[:4])
		val = u32

	case types.FieldTypeUint16:
		_, err = r.Read(buf[:2])
		u16, _ := ReadUint16(buf[:2])
		if f.flags.Is(types.FieldFlagEnum) {
			if f.enum != nil {
				enum, ok := f.enum.Value(u16)
				if ok {
					val = enum
				} else {
					err = ErrInvalidValue
				}
			} else {
				err = ErrEnumUndefined
			}
		} else {
			val = u16
		}

	case types.FieldTypeUint8:
		_, err = r.Read(buf[:1])
		u8, _ := ReadUint8(buf[:1])
		val = u8

	case types.FieldTypeFloat64:
		_, err = r.Read(buf[:8])
		u64, _ := ReadUint64(buf[:8])
		val = math.Float64frombits(u64)

	case types.FieldTypeFloat32:
		_, err = r.Read(buf[:4])
		u32, _ := ReadUint32(buf[:4])
		val = math.Float32frombits(u32)

	case types.FieldTypeBoolean:
		_, err = r.Read(buf[:1])
		b := buf[0] > 0
		val = b

	case types.FieldTypeString:
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

	case types.FieldTypeBytes:
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

	case types.FieldTypeInt256:
		_, err = r.Read(buf[:32])
		i256 := num.Int256FromBytes(buf[:32])
		val = i256

	case types.FieldTypeInt128:
		_, err = r.Read(buf[:16])
		i128 := num.Int128FromBytes(buf[:16])
		val = i128

	case types.FieldTypeDecimal256:
		_, err = r.Read(buf[:32])
		d256 := num.NewDecimal256(num.Int256FromBytes(buf[:32]), f.scale)
		val = d256

	case types.FieldTypeDecimal128:
		_, err = r.Read(buf[:16])
		d128 := num.NewDecimal128(num.Int128FromBytes(buf[:16]), f.scale)
		val = d128

	case types.FieldTypeDecimal64:
		_, err = r.Read(buf[:8])
		i64, _ := ReadInt64(buf[:8])
		d64 := num.NewDecimal64(i64, f.scale)
		val = d64

	case types.FieldTypeDecimal32:
		_, err = r.Read(buf[:4])
		i32, _ := ReadInt32(buf[:4])
		d32 := num.NewDecimal32(i32, f.scale)
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

func (f *ExportedField) WireSize() int {
	// switch f.Type {
	// case types.FieldTypeString, types.FieldTypeBytes:
	if f.Fixed > 0 {
		return int(f.Fixed)
	}
	// }
	return f.Type.Size()
}

func (f Field) WriteTo(w *bytes.Buffer) error {
	// id: u16
	binary.Write(w, LE, f.id)

	// name: string
	binary.Write(w, LE, uint16(len(f.name)))
	w.WriteString(f.name)

	// typ, flags, compression, index: byte
	binary.Write(w, LE, []byte{
		byte(f.typ),
		byte(f.flags),
		byte(f.compress),
		byte(f.index),
	})

	// fixed: u16
	binary.Write(w, LE, f.fixed)

	// scale: u8
	binary.Write(w, LE, f.scale)

	return nil
}

func (f *Field) ReadFrom(buf *bytes.Buffer) (err error) {
	if buf.Len() < 11 {
		return io.ErrShortBuffer
	}

	// id: u16
	err = binary.Read(buf, LE, &f.id)
	if err != nil {
		return
	}

	// name: string
	var l uint16
	err = binary.Read(buf, LE, &l)
	if err != nil {
		return
	}
	f.name = string(buf.Next(int(l)))
	if len(f.name) != int(l) {
		return io.ErrShortBuffer
	}

	// typ, flags, compression, index: byte
	if buf.Len() < 7 {
		return io.ErrShortBuffer
	}
	f.typ = types.FieldType(buf.Next(1)[0])
	f.flags = types.FieldFlags(buf.Next(1)[0])
	f.compress = types.FieldCompression(buf.Next(1)[0])
	f.index = types.IndexType(buf.Next(1)[0])

	// fixed: u16
	binary.Read(buf, LE, &f.fixed)

	// scale: u8
	binary.Read(buf, LE, &f.scale)

	return f.Validate()
}
