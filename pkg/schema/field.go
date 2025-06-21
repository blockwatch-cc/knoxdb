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

type FieldType = types.FieldType

const (
	FT_TIMESTAMP = types.FieldTypeTimestamp
	FT_I8        = types.FieldTypeInt8
	FT_I16       = types.FieldTypeInt16
	FT_I32       = types.FieldTypeInt32
	FT_I64       = types.FieldTypeInt64
	FT_I128      = types.FieldTypeInt128
	FT_I256      = types.FieldTypeInt256
	FT_U8        = types.FieldTypeUint8
	FT_U16       = types.FieldTypeUint16
	FT_U32       = types.FieldTypeUint32
	FT_U64       = types.FieldTypeUint64
	FT_F32       = types.FieldTypeFloat32
	FT_F64       = types.FieldTypeFloat64
	FT_D32       = types.FieldTypeDecimal32
	FT_D64       = types.FieldTypeDecimal64
	FT_D128      = types.FieldTypeDecimal128
	FT_D256      = types.FieldTypeDecimal256
	FT_BOOL      = types.FieldTypeBoolean
	FT_STRING    = types.FieldTypeString
	FT_BYTES     = types.FieldTypeBytes
	FT_BIGINT    = types.FieldTypeBigint
	FT_TIME      = types.FieldTypeTime
	FT_DATE      = types.FieldTypeDate
)

type Field struct {
	// schema values for CREATE TABLE
	name     string                 // field name from struct tag or variable name
	id       uint16                 // unique lifetime id of the field
	typ      types.FieldType        // schema field type from struct tag or Go type
	flags    types.FieldFlags       // schema flags from struct tag
	compress types.BlockCompression // data compression from struct tag
	index    types.IndexType        // index type: none, hash, int, bloom
	fixed    uint16                 // 0..65535 fixed size array/bytes/string length
	scale    uint8                  // 0..255 fixed point scale, time scale, bloom error probability 1/x (1..4)

	// encoder values for INSERT, UPDATE, QUERY
	path     []int            // reflect struct nested positions
	offset   uintptr          // struct field offset from reflect
	wireSize uint16           // wire encoding field size in bytes, min size for []byte & string
	iface    types.IfaceFlags // Go encoder default interfaces
}

// ExportedField is a performance improved version of Field
// containing exported fields for direct access from external packages
type ExportedField struct {
	Name       string
	Id         uint16
	Type       types.FieldType
	Flags      types.FieldFlags
	Compress   types.BlockCompression
	Index      types.IndexType
	IsVisible  bool
	IsInternal bool
	IsNullable bool
	IsEnum     bool
	Iface      types.IfaceFlags
	Scale      uint8
	Fixed      uint16
	Offset     uintptr
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

func (f *Field) Can(v types.IfaceFlags) bool {
	return f.iface.Is(v)
}

func (f *Field) Compress() types.BlockCompression {
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

func (f *Field) IsValid() bool {
	return len(f.name) > 0 && f.typ.IsValid()
}

func (f *Field) Is(v types.FieldFlags) bool {
	return f.flags.Is(v)
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

func (f *Field) IsNullable() bool {
	return f.flags.Is(types.FieldFlagNullable)
}

func (f *Field) IsEnum() bool {
	return f.flags.Is(types.FieldFlagEnum)
}

func (f *Field) IsFixedSize() bool {
	switch f.typ {
	case FT_STRING, FT_BYTES:
		return f.fixed > 0
	default:
		return true
	}
}

func (f *Field) IsInterface() bool {
	return f.iface != 0
}

func (f *Field) IsCompressed() bool {
	return f.compress > types.BlockCompressNone
}

func (f *Field) WireSize() int {
	switch f.typ {
	case FT_STRING, FT_BYTES:
		if f.fixed > 0 {
			return int(f.fixed)
		}
	}
	return f.typ.Size()
}

func (f *Field) TimeFormat() string {
	switch f.typ {
	case FT_TIMESTAMP, FT_DATE:
		return timeScaleFormats[f.scale]
	case FT_TIME:
		return timeOnlyFormats[f.scale]
	default:
		return ""
	}
}

func (f *Field) GoType() reflect.Type {
	if f.typ == FT_BYTES && f.fixed > 0 {
		return reflect.ArrayOf(int(f.fixed), reflect.TypeFor[byte]())
	}
	if f.typ == FT_U16 && f.IsEnum() {
		return reflect.TypeFor[string]()
	}
	return reflect.TypeOf(f.typ.Zero())
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

func (f Field) WithCompression(c types.BlockCompression) Field {
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

func (f Field) WithIndex(kind types.IndexType) Field {
	f.index = kind
	if kind != types.IndexTypeNone {
		f.flags |= types.FieldFlagIndexed
	} else {
		f.flags &^= types.FieldFlagIndexed
	}
	return f
}

func (f *Field) Validate() error {
	// require scale on decimal fields only
	if f.scale != 0 {
		var minScale, maxScale uint8
		switch f.typ {
		case FT_D32:
			maxScale = num.MaxDecimal32Precision
		case FT_D64:
			maxScale = num.MaxDecimal64Precision
		case FT_D128:
			maxScale = num.MaxDecimal128Precision
		case FT_D256:
			maxScale = num.MaxDecimal256Precision
		case FT_TIMESTAMP:
			maxScale = uint8(TIME_SCALE_SECOND)
		case FT_TIME:
			maxScale = uint8(TIME_SCALE_SECOND)
		case FT_DATE:
			minScale = uint8(TIME_SCALE_DAY)
			maxScale = uint8(TIME_SCALE_DAY)
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
		case FT_BYTES, FT_STRING:
			// ok
		default:
			return fmt.Errorf("fixed unsupported on type %s", f.typ)
		}
	}

	// require index kind in range
	if !f.index.IsValid() {
		return fmt.Errorf("invalid index kind %d", f.index)
	}

	// require index flag when index is != none
	if f.index > 0 && f.flags&types.FieldFlagIndexed == 0 {
		return fmt.Errorf("missing indexed flag with index kind set")
	}

	// require integer index on int fields only
	if f.index == types.IndexTypeInt {
		switch f.typ {
		case FT_I64, FT_I32, FT_I16, FT_I8, FT_U64, FT_U32, FT_U16, FT_U8:
			// ok
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
	if f.flags.Is(types.FieldFlagEnum) && f.typ != FT_U16 {
		return fmt.Errorf("invalid type %s for enum, requires uint16", f.typ)
	}

	return nil
}

func (f *Field) Codec() OpCode {
	if !f.IsVisible() {
		return OpCodeSkip
	}

	switch f.typ {
	case FT_TIMESTAMP:
		return OpCodeTimestamp

	case FT_DATE:
		return OpCodeDate

	case FT_TIME:
		return OpCodeTime

	case FT_I64:
		return OpCodeInt64

	case FT_I32:
		return OpCodeInt32

	case FT_I16:
		return OpCodeInt16

	case FT_I8:
		return OpCodeInt8

	case FT_U64:
		return OpCodeUint64

	case FT_U32:
		return OpCodeUint32

	case FT_U16:
		if f.flags.Is(types.FieldFlagEnum) {
			return OpCodeEnum
		}
		return OpCodeUint16

	case FT_U8:
		return OpCodeUint8

	case FT_F64:
		return OpCodeFloat64

	case FT_F32:
		return OpCodeFloat32

	case FT_BOOL:
		return OpCodeBool

	case FT_STRING:
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

	case FT_BYTES:
		switch {
		case f.Can(types.IfaceBinaryMarshaler):
			return OpCodeMarshalBinary
		case f.fixed > 0:
			return OpCodeFixedBytes
		default:
			return OpCodeBytes
		}

	case FT_I256:
		return OpCodeInt256

	case FT_I128:
		return OpCodeInt128

	case FT_D256:
		return OpCodeDecimal256

	case FT_D128:
		return OpCodeDecimal128

	case FT_D64:
		return OpCodeDecimal64

	case FT_D32:
		return OpCodeDecimal32

	case FT_BIGINT:
		return OpCodeBigInt

	default:
		return OpCodeInvalid
	}
}

// Simple per field encoder used to wire-encode individual typed values
// found in query conditions.
func (f *Field) Encode(w io.Writer, val any, layout binary.ByteOrder) (err error) {
	if val == nil {
		return ErrNilValue
	}

	// init error, will be overwritten by write branches below
	err = ErrInvalidValueType

	switch code := f.Codec(); code {
	default:
		err = EncodeInt(w, code, val, layout)

	case OpCodeFixedString,
		OpCodeFixedBytes,
		OpCodeString,
		OpCodeStringer,
		OpCodeBytes,
		OpCodeMarshalBinary,
		OpCodeMarshalText:

		err = EncodeBytes(w, val, f.fixed, layout)

	case OpCodeBool:
		b, ok := val.(bool)
		if ok {
			if b {
				_, err = w.Write([]byte{1})
			} else {
				_, err = w.Write([]byte{0})
			}
		}

	case OpCodeTimestamp, OpCodeDate, OpCodeTime:
		tv, ok := val.(time.Time)
		if ok {
			err = EncodeInt(w, OpCodeUint64, TimeScale(f.scale).ToUnix(tv), layout)
		}

	case OpCodeFloat32:
		switch v := val.(type) {
		case float32:
			err = EncodeInt(w, OpCodeUint32, math.Float32bits(v), layout)
		case float64:
			err = EncodeInt(w, OpCodeUint32, math.Float32bits(float32(v)), layout)
		}

	case OpCodeFloat64:
		switch v := val.(type) {
		case float32:
			err = EncodeInt(w, OpCodeUint64, math.Float64bits(float64(v)), layout)
		case float64:
			err = EncodeInt(w, OpCodeUint64, math.Float64bits(v), layout)
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
			err = EncodeInt(w, OpCodeUint32, uint32(v.Int32()), layout)
		}

	case OpCodeDecimal64:
		v, ok := val.(num.Decimal64)
		if ok {
			err = EncodeInt(w, OpCodeUint64, uint64(v.Int64()), layout)
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
		err = EncodeInt(w, OpCodeUint16, val.(uint16), layout)

	case OpCodeBigInt:
		v, ok := val.(num.Big)
		if ok {
			err = EncodeBytes(w, v.Bytes(), 0, layout)
		}
	}
	return
}

// Simple per field decoder used to wire-decode individual typed values
// found in query conditions.
func (f *Field) Decode(r io.Reader, layout binary.ByteOrder) (val any, err error) {
	var (
		buf [32]byte
		n   int
	)
	switch f.typ {
	case FT_TIMESTAMP, FT_TIME:
		_, err = r.Read(buf[:8])
		val = time.Unix(0, int64(layout.Uint64(buf[:8]))).UTC()

	case FT_DATE:
		_, err = r.Read(buf[:8])
		val = FromUnixDays(int64(layout.Uint64(buf[:8])))

	case FT_I64:
		_, err = r.Read(buf[:8])
		val = int64(layout.Uint64(buf[:8]))

	case FT_I32:
		_, err = r.Read(buf[:4])
		val = int32(layout.Uint32(buf[:4]))

	case FT_I16:
		_, err = r.Read(buf[:2])
		val = int16(layout.Uint16(buf[:2]))

	case FT_I8:
		_, err = r.Read(buf[:1])
		val = int8(buf[0])

	case FT_U64:
		_, err = r.Read(buf[:8])
		val = layout.Uint64(buf[:8])

	case FT_U32:
		_, err = r.Read(buf[:4])
		val = layout.Uint32(buf[:4])

	case FT_U16:
		_, err = r.Read(buf[:2])
		val = layout.Uint16(buf[:2])

	case FT_U8:
		_, err = r.Read(buf[:1])
		val = buf[0]

	case FT_F64:
		_, err = r.Read(buf[:8])
		val = math.Float64frombits(layout.Uint64(buf[:8]))

	case FT_F32:
		_, err = r.Read(buf[:4])
		val = math.Float32frombits(layout.Uint32(buf[:4]))

	case FT_BOOL:
		_, err = r.Read(buf[:1])
		val = buf[0] > 0

	case FT_STRING:
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
			u32 := layout.Uint32(buf[:4])
			b := make([]byte, int(u32))
			n, err = r.Read(b)
			val = string(b[:n])
		}

	case FT_BYTES:
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
			u32 := layout.Uint32(buf[:4])
			b := make([]byte, int(u32))
			n, err = r.Read(b)
			val = b[:n]
		}

	case FT_I256:
		_, err = r.Read(buf[:32])
		i256 := num.Int256FromBytes(buf[:32])
		val = i256

	case FT_I128:
		_, err = r.Read(buf[:16])
		i128 := num.Int128FromBytes(buf[:16])
		val = i128

	case FT_D256:
		_, err = r.Read(buf[:32])
		d256 := num.NewDecimal256(num.Int256FromBytes(buf[:32]), f.scale)
		val = d256

	case FT_D128:
		_, err = r.Read(buf[:16])
		d128 := num.NewDecimal128(num.Int128FromBytes(buf[:16]), f.scale)
		val = d128

	case FT_D64:
		_, err = r.Read(buf[:8])
		d64 := num.NewDecimal64(int64(layout.Uint64(buf[:8])), f.scale)
		val = d64

	case FT_D32:
		_, err = r.Read(buf[:4])
		d32 := num.NewDecimal32(int32(layout.Uint32(buf[:4])), f.scale)
		val = d32

	case FT_BIGINT:
		_, err = r.Read(buf[:4])
		if err != nil {
			return
		}
		u32 := layout.Uint32(buf[:4])
		b := make([]byte, int(u32))
		n, err = r.Read(b)
		val = num.NewBigFromBytes(b[:n])

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
	// case FT_STRING, types.FieldTypeBytes:
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
	f.compress = types.BlockCompression(buf.Next(1)[0])
	f.index = types.IndexType(buf.Next(1)[0])

	// fixed: u16
	binary.Read(buf, LE, &f.fixed)

	// scale: u8
	binary.Read(buf, LE, &f.scale)

	// init related properties
	f.wireSize = uint16(f.typ.Size())

	return f.Validate()
}
