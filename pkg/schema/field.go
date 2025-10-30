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

type (
	FieldType  = types.FieldType
	FieldFlags = types.FieldFlags
	IndexType  = types.IndexType
	FilterType = types.FilterType
)

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

	F_PRIMARY  = types.FieldFlagPrimary
	F_TIMEBASE = types.FieldFlagTimebase
	F_ENUM     = types.FieldFlagEnum
	F_DELETED  = types.FieldFlagDeleted
	F_METADATA = types.FieldFlagMetadata
	F_NULLABLE = types.FieldFlagNullable

	I_HASH      = types.IndexTypeHash
	I_INT       = types.IndexTypeInt
	I_PK        = types.IndexTypePk
	I_COMPOSITE = types.IndexTypeComposite

	FL_BITS    = types.FilterTypeBits
	FL_BLOOM2B = types.FilterTypeBloom2b
	FL_BLOOM3B = types.FilterTypeBloom3b
	FL_BLOOM4B = types.FilterTypeBloom4b
	FL_BLOOM5B = types.FilterTypeBloom5b
	FL_BFUSE8  = types.FilterTypeBfuse8
	FL_BFUSE16 = types.FilterTypeBfuse16
)

type Field struct {
	// schema values for CREATE TABLE
	Name     string                 // field name from struct tag or variable name
	Id       uint16                 // unique lifetime id of the field
	Type     FieldType              // schema field type from struct tag or Go type
	Flags    FieldFlags             // schema flags from struct tag
	Compress types.BlockCompression // data compression from struct tag
	Filter   FilterType             // metadata filter type
	Fixed    uint16                 // 0..65535 fixed size array/bytes/string length
	Scale    uint8                  // 0..255 fixed point scale, time scale, bloom error probability 1/x (1..4)

	// encoder values for INSERT, UPDATE, QUERY
	Path   []int   // reflect struct nested positions
	Offset uintptr // struct field offset from reflect
	Size   uint16  // wire encoding field size in bytes, min size for []byte & string
}

func NewField(typ FieldType) *Field {
	return &Field{
		Type: typ,
		Size: uint16(typ.Size()),
	}
}

func (f *Field) Clone() *Field {
	clone := *f
	return &clone
}

func (f *Field) WireSize() int {
	switch f.Type {
	case FT_STRING, FT_BYTES:
		if f.Fixed > 0 {
			return int(f.Fixed)
		}
	}
	return int(f.Size)
}

func (f *Field) IsValid() bool {
	return len(f.Name) > 0 && f.Type.IsValid()
}

func (f *Field) Is(v FieldFlags) bool {
	return f.Flags.Is(v)
}

func (f *Field) IsVisible() bool {
	return f.Flags&(F_DELETED|F_METADATA) == 0
}

func (f *Field) IsActive() bool {
	return !f.Flags.Is(F_DELETED)
}

func (f *Field) IsMeta() bool {
	return f.Flags.Is(F_METADATA)
}

func (f *Field) IsPrimary() bool {
	return f.Flags.Is(F_PRIMARY)
}

func (f *Field) IsTimebase() bool {
	return f.Flags.Is(F_TIMEBASE)
}

func (f *Field) IsNullable() bool {
	return f.Flags.Is(F_NULLABLE)
}

func (f *Field) IsEnum() bool {
	return f.Flags.Is(F_ENUM)
}

func (f *Field) IsFixedSize() bool {
	switch f.Type {
	case FT_STRING, FT_BYTES:
		return f.Fixed > 0
	default:
		return true
	}
}

func (f *Field) IsCompressed() bool {
	return f.Compress > types.BlockCompressNone
}

func (f *Field) TimeFormat() string {
	switch f.Type {
	case FT_TIMESTAMP, FT_DATE:
		return timeScaleFormats[f.Scale]
	case FT_TIME:
		return timeOnlyFormats[f.Scale]
	default:
		return ""
	}
}

func (f *Field) GoType() reflect.Type {
	if f.Type == FT_BYTES && f.Fixed > 0 {
		return reflect.ArrayOf(int(f.Fixed), reflect.TypeFor[byte]())
	}
	if f.Type == FT_U16 && f.IsEnum() {
		return reflect.TypeFor[string]()
	}
	return reflect.TypeOf(f.Type.Zero())
}

func (f *Field) WithName(n string) *Field {
	f.Name = n
	return f
}

func (f *Field) WithFlags(v types.FieldFlags) *Field {
	f.Flags = v
	return f
}

func (f *Field) WithCompression(c types.BlockCompression) *Field {
	f.Compress = c
	return f
}

func (f *Field) WithFixed(n uint16) *Field {
	f.Fixed = n
	return f
}

func (f *Field) WithScale(n uint8) *Field {
	f.Scale = n
	return f
}

func (f *Field) WithFilter(typ FilterType) *Field {
	f.Filter = typ
	return f
}

func (f *Field) Validate() error {
	// require scale on decimal fields only
	if f.Scale != 0 {
		var minScale, maxScale uint8
		switch f.Type {
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
			return fmt.Errorf("field[%s]: scale unsupported on type %s", f.Name, f.Type)
		}
		if _, err := validateInt("scale", int(f.Scale), int(minScale), int(maxScale)); err != nil {
			return fmt.Errorf("field[%s]: %v", f.Name, err)
		}
	}

	// require valid filter types
	if f.Filter > 0 {
		if !f.Filter.IsValid() {
			return fmt.Errorf("field[%s]: invalid filter type %d", f.Name, f.Filter)
		}
	}

	// require fixed on string/byte fields only
	if f.Fixed != 0 {
		if _, err := validateInt("fixed", int(f.Fixed), 1, int(MAX_FIXED)); err != nil {
			return fmt.Errorf("field[%s]: %v", f.Name, err)
		}
		switch f.Type {
		case FT_BYTES, FT_STRING:
			// ok
		default:
			return fmt.Errorf("field[%s]: fixed unsupported on type %s", f.Name, f.Type)
		}
	}

	// require uint16 for enum types
	if f.Flags.Is(F_ENUM) && f.Type != FT_U16 {
		return fmt.Errorf("field[%s]: invalid type %s for enum, requires uint16", f.Name, f.Type)
	}

	// require timebase flag only to be used with timestamp fields
	if f.Flags.Is(F_TIMEBASE) && f.Type != FT_TIMESTAMP {
		return fmt.Errorf("field[%s]: invalid use of timebase flag on type %s", f.Name, f.Type)
	}

	return nil
}

func (f *Field) Codec() OpCode {
	if !f.IsVisible() {
		return OpCodeSkip
	}

	switch f.Type {
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
		if f.Flags.Is(F_ENUM) {
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
		if f.Fixed > 0 {
			return OpCodeFixedString
		} else {
			return OpCodeString
		}

	case FT_BYTES:
		if f.Fixed > 0 {
			return OpCodeFixedBytes
		} else {
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

// Encoder to serialize individual field values to wire format.
// Use to generate composite indexes or hashes for joins. Note
// this function encodes length-prefixed inline strings.
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
		OpCodeBytes:

		err = EncodeBytes(w, val, f.Fixed, layout)

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
			err = EncodeInt(w, OpCodeUint64, TimeScale(f.Scale).ToUnix(tv), layout)
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
	switch f.Type {
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
		if f.Fixed > 0 {
			b := make([]byte, f.Fixed)
			n, err = r.Read(b)
			if n < int(f.Fixed) {
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
		if f.Fixed > 0 {
			b := make([]byte, f.Fixed)
			n, err = r.Read(b)
			if n < int(f.Fixed) {
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
		d256 := num.NewDecimal256(num.Int256FromBytes(buf[:32]), f.Scale)
		val = d256

	case FT_D128:
		_, err = r.Read(buf[:16])
		d128 := num.NewDecimal128(num.Int128FromBytes(buf[:16]), f.Scale)
		val = d128

	case FT_D64:
		_, err = r.Read(buf[:8])
		d64 := num.NewDecimal64(int64(layout.Uint64(buf[:8])), f.Scale)
		val = d64

	case FT_D32:
		_, err = r.Read(buf[:4])
		d32 := num.NewDecimal32(int32(layout.Uint32(buf[:4])), f.Scale)
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
	dst := rval.FieldByIndex(f.Path)
	if dst.Kind() == reflect.Ptr {
		if dst.IsNil() && dst.CanSet() {
			dst.Set(reflect.New(dst.Type().Elem()))
		}
		dst = dst.Elem()
	}
	return dst
}

func (f *Field) WriteTo(w *bytes.Buffer) error {
	// id: u16
	binary.Write(w, LE, f.Id)

	// name: string
	binary.Write(w, LE, uint16(len(f.Name)))
	w.WriteString(f.Name)

	// typ, flags, compression: byte
	binary.Write(w, LE, []byte{
		byte(f.Type),
		byte(f.Flags),
		byte(f.Compress),
		byte(f.Filter),
	})

	// fixed: u16
	binary.Write(w, LE, f.Fixed)

	// scale: u8
	binary.Write(w, LE, f.Scale)

	return nil
}

func (f *Field) ReadFrom(buf *bytes.Buffer) (err error) {
	if buf.Len() < 11 {
		return io.ErrShortBuffer
	}

	// id: u16
	err = binary.Read(buf, LE, &f.Id)
	if err != nil {
		return
	}

	// name: string
	var l uint16
	err = binary.Read(buf, LE, &l)
	if err != nil {
		return
	}
	f.Name = string(buf.Next(int(l)))
	if len(f.Name) != int(l) {
		return io.ErrShortBuffer
	}

	// typ, flags, compression, filter: byte
	if buf.Len() < 7 {
		return io.ErrShortBuffer
	}
	f.Type = types.FieldType(buf.Next(1)[0])
	f.Flags = types.FieldFlags(buf.Next(1)[0])
	f.Compress = types.BlockCompression(buf.Next(1)[0])
	f.Filter = types.FilterType(buf.Next(1)[0])

	// fixed: u16
	binary.Read(buf, LE, &f.Fixed)

	// scale: u8
	binary.Read(buf, LE, &f.Scale)

	// init related properties
	f.Size = uint16(f.Type.Size())

	return f.Validate()
}
