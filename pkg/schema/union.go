// Copyright (c) 2026 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package schema

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math"
	"strconv"
	"time"
	"unsafe"

	"blockwatch.cc/knoxdb/pkg/num"
)

var (
	// ensure UnionValue implements schema marshaling
	_ Marshaler   = UnionValue{}
	_ Unmarshaler = (*UnionValue)(nil)

	// UnionType defines the columnar schema used for Union values. It
	// represents the internal storage layout, but not record encoding.
	// All fields are metadata tagged, so they are not picked up by
	// default encoders.
	UnionType = SchemaOf([]*Field{
		{Type: Uint8, Name: "utag", Flags: FlagMetadata},  // id+1 field type
		{Type: Uint64, Name: "unum", Flags: FlagMetadata}, // id+2 numeric values
		{Type: Bytes, Name: "uval", Flags: FlagMetadata},  // id+3 string, bytes, bigint, i128/256 values
	})
)

// UnionValue is a tagged union for built-in primitive schema types.
// It encodes itself via Marshaler/Unmarshaler methods into a space
// efficient record layout `len(1) [ typeid(1) ] [ buf(0..254) ]`.
// The encoding is on purpose similar to raw bytes encoding to make
// it compose easily with writers and readers.
//
// Limitations
// - only primitive native types are allowed (no binary, text)
// - no nested types (list, map, union, variant)
// - string/byte max length is 254 bytes (one less than usual)
// - no type traits (array, time scale, decimal scale)
type UnionValue struct {
	_   [0]func() // disallow ==
	num uint64    // numeric value or buffer length
	buf *byte     // buffer for >8 byte types (string, bytes, bigint, i128, i256)
	typ FieldType // type tag
}

// Writes record encoding `len(1) [ typeid(1) ] [ buf(0..254) ]`.
// Null/empty unions write len=0 and no typeid/value. Note marshalers
// write an explicit length prefix byte, but for unmarshalers this
// byte is stripped.
func (u UnionValue) MarshalSchema(w *Writer) error {
	return u.MarshalBuffer(w.buf, w.layout)
}

func (u *UnionValue) UnmarshalSchema(v *View) error {
	return u.UnmarshalBuffer(v.Buffer(), v.layout)
}

// MarshalBuffer writes the union's record encoding of to a buffer using
// layout as byte order for numeric values.
func (u UnionValue) MarshalBuffer(buf *bytes.Buffer, layout binary.ByteOrder) error {
	switch u.typ {
	case 0:
		buf.WriteByte(0) // null value
	case Uint64, Int64, Float64, Timestamp, Time, Date, Duration:
		buf.WriteByte(9)
		buf.WriteByte(byte(u.typ))
		b := scratchBufferPool.Get().(*[32]byte)
		layout.PutUint64(b[:], u.num)
		buf.Write(b[:8])
		scratchBufferPool.Put(b)
	case Uint32, Int32, Float32:
		buf.WriteByte(5)
		buf.WriteByte(byte(u.typ))
		b := scratchBufferPool.Get().(*[32]byte)
		layout.PutUint32(b[:], uint32(u.num))
		buf.Write(b[:4])
		scratchBufferPool.Put(b)
	case Uint16, Int16:
		buf.WriteByte(3)
		buf.WriteByte(byte(u.typ))
		b := scratchBufferPool.Get().(*[32]byte)
		layout.PutUint16(b[:], uint16(u.num))
		buf.Write(b[:2])
		scratchBufferPool.Put(b)
	case Uint8, Int8, Boolean:
		buf.WriteByte(2)
		buf.WriteByte(byte(u.typ))
		buf.WriteByte(byte(u.num))
	case Int128:
		buf.WriteByte(17)
		buf.WriteByte(byte(u.typ))
		buf.Write(unsafe.Slice(u.buf, 16))
	case Int256:
		buf.WriteByte(33)
		buf.WriteByte(byte(u.typ))
		buf.Write(unsafe.Slice(u.buf, 32))
	case String, Bigint, Bytes:
		if u.num > MAX_BYTES-1 {
			return ErrLongValue
		}
		buf.WriteByte(byte(u.num) + 1)
		buf.WriteByte(byte(u.typ))
		buf.Write(unsafe.Slice(u.buf, u.num))
	default: // List, Map, Text, Binary, DecimalXX, Union, Variant
		return ErrInvalidValueType
	}
	return nil
}

func (u *UnionValue) UnmarshalBuffer(buf []byte, layout binary.ByteOrder) error {
	u.num = 0
	u.buf = nil
	l := len(buf)
	if l == 0 {
		return nil
	}
	u.typ = FieldType(buf[0])
	buf = buf[1:]
	switch u.typ {
	case Uint64, Int64, Float64, Timestamp, Time, Date, Duration:
		if len(buf) < 8 {
			return ErrShortBuffer
		}
		u.num = layout.Uint64(buf[:8])
	case Uint32, Int32, Float32:
		if len(buf) < 4 {
			return ErrShortBuffer
		}
		u.num = uint64(layout.Uint32(buf[:4]))
	case Uint16, Int16:
		if len(buf) < 2 {
			return ErrShortBuffer
		}
		u.num = uint64(layout.Uint16(buf[:2]))
	case Uint8, Int8, Boolean:
		if len(buf) < 1 {
			return ErrShortBuffer
		}
		u.num = uint64(buf[0])
	case Int128:
		if len(buf) < 16 {
			return ErrShortBuffer
		}
		u.num = 16
		u.buf = &buf[0]
	case Int256:
		if len(buf) < 32 {
			return ErrShortBuffer
		}
		u.num = 32
		u.buf = &buf[0]
	case String, Bytes, Bigint:
		u.num = uint64(len(buf))
		if u.num > 0 {
			u.buf = &buf[0]
		}
	default: // List, Map, Text, Binary, Bytes, DecimalXX, Bigint
		return ErrInvalidValueType
	}
	return nil
}

func Int64Union(value int64) UnionValue {
	return UnionValue{typ: Int64, num: uint64(value)}
}

func Int32Union(value int32) UnionValue {
	return UnionValue{typ: Int32, num: uint64(value)}
}

func Int16Union(value int16) UnionValue {
	return UnionValue{typ: Int16, num: uint64(value)}
}

func Int8Union(value int8) UnionValue {
	return UnionValue{typ: Int8, num: uint64(value)}
}

func Uint64Union(value uint64) UnionValue {
	return UnionValue{typ: Uint64, num: value}
}

func Uint32Union(value uint32) UnionValue {
	return UnionValue{typ: Uint32, num: uint64(value)}
}

func Uint16Union(value uint16) UnionValue {
	return UnionValue{typ: Uint16, num: uint64(value)}
}

func Uint8Union(value uint8) UnionValue {
	return UnionValue{typ: Uint8, num: uint64(value)}
}

func Float64Union(value float64) UnionValue {
	return UnionValue{typ: Float64, num: math.Float64bits(value)}
}

func Float32Union(value float32) UnionValue {
	return UnionValue{typ: Float32, num: uint64(math.Float32bits(value))}
}

func TimestampUnion(value time.Time) UnionValue {
	return UnionValue{typ: Timestamp, num: uint64(TIME_SCALE_NANO.ToUnix(value))}
}

func DurationUnion(value time.Duration) UnionValue {
	return UnionValue{typ: Duration, num: uint64(TIME_SCALE_NANO.Int64(value))}
}

func TimeUnion(value time.Time) UnionValue {
	return UnionValue{typ: Time, num: uint64(TIME_SCALE_NANO.ToUnix(value))}
}

func DateUnion(value time.Time) UnionValue {
	return UnionValue{typ: Date, num: uint64(TIME_SCALE_DAY.ToUnix(value))}
}

func BoolUnion(value bool) UnionValue {
	u := uint64(0)
	if value {
		u = 1
	}
	return UnionValue{typ: Boolean, num: u}
}

func StringUnion(value string) UnionValue {
	return UnionValue{
		typ: String,
		num: uint64(len(value)),
		buf: unsafe.StringData(value),
	}
}

func BytesUnion(value []byte) UnionValue {
	return UnionValue{
		typ: Bytes,
		num: uint64(len(value)),
		buf: unsafe.SliceData(value),
	}
}

func Int128Union(value num.Int128) UnionValue {
	return UnionValue{
		typ: Int128,
		num: 16,
		buf: unsafe.SliceData(value.Bytes()),
	}
}

func Int256Union(value num.Int256) UnionValue {
	return UnionValue{
		typ: Int256,
		num: 32,
		buf: unsafe.SliceData(value.Bytes()),
	}
}

func BigintUnion(value num.Big) UnionValue {
	buf := value.Bytes()
	return UnionValue{
		typ: Bigint,
		num: uint64(len(buf)),
		buf: unsafe.SliceData(buf),
	}
}

func (u UnionValue) IsNull() bool {
	return u.typ == 0
}

func (u UnionValue) Type() FieldType {
	return u.typ
}

func (u UnionValue) Int64() int64 {
	u.ensureType(Int64)
	return int64(u.num)
}

func (u UnionValue) Int32() int32 {
	u.ensureType(Int32)
	return int32(u.num)
}

func (u UnionValue) Int16() int16 {
	u.ensureType(Int16)
	return int16(u.num)
}

func (u UnionValue) Int8() int8 {
	u.ensureType(Int8)
	return int8(u.num)
}

func (u UnionValue) Uint64() uint64 {
	u.ensureType(Uint64)
	return u.num
}

func (u UnionValue) Uint32() uint32 {
	u.ensureType(Uint32)
	return uint32(u.num)
}

func (u UnionValue) Uint16() uint16 {
	u.ensureType(Uint16)
	return uint16(u.num)
}

func (u UnionValue) Uint8() uint8 {
	u.ensureType(Uint8)
	return uint8(u.num)
}

func (u UnionValue) Float64() float64 {
	u.ensureType(Float64)
	return math.Float64frombits(u.num)
}

func (u UnionValue) Float32() float32 {
	u.ensureType(Float32)
	return math.Float32frombits(uint32(u.num))
}

func (u UnionValue) Bool() bool {
	u.ensureType(Boolean)
	return u.num == 1
}

func (u UnionValue) Timestamp() time.Time {
	u.ensureType(Timestamp)
	return u.time()
}

func (u UnionValue) Duration() time.Duration {
	u.ensureType(Duration)
	return time.Duration(u.num)
}

func (u UnionValue) Time() time.Time {
	u.ensureType(Time)
	return TIME_SCALE_SECOND.FromUnix(int64(u.num))
}

func (u UnionValue) Date() time.Time {
	u.ensureType(Date)
	return TIME_SCALE_DAY.FromUnix(int64(u.num))
}

func (u UnionValue) String() string {
	if u.typ == String {
		return u.str()
	}
	var buf []byte
	return string(u.append(buf))
}

func (u UnionValue) Bytes() []byte {
	u.ensureType(Bytes)
	return u.bytes()
}

func (u UnionValue) Bigint() num.Big {
	u.ensureType(Bigint)
	return num.NewBigFromBytes(u.bytes())
}

func (u UnionValue) Int128() num.Int128 {
	u.ensureType(Int128)
	return num.Int128FromBytes(u.bytes())
}

func (u UnionValue) Int256() num.Int256 {
	u.ensureType(Int256)
	return num.Int256FromBytes(u.bytes())
}

func (u UnionValue) Value() any {
	switch u.typ {
	case Timestamp:
		return u.Timestamp()
	case Duration:
		return u.Duration()
	case Time:
		return u.Time()
	case Date:
		return u.Date()
	case Uint64:
		return u.num
	case Uint32:
		return uint32(u.num)
	case Uint16:
		return uint16(u.num)
	case Uint8:
		return uint8(u.num)
	case Int64:
		return int64(u.num)
	case Int32:
		return int32(u.num)
	case Int16:
		return int16(u.num)
	case Int8:
		return int8(u.num)
	case Boolean:
		return u.num == 1
	case Float64:
		return math.Float64frombits(u.num)
	case Float32:
		return math.Float32frombits(uint32(u.num))
	case String:
		return u.str()
	case Bytes:
		return u.bytes()
	case Bigint:
		return num.NewBigFromBytes(u.bytes())
	case Int128:
		return num.Int128FromBytes(u.bytes())
	case Int256:
		return num.Int256FromBytes(u.bytes())
	default:
		return nil
	}
}

func (u UnionValue) ensureType(typ FieldType) {
	if u.typ != typ {
		panic(fmt.Sprintf("union type is %s, not %s", u.typ, typ))
	}
}

func (u UnionValue) str() string {
	return unsafe.String(u.buf, u.num)
}

func (u UnionValue) bytes() []byte {
	return unsafe.Slice(u.buf, u.num)
}

func (u UnionValue) time() time.Time {
	return TIME_SCALE_NANO.FromUnix(int64(u.num))
}

func (u UnionValue) append(dst []byte) []byte {
	switch u.typ {
	case Timestamp:
		return append(dst, TIME_SCALE_NANO.Format(u.time())...)
	case Duration:
		return append(dst, u.Duration().String()...)
	case Time:
		return append(dst, TIME_SCALE_SECOND.FormatTime(u.time())...)
	case Date:
		return append(dst, TIME_SCALE_DAY.Format(u.time())...)
	case Int64, Int32, Int16, Int8:
		return strconv.AppendInt(dst, int64(u.num), 10)
	case Uint64, Uint32, Uint16, Uint8:
		return strconv.AppendUint(dst, u.num, 10)
	case Boolean:
		return strconv.AppendBool(dst, u.Bool())
	case Float64:
		return strconv.AppendFloat(dst, u.Float64(), 'g', -1, 64)
	case Float32:
		return strconv.AppendFloat(dst, float64(u.Float32()), 'g', -1, 32)
	case String:
		return append(dst, u.str()...)
	case Bytes:
		return hex.AppendEncode(dst, u.bytes())
	case Int128:
		return append(dst, u.Int128().String()...)
	case Int256:
		return append(dst, u.Int256().String()...)
	case Bigint:
		return append(dst, u.Bigint().String()...)
	default:
		panic(fmt.Sprintf("bad union type: %s", u.typ))
	}
}
