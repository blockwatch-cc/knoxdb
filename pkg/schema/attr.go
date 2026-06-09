package schema

import (
	"fmt"
	"math"
	"strconv"
	"time"
	"unsafe"
)

var (
	_ Marshaler   = Attr{}
	_ Unmarshaler = (*Attr)(nil)

	AttrSchema = SchemaOf([]*Field{
		FieldOf(Uint8, WithName("typeid")),
		FieldOf(String, WithName("key")),
		FieldOf(Bytes, WithName("value")),
	})
)

// Attr is an allocation free key/value type for named union types.
// It efficiently encodes itself via Writer and decodes via View.
// Note Attr is not directly compatible with Schema due to its
// private fields and the unsupported *byte pointer.
type Attr struct {
	_   [0]func() // disallow ==
	typ FieldType // Uint8:  1 byte type id for value
	key string    // String: 1 byte len + max 255 byte data
	num uint64    // Uint64: 8 byte fixed
	buf *byte     // -- NOT supported in schema
}

func (a Attr) MarshalSchema(w *Writer) error {
	if len(a.key) > MAX_STRING {
		return ErrLongValue
	}
	w.WriteUint8(uint8(a.typ))
	w.WriteString(a.key)
	switch a.typ {
	case Uint64, Int64, Float64, Timestamp, Time, Date, Duration:
		w.buf.WriteByte(8)
		w.writeU64(a.num)
	case Uint32, Int32, Float32:
		w.buf.WriteByte(4)
		w.writeU32(uint32(a.num))
	case Uint16, Int16:
		w.buf.WriteByte(2)
		w.writeU16(uint16(a.num))
	case Uint8, Int8, Boolean:
		w.buf.WriteByte(1)
		w.buf.WriteByte(uint8(a.num))
	case String:
		if a.num > MAX_BYTES {
			return ErrLongValue
		}
		w.WriteBytes(unsafe.Slice(a.buf, a.num))
	default: // List, Map, Text, Binary, Bytes, DecimalXX, Bigint
		return ErrInvalidValueType
	}
	w.next()
	return nil
}

func (a *Attr) UnmarshalSchema(v *View) error {
	a.typ = FieldType(v.Uint8(0))
	a.key = v.String(1)
	buf := v.Bytes(2)
	switch a.typ {
	case Uint64, Int64, Float64, Timestamp, Time, Date, Duration:
		a.num = v.layout.Uint64(buf[:8])
	case Uint32, Int32, Float32:
		a.num = uint64(v.layout.Uint32(buf[:4]))
	case Uint16, Int16:
		a.num = uint64(v.layout.Uint16(buf[:2]))
	case Uint8, Int8, Boolean:
		a.num = uint64(buf[0])
	case String:
		a.num = uint64(len(buf))
		if a.num > 0 {
			a.buf = &buf[0]
		}
	default: // List, Map, Text, Binary, Bytes, DecimalXX, Bigint
		return ErrInvalidValueType
	}
	return nil
}

func Int64Attr(key string, value int64) Attr {
	return Attr{typ: Int64, key: key, num: uint64(value)}
}

func Int32Attr(key string, value int32) Attr {
	return Attr{typ: Int32, key: key, num: uint64(value)}
}

func Int16Attr(key string, value int16) Attr {
	return Attr{typ: Int16, key: key, num: uint64(value)}
}

func Int8Attr(key string, value int8) Attr {
	return Attr{typ: Int8, key: key, num: uint64(value)}
}

func Uint64Attr(key string, value uint64) Attr {
	return Attr{typ: Uint64, key: key, num: value}
}

func Uint32Attr(key string, value uint32) Attr {
	return Attr{typ: Uint32, key: key, num: uint64(value)}
}

func Uint16Attr(key string, value uint16) Attr {
	return Attr{typ: Uint16, key: key, num: uint64(value)}
}

func Uint8Attr(key string, value uint8) Attr {
	return Attr{typ: Uint8, key: key, num: uint64(value)}
}

func Float64Attr(key string, value float64) Attr {
	return Attr{typ: Float64, key: key, num: math.Float64bits(value)}
}

func Float32Attr(key string, value float32) Attr {
	return Attr{typ: Float32, key: key, num: uint64(math.Float32bits(value))}
}

func TimestampAttr(key string, value time.Time) Attr {
	return Attr{typ: Timestamp, key: key, num: uint64(TIME_SCALE_NANO.ToUnix(value))}
}

func DurationAttr(key string, value time.Duration) Attr {
	return Attr{typ: Duration, key: key, num: uint64(TIME_SCALE_NANO.Int64(value))}
}

func TimeAttr(key string, value time.Time) Attr {
	return Attr{typ: Time, key: key, num: uint64(TIME_SCALE_NANO.ToUnix(value))}
}

func DateAttr(key string, value time.Time) Attr {
	return Attr{typ: Time, key: key, num: uint64(TIME_SCALE_DAY.ToUnix(value))}
}

func BoolAttr(key string, value bool) Attr {
	u := uint64(0)
	if value {
		u = 1
	}
	return Attr{typ: Boolean, key: key, num: u}
}

func StringAttr(key, value string) Attr {
	return Attr{
		typ: String,
		key: key,
		num: uint64(len(value)),
		buf: unsafe.StringData(value),
	}
}

func (a Attr) Key() string {
	return a.key
}

func (a Attr) Int64() int64 {
	a.ensureType(Int64)
	return int64(a.num)
}

func (a Attr) Int32() int32 {
	a.ensureType(Int32)
	return int32(a.num)
}

func (a Attr) Int16() int16 {
	a.ensureType(Int16)
	return int16(a.num)
}

func (a Attr) Int8() int8 {
	a.ensureType(Int8)
	return int8(a.num)
}

func (a Attr) Uint64() uint64 {
	a.ensureType(Uint64)
	return a.num
}

func (a Attr) Uint32() uint32 {
	a.ensureType(Uint32)
	return uint32(a.num)
}

func (a Attr) Uint16() uint16 {
	a.ensureType(Uint16)
	return uint16(a.num)
}

func (a Attr) Uint8() uint8 {
	a.ensureType(Uint8)
	return uint8(a.num)
}

func (a Attr) Float64() float64 {
	a.ensureType(Float64)
	return math.Float64frombits(a.num)
}

func (a Attr) Float32() float32 {
	a.ensureType(Float32)
	return math.Float32frombits(uint32(a.num))
}

func (a Attr) Bool() bool {
	a.ensureType(Boolean)
	return a.num == 1
}

func (a Attr) Timestamp() time.Time {
	a.ensureType(Timestamp)
	return a.time()
}

func (a Attr) Duration() time.Duration {
	a.ensureType(Duration)
	return time.Duration(a.num)
}

func (a Attr) Time() time.Time {
	a.ensureType(Time)
	return a.time()
}

func (a Attr) Date() time.Time {
	a.ensureType(Date)
	return TIME_SCALE_DAY.FromUnix(int64(a.num))
}

func (a Attr) String() string {
	if a.typ == String {
		return a.str()
	}
	var buf []byte
	return string(a.append(buf))
}

func (a Attr) Value() any {
	switch a.typ {
	case Uint64:
		return a.num
	case Int64:
		return a.Int64()
	case Float64:
		return a.Float64()
	case Timestamp:
		return a.Timestamp()
	case Duration:
		return a.Duration()
	case Time:
		return a.Time()
	case Date:
		return a.Date()
	case Uint32:
		return a.Uint32()
	case Int32:
		return a.Int32()
	case Float32:
		return a.Float32()
	case Uint16:
		return a.Uint16()
	case Int16:
		return a.Int16()
	case Uint8:
		return a.Uint8()
	case Int8:
		return a.Int8()
	case Boolean:
		return a.Bool()
	case String:
		return a.String()
	default:
		return nil
	}
}

func (a Attr) ensureType(typ FieldType) {
	if a.typ != typ {
		panic(fmt.Sprintf("attr type is %s, not %s", a.typ, typ))
	}
}

func (a Attr) str() string {
	return unsafe.String(a.buf, a.num)
}

func (a Attr) time() time.Time {
	return TIME_SCALE_NANO.FromUnix(int64(a.num))
}

func (a Attr) append(dst []byte) []byte {
	switch a.typ {
	case String:
		return append(dst, a.str()...)
	case Int64, Int32, Int16, Int8:
		return strconv.AppendInt(dst, int64(a.num), 10)
	case Uint64, Uint32, Uint16, Uint8:
		return strconv.AppendUint(dst, a.num, 10)
	case Float64:
		return strconv.AppendFloat(dst, a.Float64(), 'g', -1, 64)
	case Float32:
		return strconv.AppendFloat(dst, float64(a.Float32()), 'g', -1, 32)
	case Boolean:
		return strconv.AppendBool(dst, a.Bool())
	case Timestamp:
		return append(dst, TIME_SCALE_NANO.Format(a.time())...)
	case Time:
		return append(dst, TIME_SCALE_SECOND.FormatTime(a.time())...)
	case Date:
		return append(dst, TIME_SCALE_DAY.Format(a.time())...)
	case Duration:
		return append(dst, a.Duration().String()...)
	default:
		panic(fmt.Sprintf("bad type: %s", a.typ))
	}
}
