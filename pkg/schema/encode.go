// Copyright (c) 2024-2026 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package schema

import (
	"bytes"
	"encoding/binary"
	"math"
	"math/big"
	"sync"
	"time"

	"blockwatch.cc/knoxdb/pkg/num"
	"blockwatch.cc/knoxdb/pkg/schema/enum"
	"blockwatch.cc/knoxdb/pkg/util"
)

var scratchBufferPool = sync.Pool{
	New: func() any {
		var buf [32]byte
		return &buf
	},
}

// WriteValue serializes the value of an individual field to wire format.
// It is used in queries and for metadata index nodes. WriteValue accepts
// values of both the logical Go type (e.g. Decimal64 or time.Time) and
// the physical representation (e.g. int64) for a field and converts if
// compatible. It does not cast or type convert otherwise. Byte arrays
// of type [n]byte must be converted to byte slice []byte when used as
// argument.
func (f *Field) WriteValue(w *bytes.Buffer, val any, layout binary.ByteOrder) (err error) {
	if val == nil {
		return ErrNilValue
	}

	// init error, will be overwritten by write branches below
	err = ErrInvalidValueType

	// get scratch buffer
	// buf := unsafe.Slice(scratchBufferPool.Get().(*byte), 32)[:32]
	buf := scratchBufferPool.Get().(*[32]byte)

	switch f.Type {
	case Timestamp, Time, Date:
		switch tv := val.(type) {
		case *time.Time:
			layout.PutUint64(buf[:], uint64(TimeScale(f.Scale).ToUnix(*tv)))
			_, err = w.Write(buf[:8])
		case time.Time:
			layout.PutUint64(buf[:], uint64(TimeScale(f.Scale).ToUnix(tv)))
			_, err = w.Write(buf[:8])
		case int64:
			layout.PutUint64(buf[:], uint64(tv))
			_, err = w.Write(buf[:8])
		}

	case Duration:
		switch d := val.(type) {
		case time.Duration:
			layout.PutUint64(buf[:], uint64(TimeScale(f.Scale).Int64(d)))
			_, err = w.Write(buf[:8])
		case int64:
			layout.PutUint64(buf[:], uint64(d))
			_, err = w.Write(buf[:8])
		}

	case Int64:
		if v, ok := val.(int64); ok {
			layout.PutUint64(buf[:], uint64(v))
			_, err = w.Write(buf[:8])
		}

	case Int32:
		if v, ok := val.(int32); ok {
			layout.PutUint32(buf[:], uint32(v))
			_, err = w.Write(buf[:4])
		}

	case Int16:
		if v, ok := val.(int16); ok {
			layout.PutUint16(buf[:], uint16(v))
			_, err = w.Write(buf[:2])
		}

	case Int8:
		if v, ok := val.(int8); ok {
			err = w.WriteByte(uint8(v))
		}

	case Uint64:
		if v, ok := val.(uint64); ok {
			layout.PutUint64(buf[:], v)
			_, err = w.Write(buf[:8])
		}

	case Uint32:
		if v, ok := val.(uint32); ok {
			layout.PutUint32(buf[:], v)
			_, err = w.Write(buf[:4])
		}

	case Uint16:
		if v, ok := val.(uint16); ok {
			layout.PutUint16(buf[:], v)
			_, err = w.Write(buf[:2])
		}

	case Enum:
		switch v := val.(type) {
		case uint16:
			layout.PutUint16(buf[:], v)
			_, err = w.Write(buf[:2])
		case string:
			if f.Enum != nil {
				val, ok := f.Enum.Code(v)
				if ok {
					layout.PutUint16(buf[:], val)
					_, err = w.Write(buf[:2])
				} else {
					err = enum.ErrEnumNoCode
				}
			} else {
				err = ErrEnumUndefined
			}
		}

	case Uint8:
		if v, ok := val.(uint8); ok {
			err = w.WriteByte(v)
		}

	case Float64:
		if v, ok := val.(float64); ok {
			layout.PutUint64(buf[:], math.Float64bits(v))
			_, err = w.Write(buf[:8])
		}

	case Float32:
		if v, ok := val.(float32); ok {
			layout.PutUint32(buf[:], math.Float32bits(v))
			_, err = w.Write(buf[:4])
		}

	case Boolean:
		if v, ok := val.(bool); ok {
			if v {
				buf[0] = 1
			} else {
				buf[0] = 0
			}
			_, err = w.Write(buf[:1])
		}

	case String, Bytes:
		// Note: user must convert arrays to byte slice
		var (
			bval []byte
			ok   = true
		)
		switch v := val.(type) {
		case []byte:
			bval = v
		case string:
			bval = util.UnsafeGetBytes(v)
		default:
			ok = false
		}
		if ok {
			l := len(bval)
			if f.IsArray() {
				// fixed len
				if l != int(f.Scale) {
					err = ErrShortValue
				} else {
					_, err = w.Write(bval)
				}
			} else {
				// 1 byte len
				if l <= MAX_STRING {
					_, err = w.Write([]byte{byte(l)})
					if err == nil {
						_, err = w.Write(bval)
					}
				} else {
					err = ErrLongValue
				}
			}
		}

	case Text, Binary, List, Map, Variant:
		// 4 byte len
		var (
			bval []byte
			ok   = true
		)
		switch v := val.(type) {
		case []byte:
			bval = v
		case string:
			bval = util.UnsafeGetBytes(v)
		default:
			ok = false
		}
		if ok {
			l := len(bval)
			layout.PutUint32(buf[:], uint32(l))
			_, err = w.Write(buf[:4])
			if err == nil {
				_, err = w.Write(bval)
			}
		}

	case Int256:
		if v, ok := val.(num.Int256); ok {
			_, err = w.Write(v.Bytes())
		}

	case Int128:
		if v, ok := val.(num.Int128); ok {
			_, err = w.Write(v.Bytes())
		}

	case Decimal256:
		switch v := val.(type) {
		case num.Decimal256:
			_, err = w.Write(v.Int256().Bytes())
		case num.Int256:
			_, err = w.Write(v.Bytes())
		}

	case Decimal128:
		switch v := val.(type) {
		case num.Decimal128:
			_, err = w.Write(v.Int128().Bytes())
		case num.Int128:
			_, err = w.Write(v.Bytes())
		}

	case Decimal64:
		switch v := val.(type) {
		case num.Decimal64:
			layout.PutUint64(buf[:], uint64(v.Int64()))
			_, err = w.Write(buf[:8])
		case int64:
			layout.PutUint64(buf[:], uint64(v))
			_, err = w.Write(buf[:8])
		}

	case Decimal32:
		switch v := val.(type) {
		case num.Decimal32:
			layout.PutUint32(buf[:], uint32(v.Int32()))
			_, err = w.Write(buf[:4])
		case int32:
			layout.PutUint32(buf[:], uint32(v))
			_, err = w.Write(buf[:4])
		}

	case Bigint:
		// 1 byte len
		var (
			bval []byte
			ok   = true
		)
		switch v := val.(type) {
		case num.Big:
			bval = v.Bytes()
		case *big.Int:
			bval = v.Bytes()
		case []byte:
			bval = v
		default:
			ok = false
		}
		if ok {
			l := len(bval)
			if l <= MAX_BYTES {
				_, err = w.Write([]byte{byte(l)})
				if err == nil {
					_, err = w.Write(bval)
				}
			} else {
				err = ErrLongValue
			}
		}

	case Union:
		v, ok := val.(UnionValue)
		if ok {
			err = v.MarshalBuffer(w, layout)
		}

	default:
		err = ErrInvalidField
	}

	scratchBufferPool.Put(buf)
	return
}

// ReadValue reads and decodes an individual typed value from wire format.
// It is used in query conditions. Read always emits the logical type for
// a field, e.g. time.Time instead of int64. Byte arrays [n]byte are returned
// as byte slices []byte with the original array length to avoid reflect calls.
// ReadValue panics when remaining buffer content is too short.
func (f *Field) ReadValue(buf *bytes.Buffer, layout binary.ByteOrder) (val any, err error) {
	switch f.Type {
	case Timestamp, Time, Date:
		val = TimeScale(f.Scale).FromUnix(int64(layout.Uint64(buf.Next(8))))

	case Duration:
		val = TimeScale(f.Scale).Duration(int64(layout.Uint64(buf.Next(8))))

	case Int64:
		val = int64(layout.Uint64(buf.Next(8)))

	case Int32:
		val = int32(layout.Uint32(buf.Next(4)))

	case Int16:
		val = int16(layout.Uint16(buf.Next(2)))

	case Int8:
		val = int8(buf.Next(1)[0])

	case Uint64:
		val = layout.Uint64(buf.Next(8))

	case Uint32:
		val = layout.Uint32(buf.Next(4))

	case Uint16:
		val = layout.Uint16(buf.Next(2))

	case Uint8:
		val = buf.Next(1)[0]

	case Float64:
		val = math.Float64frombits(layout.Uint64(buf.Next(8)))

	case Float32:
		val = math.Float32frombits(layout.Uint32(buf.Next(4)))

	case Boolean:
		val = buf.Next(1)[0] > 0

	case String:
		if f.IsArray() {
			val = util.UnsafeGetString(buf.Next(int(f.Scale)))
		} else {
			val = util.UnsafeGetString(buf.Next(int(buf.Next(1)[0])))
		}

	case Text:
		val = util.UnsafeGetString(buf.Next(int(layout.Uint32(buf.Next(4)))))

	case Bytes:
		if f.IsArray() {
			val = buf.Next(int(f.Scale))
		} else {
			val = buf.Next(int(buf.Next(1)[0]))
		}

	case Binary, List, Map, Variant:
		val = buf.Next(int(layout.Uint32(buf.Next(4))))

	case Int256:
		val = num.Int256FromBytes(buf.Next(32))

	case Int128:
		val = num.Int128FromBytes(buf.Next(16))

	case Decimal256:
		val = num.NewDecimal256(num.Int256FromBytes(buf.Next(32)), f.Scale)

	case Decimal128:
		val = num.NewDecimal128(num.Int128FromBytes(buf.Next(16)), f.Scale)

	case Decimal64:
		val = num.NewDecimal64(int64(layout.Uint64(buf.Next(8))), f.Scale)

	case Decimal32:
		val = num.NewDecimal32(int32(layout.Uint32(buf.Next(4))), f.Scale)

	case Bigint:
		val = num.NewBigFromBytes(buf.Next(int(buf.Next(1)[0])))

	case Union:
		var u UnionValue
		err = u.UnmarshalBuffer(buf.Next(int(buf.Next(1)[0])), layout)
		val = u

	case Enum:
		if f.Enum == nil {
			return nil, ErrEnumUndefined
		}
		if s, ok := f.Enum.Value(layout.Uint16(buf.Next(2))); ok {
			val = s
		} else {
			err = ErrInvalidEnum
		}

	default:
		err = ErrInvalidField
	}

	return
}
