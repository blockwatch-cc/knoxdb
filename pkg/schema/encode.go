// Copyright (c) 2024-2026 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package schema

import (
	"encoding/binary"
	"io"
	"math"
	"math/big"
	"sync"
	"time"
	"unsafe"

	"blockwatch.cc/knoxdb/pkg/num"
	"blockwatch.cc/knoxdb/pkg/schema/enum"
	"blockwatch.cc/knoxdb/pkg/util"
)

var scratchBufferPool = sync.Pool{
	New: func() any {
		var buf [32]byte
		return &buf[0]
	},
}

// WriteValue serializes the value of an individual field to wire format.
// It is used in queries and for metadata index nodes. WriteValue accepts
// values of both the logical Go type (e.g. Decimal64 or time.Time) and
// the physical representation (e.g. int64) for a field and converts if
// compatible. It does not cast or type convert otherwise. Byte arrays
// of type [n]byte must be converted to byte slice []byte when used as
// argument.
func (f *Field) WriteValue(w io.Writer, val any, layout binary.ByteOrder) (err error) {
	if val == nil {
		return ErrNilValue
	}

	// init error, will be overwritten by write branches below
	err = ErrInvalidValueType

	// get scratch buffer
	buf := unsafe.Slice(scratchBufferPool.Get().(*byte), 32)[:32]

	switch f.Type {
	case Timestamp, Time, Date:
		switch tv := val.(type) {
		case *time.Time:
			layout.PutUint64(buf, uint64(TimeScale(f.Scale).ToUnix(*tv)))
			_, err = w.Write(buf[:8])
		case time.Time:
			layout.PutUint64(buf, uint64(TimeScale(f.Scale).ToUnix(tv)))
			_, err = w.Write(buf[:8])
		case int64:
			layout.PutUint64(buf, uint64(tv))
			_, err = w.Write(buf[:8])
		}

	case Int64:
		if v, ok := val.(int64); ok {
			layout.PutUint64(buf, uint64(v))
			_, err = w.Write(buf[:8])
		}

	case Int32:
		if v, ok := val.(int32); ok {
			layout.PutUint32(buf, uint32(v))
			_, err = w.Write(buf[:4])
		}

	case Int16:
		if v, ok := val.(int16); ok {
			layout.PutUint16(buf, uint16(v))
			_, err = w.Write(buf[:2])
		}

	case Int8:
		if v, ok := val.(int8); ok {
			buf[0] = uint8(v)
			_, err = w.Write(buf[:1])
		}

	case Uint64:
		if v, ok := val.(uint64); ok {
			layout.PutUint64(buf, v)
			_, err = w.Write(buf[:8])
		}

	case Uint32:
		if v, ok := val.(uint32); ok {
			layout.PutUint32(buf, v)
			_, err = w.Write(buf[:4])
		}

	case Uint16:
		switch v := val.(type) {
		case uint16:
			layout.PutUint16(buf, v)
			_, err = w.Write(buf[:2])
		case string:
			if f.IsEnum() {
				if f.Enum != nil {
					val, ok := f.Enum.Code(v)
					if ok {
						layout.PutUint16(buf, val)
						_, err = w.Write(buf[:2])
					} else {
						err = enum.ErrEnumNoCode
					}
				} else {
					err = ErrEnumUndefined
				}
			}
		}

	case Uint8:
		if v, ok := val.(uint8); ok {
			buf[0] = v
			_, err = w.Write(buf[:1])
		}

	case Float64:
		if v, ok := val.(float64); ok {
			layout.PutUint64(buf, math.Float64bits(v))
			_, err = w.Write(buf[:8])
		}

	case Float32:
		if v, ok := val.(float32); ok {
			layout.PutUint32(buf, math.Float32bits(v))
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

	case Text, Binary, List, Map:
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
			layout.PutUint32(buf, uint32(l))
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
			layout.PutUint64(buf, uint64(v.Int64()))
			_, err = w.Write(buf[:8])
		case int64:
			layout.PutUint64(buf, uint64(v))
			_, err = w.Write(buf[:8])
		}

	case Decimal32:
		switch v := val.(type) {
		case num.Decimal32:
			layout.PutUint32(buf, uint32(v.Int32()))
			_, err = w.Write(buf[:4])
		case int32:
			layout.PutUint32(buf, uint32(v))
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

	default:
		err = ErrInvalidField
	}

	scratchBufferPool.Put(&buf[0])
	return
}

// ReadValue reads and decodes an individual typed value from wire format.
// It is used in query conditions. Read always emits the logical type for
// a field, e.g. time.Time instead of int64. Byte arrays [n]byte are returned
// as byte slices []byte with the original array length to avoid reflect calls.
func (f *Field) ReadValue(r io.Reader, layout binary.ByteOrder) (val any, err error) {
	var (
		buf = unsafe.Slice(scratchBufferPool.Get().(*byte), 32)[:32]
		n   int
	)
	switch f.Type {
	case Timestamp, Time, Date:
		_, err = r.Read(buf[:8])
		val = TimeScale(f.Scale).FromUnix(int64(layout.Uint64(buf[:8])))

	case Int64:
		_, err = r.Read(buf[:8])
		val = int64(layout.Uint64(buf[:8]))

	case Int32:
		_, err = r.Read(buf[:4])
		val = int32(layout.Uint32(buf[:4]))

	case Int16:
		_, err = r.Read(buf[:2])
		val = int16(layout.Uint16(buf[:2]))

	case Int8:
		_, err = r.Read(buf[:1])
		val = int8(buf[0])

	case Uint64:
		_, err = r.Read(buf[:8])
		val = layout.Uint64(buf[:8])

	case Uint32:
		_, err = r.Read(buf[:4])
		val = layout.Uint32(buf[:4])

	case Uint16:
		_, err = r.Read(buf[:2])
		val = layout.Uint16(buf[:2])
		if f.IsEnum() {
			if f.Enum != nil {
				s, ok := f.Enum.Value(val.(uint16))
				if ok {
					return s, nil
				}
				return nil, ErrInvalidEnum
			} else {
				return nil, ErrEnumUndefined
			}
		}

	case Uint8:
		_, err = r.Read(buf[:1])
		val = buf[0]

	case Float64:
		_, err = r.Read(buf[:8])
		val = math.Float64frombits(layout.Uint64(buf[:8]))

	case Float32:
		_, err = r.Read(buf[:4])
		val = math.Float32frombits(layout.Uint32(buf[:4]))

	case Boolean:
		_, err = r.Read(buf[:1])
		val = buf[0] > 0

	case String:
		if f.IsArray() {
			b := make([]byte, f.Scale)
			n, err = r.Read(b)
			if n < int(f.Scale) {
				return nil, ErrShortBuffer
			}
			val = util.UnsafeGetString(b[:n])
		} else {
			_, err = r.Read(buf[:1])
			if err != nil {
				return
			}
			b := make([]byte, int(buf[0]))
			n, err = r.Read(b)
			val = util.UnsafeGetString(b[:n])
		}

	case Text:
		_, err = r.Read(buf[:4])
		if err != nil {
			return
		}
		u32 := layout.Uint32(buf[:4])
		b := make([]byte, int(u32))
		n, err = r.Read(b)
		val = util.UnsafeGetString(b[:n])

	case Bytes:
		if f.IsArray() {
			b := make([]byte, f.Scale)
			n, err = r.Read(b)
			if n < int(f.Scale) {
				return nil, ErrShortBuffer
			}
			val = b[:n]
		} else {
			_, err = r.Read(buf[:1])
			if err != nil {
				return
			}
			b := make([]byte, int(buf[0]))
			n, err = r.Read(b)
			val = b[:n]
		}

	case Binary, List, Map:
		_, err = r.Read(buf[:4])
		if err != nil {
			return
		}
		u32 := layout.Uint32(buf[:4])
		b := make([]byte, int(u32))
		n, err = r.Read(b)
		val = b[:n]

	case Int256:
		_, err = r.Read(buf[:32])
		i256 := num.Int256FromBytes(buf[:32])
		val = i256

	case Int128:
		_, err = r.Read(buf[:16])
		i128 := num.Int128FromBytes(buf[:16])
		val = i128

	case Decimal256:
		_, err = r.Read(buf[:32])
		d256 := num.NewDecimal256(num.Int256FromBytes(buf[:32]), f.Scale)
		val = d256

	case Decimal128:
		_, err = r.Read(buf[:16])
		d128 := num.NewDecimal128(num.Int128FromBytes(buf[:16]), f.Scale)
		val = d128

	case Decimal64:
		_, err = r.Read(buf[:8])
		d64 := num.NewDecimal64(int64(layout.Uint64(buf[:8])), f.Scale)
		val = d64

	case Decimal32:
		_, err = r.Read(buf[:4])
		d32 := num.NewDecimal32(int32(layout.Uint32(buf[:4])), f.Scale)
		val = d32

	case Bigint:
		_, err = r.Read(buf[:1])
		if err != nil {
			return
		}
		var b [256]byte
		n, err = r.Read(b[:buf[0]])
		val = num.NewBigFromBytes(b[:n])

	default:
		err = ErrInvalidField
	}

	scratchBufferPool.Put(&buf[0])
	return
}
