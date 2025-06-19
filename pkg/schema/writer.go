// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package schema

import (
	"bytes"
	"encoding/binary"
	"math"
	"math/big"
	"time"

	"blockwatch.cc/knoxdb/pkg/num"
	"blockwatch.cc/knoxdb/pkg/util"
)

// Constructs wire encoded messages from typed values.
type Writer struct {
	schema      *Schema        // target schema
	buf         []byte         // wire size encoded message buffer (min/fixed sizes)
	ofs         []int          // field offsets in wire encoding (as if all fields were fixed)
	len         []int          // field lengths in wire encoding
	dyn         map[int][]byte // data for variable length fields
	internal    bool           // flag indicating to output internal fields
	firstDyn    int            // position of first dynamic field (optimization)
	minWireSize int            // min size with or without internal fields
	layout      binary.ByteOrder
}

func NewWriter(s *Schema, layout binary.ByteOrder) *Writer {
	b := &Writer{layout: layout}
	return b.initFromSchema(s)
}

func NewInternalWriter(s *Schema, layout binary.ByteOrder) *Writer {
	b := &Writer{
		layout:   layout,
		internal: true,
	}
	return b.initFromSchema(s)
}

func (b Writer) Len() int {
	sz := b.minWireSize
	for _, v := range b.dyn {
		sz += len(v)
	}
	return sz
}

func (b *Writer) initFromSchema(s *Schema) *Writer {
	b.schema = s
	b.firstDyn = -1
	b.ofs = make([]int, len(s.fields))
	b.len = make([]int, len(s.fields))
	if !s.isFixedSize {
		b.dyn = make(map[int][]byte)
	}
	var ofs int
	for i, f := range s.fields {
		// skip deleted fields
		if !f.IsActive() {
			b.ofs[i] = -1
			continue
		}
		// skip internal fields unless requested
		if f.IsInternal() && !b.internal {
			b.ofs[i] = -1
			continue
		}
		sz := f.typ.Size()
		if f.fixed > 0 {
			sz = int(f.fixed)
		}
		b.minWireSize += sz
		if !f.IsFixedSize() && b.firstDyn < 0 {
			b.firstDyn = i
		}
		b.ofs[i] = ofs
		b.len[i] = sz
		ofs += sz
	}
	b.buf = make([]byte, b.minWireSize)
	return b
}

func (b *Writer) Reset() {
	if b.dyn != nil {
		clear(b.dyn)
	}
	clear(b.buf)
}

func (b *Writer) Write(i int, val any) error {
	// sanity checks
	if val == nil {
		return ErrNilValue
	}
	if i < 0 || i >= len(b.ofs) {
		return ErrInvalidField
	}

	// init write offset
	x, y := b.ofs[i], b.ofs[i]+b.len[i]
	field := &b.schema.fields[i]

	// skip hidden fields
	if x < 0 {
		return nil
	}

	var err error
	switch field.typ {
	case FT_U64:
		if u64, ok := val.(uint64); ok {
			b.layout.PutUint64(b.buf[x:y], u64)
		} else {
			err = ErrInvalidValueType
		}

	case FT_STRING, FT_BYTES:
		var buf []byte
		switch v := val.(type) {
		case []byte:
			buf = v
		case string:
			buf = util.UnsafeGetBytes(v)
		default:
			err = ErrInvalidValueType
		}
		if field.IsFixedSize() {
			// fixed size
			if len(buf) == int(field.fixed) {
				copy(b.buf[x:y], buf)
			} else {
				err = ErrShortValue
			}
		} else {
			// variable size
			b.dyn[i] = bytes.Clone(buf)
		}

	case FT_TIMESTAMP:
		switch tm := val.(type) {
		case time.Time:
			b.layout.PutUint64(b.buf[x:y], uint64(TimeScale(field.scale).ToUnix(tm)))
		case int64:
			b.layout.PutUint64(b.buf[x:y], uint64(tm))
		default:
			err = ErrInvalidValueType
		}

	case FT_I64:
		if i64, ok := val.(int64); ok {
			b.layout.PutUint64(b.buf[x:y], uint64(i64))
		} else {
			err = ErrInvalidValueType
		}

	case FT_F64:
		if f64, ok := val.(float64); ok {
			b.layout.PutUint64(b.buf[x:y], math.Float64bits(f64))
		} else {
			err = ErrInvalidValueType
		}

	case FT_F32:
		if f32, ok := val.(float32); ok {
			b.layout.PutUint32(b.buf[x:y], math.Float32bits(f32))
		} else {
			err = ErrInvalidValueType
		}

	case FT_BOOL:
		if v, ok := val.(bool); ok {
			if v {
				b.buf[x] = 1
			} else {
				b.buf[x] = 0
			}
		} else {
			err = ErrInvalidValueType
		}

	case FT_I32:
		if i32, ok := val.(int32); ok {
			b.layout.PutUint32(b.buf[x:y], uint32(i32))
		} else {
			err = ErrInvalidValueType
		}

	case FT_I16:
		if i16, ok := val.(int16); ok {
			b.layout.PutUint16(b.buf[x:y], uint16(i16))
		} else {
			err = ErrInvalidValueType
		}

	case FT_I8:
		if i8, ok := val.(int8); ok {
			b.buf[x] = uint8(i8)
		} else {
			err = ErrInvalidValueType
		}

	case FT_U32:
		if u32, ok := val.(uint32); ok {
			b.layout.PutUint32(b.buf[x:y], u32)
		} else {
			err = ErrInvalidValueType
		}

	case FT_U16:
		if u16, ok := val.(uint16); ok {
			b.layout.PutUint16(b.buf[x:y], u16)
		} else {
			err = ErrInvalidValueType
		}

	case FT_U8:
		if u8, ok := val.(uint8); ok {
			b.buf[x] = u8
		} else {
			err = ErrInvalidValueType
		}

	case FT_I256:
		if i256, ok := val.(num.Int256); ok {
			copy(b.buf[x:y], i256.Bytes())
		} else {
			err = ErrInvalidValueType
		}

	case FT_I128:
		if i128, ok := val.(num.Int128); ok {
			copy(b.buf[x:y], i128.Bytes())
		} else {
			err = ErrInvalidValueType
		}

	case FT_D256:
		switch v := val.(type) {
		case num.Decimal256:
			copy(b.buf[x:y], v.Int256().Bytes())
		case num.Int256:
			copy(b.buf[x:y], v.Bytes())
		default:
			err = ErrInvalidValueType
		}

	case FT_D128:
		switch v := val.(type) {
		case num.Decimal128:
			copy(b.buf[x:y], v.Int128().Bytes())
		case num.Int128:
			copy(b.buf[x:y], v.Bytes())
		default:
			err = ErrInvalidValueType
		}

	case FT_D64:
		switch v := val.(type) {
		case num.Decimal64:
			b.layout.PutUint64(b.buf[x:y], uint64(v.Int64()))
		case int64:
			b.layout.PutUint64(b.buf[x:y], uint64(v))
		default:
			err = ErrInvalidValueType
		}

	case FT_D32:
		switch v := val.(type) {
		case num.Decimal32:
			b.layout.PutUint32(b.buf[x:y], uint32(v.Int64()))
		case int64:
			b.layout.PutUint32(b.buf[x:y], uint32(v))
		default:
			err = ErrInvalidValueType
		}

	case FT_BIGINT:
		var buf []byte
		switch v := val.(type) {
		case num.Big:
			buf = v.Bytes()
		case *big.Int:
			buf = v.Bytes()
		default:
			err = ErrInvalidValueType
		}
		// variable size (already in new allocated []byte)
		b.dyn[i] = buf

	default:
		err = ErrInvalidField
	}

	return err
}

func (b *Writer) Bytes() []byte {
	// fast path for fixed length schemas
	if b.dyn == nil {
		return bytes.Clone(b.buf)
	}

	// count variable len bytes
	var sz int
	for _, v := range b.dyn {
		sz += len(v)
	}

	// alloc output buffer
	buf := bytes.NewBuffer(make([]byte, 0, b.minWireSize+sz))

	// write data up to first variable length field
	n := b.firstDyn
	buf.Write(b.buf[:b.ofs[n]])

	// write remaining fields one by one
	for _, f := range b.schema.fields[n:] {
		// skip hidden fields
		if b.ofs[n] < 0 {
			n++
			continue
		}

		// write next visible field
		if f.IsFixedSize() {
			// write fixed size field
			buf.Write(b.buf[b.ofs[n] : b.ofs[n]+b.len[n]])
		} else {
			// write dynamic field
			val := b.dyn[n]
			var l [4]byte
			LE.PutUint32(l[:], uint32(len(val)))
			buf.Write(l[:])
			buf.Write(val)
		}
		n++
	}

	// return buffer contents
	return buf.Bytes()
}
