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
	firstDyn    int            // position of first dynamic field (optimization)
	minWireSize int            // min size
	layout      binary.ByteOrder
}

func NewWriter(s *Schema, layout binary.ByteOrder) *Writer {
	w := &Writer{layout: layout}
	return w.initFromSchema(s)
}

func (w Writer) Len() int {
	sz := w.minWireSize
	for _, v := range w.dyn {
		sz += len(v)
	}
	return sz
}

func (w *Writer) initFromSchema(s *Schema) *Writer {
	w.schema = s
	w.firstDyn = -1
	w.ofs = make([]int, len(s.Fields))
	w.len = make([]int, len(s.Fields))
	if !s.IsFixedSize {
		w.dyn = make(map[int][]byte)
	}
	var ofs int
	for i, f := range s.Fields {
		// skip deleted fields
		if !f.IsActive() {
			w.ofs[i] = -1
			continue
		}
		sz := f.WireSize()
		w.minWireSize += sz
		if !f.IsFixedSize() && w.firstDyn < 0 {
			w.firstDyn = i
		}
		w.ofs[i] = ofs
		w.len[i] = sz
		ofs += sz
	}
	w.buf = make([]byte, w.minWireSize)
	return w
}

func (w *Writer) Reset() {
	if w.dyn != nil {
		clear(w.dyn)
	}
	clear(w.buf)
}

func (w *Writer) Write(i int, val any) error {
	// sanity checks
	if val == nil {
		return ErrNilValue
	}
	if i < 0 || i >= len(w.ofs) {
		return ErrInvalidField
	}

	// init write offset
	x, y := w.ofs[i], w.ofs[i]+w.len[i]
	field := w.schema.Fields[i]

	// skip hidden fields
	if x < 0 {
		return nil
	}

	var err error
	switch field.Type {
	case FT_U64:
		if u64, ok := val.(uint64); ok {
			w.layout.PutUint64(w.buf[x:y], u64)
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
			if len(buf) == int(field.Fixed) {
				copy(w.buf[x:y], buf)
			} else {
				err = ErrShortValue
			}
		} else {
			// variable size
			// w.dyn[i] = bytes.Clone(buf)
			w.dyn[i] = buf
		}

	case FT_TIMESTAMP:
		switch tm := val.(type) {
		case time.Time:
			w.layout.PutUint64(w.buf[x:y], uint64(TimeScale(field.Scale).ToUnix(tm)))
		case int64:
			w.layout.PutUint64(w.buf[x:y], uint64(tm))
		default:
			err = ErrInvalidValueType
		}

	case FT_I64:
		if i64, ok := val.(int64); ok {
			w.layout.PutUint64(w.buf[x:y], uint64(i64))
		} else {
			err = ErrInvalidValueType
		}

	case FT_F64:
		if f64, ok := val.(float64); ok {
			w.layout.PutUint64(w.buf[x:y], math.Float64bits(f64))
		} else {
			err = ErrInvalidValueType
		}

	case FT_F32:
		if f32, ok := val.(float32); ok {
			w.layout.PutUint32(w.buf[x:y], math.Float32bits(f32))
		} else {
			err = ErrInvalidValueType
		}

	case FT_BOOL:
		if v, ok := val.(bool); ok {
			if v {
				w.buf[x] = 1
			} else {
				w.buf[x] = 0
			}
		} else {
			err = ErrInvalidValueType
		}

	case FT_I32:
		if i32, ok := val.(int32); ok {
			w.layout.PutUint32(w.buf[x:y], uint32(i32))
		} else {
			err = ErrInvalidValueType
		}

	case FT_I16:
		if i16, ok := val.(int16); ok {
			w.layout.PutUint16(w.buf[x:y], uint16(i16))
		} else {
			err = ErrInvalidValueType
		}

	case FT_I8:
		if i8, ok := val.(int8); ok {
			w.buf[x] = uint8(i8)
		} else {
			err = ErrInvalidValueType
		}

	case FT_U32:
		if u32, ok := val.(uint32); ok {
			w.layout.PutUint32(w.buf[x:y], u32)
		} else {
			err = ErrInvalidValueType
		}

	case FT_U16:
		if u16, ok := val.(uint16); ok {
			w.layout.PutUint16(w.buf[x:y], u16)
		} else {
			err = ErrInvalidValueType
		}

	case FT_U8:
		if u8, ok := val.(uint8); ok {
			w.buf[x] = u8
		} else {
			err = ErrInvalidValueType
		}

	case FT_I256:
		if i256, ok := val.(num.Int256); ok {
			copy(w.buf[x:y], i256.Bytes())
		} else {
			err = ErrInvalidValueType
		}

	case FT_I128:
		if i128, ok := val.(num.Int128); ok {
			copy(w.buf[x:y], i128.Bytes())
		} else {
			err = ErrInvalidValueType
		}

	case FT_D256:
		switch v := val.(type) {
		case num.Decimal256:
			copy(w.buf[x:y], v.Int256().Bytes())
		case num.Int256:
			copy(w.buf[x:y], v.Bytes())
		default:
			err = ErrInvalidValueType
		}

	case FT_D128:
		switch v := val.(type) {
		case num.Decimal128:
			copy(w.buf[x:y], v.Int128().Bytes())
		case num.Int128:
			copy(w.buf[x:y], v.Bytes())
		default:
			err = ErrInvalidValueType
		}

	case FT_D64:
		switch v := val.(type) {
		case num.Decimal64:
			w.layout.PutUint64(w.buf[x:y], uint64(v.Int64()))
		case int64:
			w.layout.PutUint64(w.buf[x:y], uint64(v))
		default:
			err = ErrInvalidValueType
		}

	case FT_D32:
		switch v := val.(type) {
		case num.Decimal32:
			w.layout.PutUint32(w.buf[x:y], uint32(v.Int64()))
		case int64:
			w.layout.PutUint32(w.buf[x:y], uint32(v))
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
		case []byte:
			buf = v
		default:
			err = ErrInvalidValueType
		}
		// variable size (already in new allocated []byte)
		// b.dyn[i] = bytes.Clone(buf)
		w.dyn[i] = buf

	default:
		err = ErrInvalidField
	}

	return err
}

func (w *Writer) Bytes() []byte {
	// fast path for fixed length schemas
	if w.dyn == nil {
		return bytes.Clone(w.buf)
	}

	// count variable len bytes
	var sz int
	for _, v := range w.dyn {
		sz += len(v)
	}

	// alloc output buffer
	buf := bytes.NewBuffer(make([]byte, 0, w.minWireSize+sz))

	// write data up to first variable length field
	n := w.firstDyn
	buf.Write(w.buf[:w.ofs[n]])

	// write remaining fields one by one
	for _, f := range w.schema.Fields[n:] {
		// skip hidden fields
		if w.ofs[n] < 0 {
			n++
			continue
		}

		// write next visible field
		if f.IsFixedSize() {
			// write fixed size field
			buf.Write(w.buf[w.ofs[n] : w.ofs[n]+w.len[n]])
		} else {
			// write dynamic field
			val := w.dyn[n]
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
