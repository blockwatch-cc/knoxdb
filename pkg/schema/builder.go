// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package schema

import (
	"bytes"
	"math"
	"slices"
	"time"

	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/num"
	"blockwatch.cc/knoxdb/pkg/util"
)

// Constructs wire encoded messages from typed values.
type Builder struct {
	schema      *Schema        // target schema
	buf         []byte         // wire size encoded message buffer (min/fixed sizes)
	ofs         []int          // field offsets in wire encoding (as if all fields were fixed)
	len         []int          // field lengths in wire encoding
	dyn         map[int][]byte // data for variable length fields
	internal    bool           // flag indicating to output internal fields
	firstDyn    int            // position of first dynamic field (optimization)
	minWireSize int            // min size with or without internal fields
}

func NewBuilder(s *Schema) *Builder {
	b := &Builder{}
	return b.initFromSchema(s)
}

func NewInternalBuilder(s *Schema) *Builder {
	b := &Builder{
		internal: true,
	}
	return b.initFromSchema(s)
}

func (b Builder) WireSize() int {
	return b.minWireSize
}

func (b *Builder) initFromSchema(s *Schema) *Builder {
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

func (b *Builder) Reset() {
	if b.dyn != nil {
		clear(b.dyn)
	}
	clear(b.buf)
}

func (b *Builder) Write(i int, val any) error {
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
	case types.FieldTypeUint64:
		if u64, ok := val.(uint64); ok {
			LE.PutUint64(b.buf[x:y], u64)
		} else {
			err = ErrInvalidValueType
		}

	case types.FieldTypeString, types.FieldTypeBytes:
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
			b.dyn[i] = slices.Clone(buf)
		}

	case types.FieldTypeDatetime:
		switch tm := val.(type) {
		case time.Time:
			LE.PutUint64(b.buf[x:y], uint64(tm.UnixNano()))
		case int64:
			LE.PutUint64(b.buf[x:y], uint64(tm))
		default:
			err = ErrInvalidValueType
		}

	case types.FieldTypeInt64:
		if i64, ok := val.(int64); ok {
			LE.PutUint64(b.buf[x:y], uint64(i64))
		} else {
			err = ErrInvalidValueType
		}

	case types.FieldTypeFloat64:
		if f64, ok := val.(float64); ok {
			LE.PutUint64(b.buf[x:y], math.Float64bits(f64))
		} else {
			err = ErrInvalidValueType
		}

	case types.FieldTypeFloat32:
		if f32, ok := val.(float32); ok {
			LE.PutUint32(b.buf[x:y], math.Float32bits(f32))
		} else {
			err = ErrInvalidValueType
		}

	case types.FieldTypeBoolean:
		if v, ok := val.(bool); ok {
			if v {
				b.buf[x] = 1
			} else {
				b.buf[x] = 0
			}
		} else {
			err = ErrInvalidValueType
		}

	case types.FieldTypeInt32:
		if i32, ok := val.(int32); ok {
			LE.PutUint32(b.buf[x:y], uint32(i32))
		} else {
			err = ErrInvalidValueType
		}

	case types.FieldTypeInt16:
		if i16, ok := val.(int16); ok {
			LE.PutUint16(b.buf[x:y], uint16(i16))
		} else {
			err = ErrInvalidValueType
		}

	case types.FieldTypeInt8:
		if i8, ok := val.(int8); ok {
			b.buf[x] = uint8(i8)
		} else {
			err = ErrInvalidValueType
		}

	case types.FieldTypeUint32:
		if u32, ok := val.(uint32); ok {
			LE.PutUint32(b.buf[x:y], u32)
		} else {
			err = ErrInvalidValueType
		}

	case types.FieldTypeUint16:
		if u16, ok := val.(uint16); ok {
			LE.PutUint16(b.buf[x:y], u16)
		} else {
			err = ErrInvalidValueType
		}

	case types.FieldTypeUint8:
		if u8, ok := val.(uint8); ok {
			b.buf[x] = u8
		} else {
			err = ErrInvalidValueType
		}

	case types.FieldTypeInt256:
		if i256, ok := val.(num.Int256); ok {
			copy(b.buf[x:y], i256.Bytes())
		} else {
			err = ErrInvalidValueType
		}

	case types.FieldTypeInt128:
		if i128, ok := val.(num.Int128); ok {
			copy(b.buf[x:y], i128.Bytes())
		} else {
			err = ErrInvalidValueType
		}

	case types.FieldTypeDecimal256:
		switch v := val.(type) {
		case num.Decimal256:
			copy(b.buf[x:y], v.Int256().Bytes())
		case num.Int256:
			copy(b.buf[x:y], v.Bytes())
		default:
			err = ErrInvalidValueType
		}

	case types.FieldTypeDecimal128:
		switch v := val.(type) {
		case num.Decimal128:
			copy(b.buf[x:y], v.Int128().Bytes())
		case num.Int128:
			copy(b.buf[x:y], v.Bytes())
		default:
			err = ErrInvalidValueType
		}

	case types.FieldTypeDecimal64:
		switch v := val.(type) {
		case num.Decimal64:
			LE.PutUint64(b.buf[x:y], uint64(v.Int64()))
		case int64:
			LE.PutUint64(b.buf[x:y], uint64(v))
		default:
			err = ErrInvalidValueType
		}

	case types.FieldTypeDecimal32:
		switch v := val.(type) {
		case num.Decimal32:
			LE.PutUint32(b.buf[x:y], uint32(v.Int64()))
		case int64:
			LE.PutUint32(b.buf[x:y], uint32(v))
		default:
			err = ErrInvalidValueType
		}

	default:
		err = ErrInvalidField
	}

	return err
}

func (b *Builder) Bytes() []byte {
	// fast path for fixed length schemas
	if b.dyn == nil {
		return slices.Clone(b.buf)
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
			buf.Write(Uint32Bytes(uint32(len(val))))
			buf.Write(val)
		}
		n++
	}

	// return buffer contents
	return buf.Bytes()
}
