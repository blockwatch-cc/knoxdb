// Copyright (c) 2024-2026 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package encode

import (
	"bytes"
	"fmt"
	"iter"
	"reflect"
	"sync"
	"time"
	"unsafe"

	"blockwatch.cc/knoxdb/pkg/num"
	"blockwatch.cc/knoxdb/pkg/schema"
	"blockwatch.cc/knoxdb/pkg/schema/enum"
	sreflect "blockwatch.cc/knoxdb/pkg/schema/reflect"
)

type DecoderT[T any] struct {
	*Decoder
}

func NewDecoderFor[T any](opts ...schema.Option) *DecoderT[T] {
	s, err := sreflect.SchemaFor[T](opts...)
	if err != nil {
		panic(err)
	}
	l, err := sreflect.LayoutFor[T]()
	if err != nil {
		panic(err)
	}
	return &DecoderT[T]{
		NewDecoderWithLayout(s, l),
	}
}

func (d *DecoderT[T]) Decode(buf []byte, val *T) (*T, error) {
	if val == nil {
		val = new(T)
	}
	if err := d.Decoder.Decode(buf, val); err != nil {
		return nil, err
	}
	return val, nil
}

func (d *DecoderT[T]) DecodeBatch(buf []byte, dst []T) ([]T, error) {
	var n int
	if d.schema.IsFixedSize {
		n = len(buf) / d.schema.MinWireSize
	} else {
		if d.view == nil {
			d.view = schema.NewView(d.schema)
		}
		n = d.view.Count(buf)
	}
	if cap(dst) < n {
		dst = make([]T, n)
	}
	dst = dst[:n]

	// decode
	n, err := d.Decoder.DecodeBatch(buf, dst)
	if err != nil {
		return nil, err
	}
	return dst[:n], nil
}

func (d *DecoderT[T]) DecodeBatchSeq(buf []byte, dst *T) iter.Seq2[*T, error] {
	if dst == nil {
		dst = new(T)
	}
	base := unsafe.Pointer(dst)
	return func(yield func(*T, error) bool) {
		var err error
		for len(buf) > 0 {
			buf, err = d.decodePtr(buf, base)
			if !yield(dst, err) {
				return
			}
			if err != nil {
				return
			}
		}
	}
}

var decoderPool = sync.Pool{}

type Decoder struct {
	schema  *schema.Schema
	view    *schema.View
	layout  *sreflect.Layout
	buf     *bytes.Buffer
	opcodes []OpCode
	nested  map[uint32]*Decoder
}

func NewDecoder(s *schema.Schema) *Decoder {
	return NewDecoderWithLayout(s, nil)
}

func NewDecoderWithLayout(s *schema.Schema, l *sreflect.Layout) *Decoder {
	var dec *Decoder
	if dif := decoderPool.Get(); dif != nil {
		dec = dif.(*Decoder)
	} else {
		dec = &Decoder{}
	}
	dec.schema = s
	dec.layout = l
	dec.opcodes = CompileCodecs(s)
	if l != nil && len(l.Children) > 0 && dec.nested == nil {
		dec.nested = make(map[uint32]*Decoder)
	}
	return dec
}

func (d *Decoder) initLayout(val any) error {
	if d.layout != nil {
		return nil
	}
	l, err := sreflect.LayoutOf(val, d.schema)
	if err != nil {
		return err
	}
	d.layout = l
	if len(l.Children) > 0 && d.nested == nil {
		d.nested = make(map[uint32]*Decoder)
	}
	return nil
}

func (d *Decoder) Close() {
	d.schema = nil
	d.view = nil
	d.layout = nil
	d.buf = nil
	d.opcodes = nil
	for _, v := range d.nested {
		v.Close()
	}
	clear(d.nested)
	decoderPool.Put(d)
}

func (d *Decoder) Schema() *schema.Schema {
	return d.schema
}

// Decode decodes a single record into the pointer passed by val
// or an error if val is not a pointer or val's type is incompatible
// with the decoder schema.
func (d *Decoder) Decode(buf []byte, val any) error {
	if val == nil {
		return schema.ErrNilValue
	}

	// redirect to unmarshaler when implemented
	if m, ok := val.(schema.Unmarshaler); ok {
		if d.view == nil {
			d.view = schema.NewView(d.schema)
		}
		return m.UnmarshalSchema(d.view.Reset(buf))
	}

	// ensure Go type layout is resolved
	if err := d.initLayout(val); err != nil {
		return err
	}

	rval := reflect.ValueOf(val)

	// ensure the type actually matches our layout
	if rval.Elem().Type() != d.layout.Type {
		return fmt.Errorf("decode: type mismatch: expected %s, have %s", d.layout.Type, rval.Type())
	}

	// decode single object
	_, err := d.decodePtr(buf, rval.UnsafePointer())
	return err
}

// DecodeBatch decodes up to len(dst) records into dst and returns
// the number of successful records or an error if dst's type is
// incompatible with the decoder schema.
func (d *Decoder) DecodeBatch(buf []byte, dst any) (int, error) {
	if dst == nil {
		return 0, schema.ErrNilValue
	}
	rslice := reflect.Indirect(reflect.ValueOf(dst))

	// ensure Go type layout is resolved
	if err := d.initLayout(dst); err != nil {
		return 0, err
	}

	// redirect to marshaler if implemented by elem type
	if d.layout.Unmarshaler != nil {
		return d.unmarshalSlice(rslice.UnsafePointer(), rslice.Len(), buf)
	}

	// ensure the type actually matches our layout
	etyp := rslice.Type().Elem()
	if etyp != d.layout.Type {
		return 0, fmt.Errorf("decode: type mismatch: expected %s, have %s", d.layout.Type, etyp)
	}

	// process slice elements
	return d.decodeSlice(rslice.UnsafePointer(), rslice.Len(), buf)
}

func (d *Decoder) decodePtr(buf []byte, base unsafe.Pointer) ([]byte, error) {
	var err error
	for op, code := range d.opcodes {
		if code == OC_SKIP {
			continue
		}
		ptr := unsafe.Add(base, d.layout.Offsets[op])
		buf, err = d.readField(code, d.schema.Fields[op], ptr, buf)
		if err != nil {
			return nil, fmt.Errorf("field[%s]: %w", d.schema.Fields[op].Name, err)
		}
	}
	return buf, nil
}

func (d *Decoder) decodeSlice(base unsafe.Pointer, baseLen int, buf []byte) (int, error) {
	var (
		n   int
		err error
	)
	for len(buf) > 0 && n < baseLen {
		// decoePtr code intentionally duplicated because it does not
		// get inlined
		for op, code := range d.opcodes {
			if code == OC_SKIP {
				continue
			}
			ptr := unsafe.Add(base, d.layout.Offsets[op])
			buf, err = d.readField(code, d.schema.Fields[op], ptr, buf)
			if err != nil {
				return n, fmt.Errorf("field[%s]: %w", d.schema.Fields[op].Name, err)
			}
		}
		n++
		base = unsafe.Add(base, d.layout.Size)
	}
	return n, nil
}

func (d *Decoder) unmarshalSlice(base unsafe.Pointer, baseLen int, buf []byte) (int, error) {
	var (
		n    int
		mtyp schema.Unmarshaler
	)
	if d.view == nil {
		d.view = schema.NewView(d.schema)
	}
	for _, view := range d.view.All(buf) {
		*(*iface)(unsafe.Pointer(&mtyp)) = iface{
			itab: d.layout.Unmarshaler, // T's interface table
			data: base,                 // ptr to each []T elements
		}
		if err := mtyp.UnmarshalSchema(view); err != nil {
			return n, fmt.Errorf("%s: %w", d.layout.Type.Name(), err)
		}
		base = unsafe.Add(base, d.layout.Size) // add struct size
		n++
		if n == baseLen {
			break
		}
	}
	return n, nil
}

// reads data for a field in native machine byte order layout
func (d *Decoder) readField(code OpCode, field *schema.Field, ptr unsafe.Pointer, buf []byte) ([]byte, error) {
	switch code {
	case OC_I64, OC_U64, OC_F64:
		_ = buf[7]
		*(*uint64)(ptr) = *(*uint64)(unsafe.Pointer(&buf[0]))
		buf = buf[8:]

	case OC_I32, OC_U32, OC_F32:
		_ = buf[3]
		*(*uint32)(ptr) = *(*uint32)(unsafe.Pointer(&buf[0]))
		buf = buf[4:]

	case OC_I16, OC_U16:
		_ = buf[1]
		*(*uint16)(ptr) = *(*uint16)(unsafe.Pointer(&buf[0]))
		buf = buf[2:]

	case OC_I8, OC_U8, OC_BOOL:
		_ = buf[0]
		*(*uint8)(ptr) = *(*uint8)(unsafe.Pointer(&buf[0]))
		buf = buf[1:]

	case OC_FIXBYTES:
		_ = buf[field.Scale-1]
		copy(unsafe.Slice((*byte)(ptr), field.Scale), buf[:field.Scale])
		buf = buf[field.Scale:]

	case OC_FIXSTRING:
		_ = buf[field.Scale-1]
		*(*string)(ptr) = unsafe.String(unsafe.SliceData(buf), field.Scale)
		buf = buf[field.Scale:]

	case OC_STRING:
		l := int(buf[0])
		buf = buf[1:]
		if l > 0 {
			_ = buf[l-1]
			*(*string)(ptr) = unsafe.String(unsafe.SliceData(buf), l)
			buf = buf[l:]
		}

	case OC_BYTES:
		l := int(buf[0])
		buf = buf[1:]
		if l > 0 {
			_ = buf[l-1]
			*(*[]byte)(ptr) = buf[:l]
			buf = buf[l:]
		}

	case OC_TEXT:
		l := *(*uint32)(unsafe.Pointer(&buf[0]))
		buf = buf[4:]
		if l > 0 {
			_ = buf[l-1]
			*(*string)(ptr) = unsafe.String(unsafe.SliceData(buf), l)
			buf = buf[l:]
		}

	case OC_BLOB:
		l := *(*uint32)(unsafe.Pointer(&buf[0]))
		buf = buf[4:]
		if l > 0 {
			_ = buf[l-1]
			*(*[]byte)(ptr) = buf[:l]
			buf = buf[l:]
		}

	case OC_TIMESTAMP, OC_TIME, OC_DATE:
		_ = buf[7]
		*(*time.Time)(ptr) = schema.TimeScale(field.Scale).
			FromUnix(*(*int64)(unsafe.Pointer(&buf[0])))
		buf = buf[8:]

	case OC_DURATION:
		_ = buf[7]
		*(*time.Duration)(ptr) = schema.TimeScale(field.Scale).
			Duration(*(*int64)(unsafe.Pointer(&buf[0])))
		buf = buf[8:]

	case OC_I128:
		_ = buf[15]
		*(*num.Int128)(ptr) = num.Int128FromBytes(buf[:16])
		buf = buf[16:]

	case OC_I256:
		_ = buf[31]
		*(*num.Int256)(ptr) = num.Int256FromBytes(buf[:32])
		buf = buf[32:]

	case OC_D32:
		_ = buf[3]
		(*(*num.Decimal32)(ptr)).Set(*(*int32)(unsafe.Pointer(&buf[0])))
		(*(*num.Decimal32)(ptr)).SetScale(field.Scale)
		buf = buf[4:]

	case OC_D64:
		_ = buf[7]
		(*(*num.Decimal64)(ptr)).Set(*(*int64)(unsafe.Pointer(&buf[0])))
		(*(*num.Decimal64)(ptr)).SetScale(field.Scale)
		buf = buf[8:]

	case OC_D128:
		_ = buf[15]
		(*(*num.Decimal128)(ptr)).Set(num.Int128FromBytes(buf[:16]))
		(*(*num.Decimal128)(ptr)).SetScale(field.Scale)
		buf = buf[16:]

	case OC_D256:
		_ = buf[31]
		(*(*num.Decimal256)(ptr)).Set(num.Int256FromBytes(buf[:32]))
		(*(*num.Decimal256)(ptr)).SetScale(field.Scale)
		buf = buf[32:]

	case OC_ENUM:
		_ = buf[1]
		u16 := *(*uint16)(unsafe.Pointer(&buf[0]))
		val, ok := field.Enum.Value(u16)
		buf = buf[2:]
		if !ok {
			return nil, enum.ErrEnumNoCode
		}
		*(*string)(ptr) = val // FIXME: may break when enum dict grows

	case OC_BIGINT:
		l := buf[0]
		buf = buf[1:]
		if l > 0 {
			_ = buf[l-1]
			_ = (*num.Big)(ptr).UnmarshalBinary(buf[:l])
			buf = buf[l:]
		}

	case OC_UNION:
		l := buf[0]
		buf = buf[1:]
		_ = buf[l-1]
		err := (*schema.UnionValue)(ptr).UnmarshalBuffer(buf[:l], schema.LE)
		buf = buf[l:]
		if err != nil {
			return nil, err
		}

	case OC_LIST:
		_ = buf[3]
		l := *(*uint32)(unsafe.Pointer(&buf[0]))
		buf = buf[4:]
		if l > 0 {
			_ = buf[l-1]

			// use sub-decoder for schema
			sub, ok := d.nested[uint32(field.Id)]
			if !ok {
				layout := d.layout.Children[uint32(field.Id)]
				sub = NewDecoderWithLayout(field.Child, layout)
				d.nested[uint32(field.Id)] = sub
			}

			// pre-allocate max elements
			var n int
			if field.Child.IsFixedSize {
				n = int(l) / field.Child.MinWireSize
			} else {
				if sub.view == nil {
					sub.view = schema.NewView(sub.schema)
				}
				n = sub.view.Count(buf[:l])
			}

			// map pointer back to slice type
			slice := (*sliceType)(ptr)
			if slice.Data == nil || slice.Cap < n {
				// alloc space for new slice via reflect to avoid GC issues
				rspace := reflect.MakeSlice(reflect.SliceOf(sub.layout.Type), n, n)
				slice.Data = rspace.UnsafePointer()
				slice.Len = n
				slice.Cap = n
			} else {
				// reuse existing slice
				slice.Len = n
			}

			// call decoder
			var err error
			if sub.layout.Unmarshaler != nil {
				_, err = sub.unmarshalSlice(slice.Data, n, buf[:l])
			} else {
				_, err = sub.decodeSlice(slice.Data, n, buf[:l])
			}
			if err != nil {
				return nil, err
			}

			buf = buf[l:]
		}
	case OC_INVALID, OC_MAP, OC_VARIANT:
		return nil, schema.ErrInvalidValueType
	}
	return buf, nil
}
