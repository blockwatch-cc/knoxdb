// Copyright (c) 2024-2026 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package encode

import (
	"bytes"
	"fmt"
	"reflect"
	"sync"
	"time"
	"unsafe"

	"blockwatch.cc/knoxdb/pkg/num"
	"blockwatch.cc/knoxdb/pkg/schema"
	sreflect "blockwatch.cc/knoxdb/pkg/schema/reflect"
)

type DecoderT[T any] struct {
	dec *Decoder
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
		dec: NewDecoderWithLayout(s, l),
	}
}

func (d *DecoderT[T]) Schema() *schema.Schema {
	return d.dec.schema
}

func (d *DecoderT[T]) Decode(buf []byte, val *T) (*T, error) {
	if val == nil {
		val = new(T)
	}
	d.dec.decodePtr(buf, unsafe.Pointer(val))
	return val, nil
}

func (d *DecoderT[T]) DecodeSlice(buf []byte, res []T) ([]T, error) {
	if res == nil {
		// pre-allocate space for the number of encoded elements
		var n int
		if d.dec.schema.IsFixedSize {
			n = len(buf) / d.dec.schema.MinWireSize
		} else {
			if d.dec.view == nil {
				d.dec.view = schema.NewView(d.dec.schema)
			}
			n = d.dec.view.Count(buf)
		}
		res = make([]T, n)
	}
	var n int
	for n = range res {
		if len(buf) == 0 {
			break
		}
		buf = d.dec.decodePtr(buf, unsafe.Pointer(&res[n]))
	}
	return res[:n], nil
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

func (d *Decoder) Decode(buf []byte, val any) error {
	if val == nil {
		return schema.ErrNilValue
	}
	// ensure Go type layout is resolved
	if err := d.initLayout(val); err != nil {
		return err
	}

	rval := reflect.Indirect(reflect.ValueOf(val))

	// ensure the type actually matches our layout
	if rval.Type() != d.layout.Type {
		return fmt.Errorf("decode: type mismatch: expected %s, have %s", d.layout.Type, rval.Type())
	}

	d.decodePtr(buf, rval.Addr().UnsafePointer())
	return nil
}

func (d *Decoder) decodePtr(buf []byte, base unsafe.Pointer) []byte {
	for op, code := range d.opcodes {
		if code == OC_SKIP {
			continue
		}
		field := d.schema.Fields[op]
		ptr := unsafe.Add(base, d.layout.Offsets[op])
		buf = d.readField(code, field, ptr, buf)
	}
	return buf
}

func (d *Decoder) DecodeSlice(buf []byte, slice any) (int, error) {
	if slice == nil {
		return 0, schema.ErrNilValue
	}
	rslice := reflect.Indirect(reflect.ValueOf(slice))

	// ensure Go type layout is resolved
	if err := d.initLayout(slice); err != nil {
		return 0, err
	}

	// ensure the type actually matches our layout
	etyp := rslice.Type().Elem()
	if etyp != d.layout.Type {
		return 0, fmt.Errorf("decode: type mismatch: expected %s, have %s", d.layout.Type, etyp)
	}

	base := rslice.UnsafePointer()
	var n int
	for range rslice.Len() {
		for op, code := range d.opcodes {
			if code == OC_SKIP {
				continue
			}
			ptr := unsafe.Add(base, d.layout.Offsets[op])
			buf = d.readField(code, d.schema.Fields[op], ptr, buf)
		}
		base = unsafe.Add(base, d.layout.Size)
		n++
		if len(buf) == 0 {
			break
		}
	}
	return n, nil
}

func (d *Decoder) decodeNestedSlice(base unsafe.Pointer, baseLen int, buf []byte) {
	for range baseLen {
		for op, code := range d.opcodes {
			if code == OC_SKIP {
				continue
			}
			ptr := unsafe.Add(base, d.layout.Offsets[op])
			buf = d.readField(code, d.schema.Fields[op], ptr, buf)
		}
		base = unsafe.Add(base, d.layout.Size)
	}
}

// reads data for a field in native machine byte order layout
func (d *Decoder) readField(code OpCode, field *schema.Field, ptr unsafe.Pointer, buf []byte) []byte {
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
		*(*time.Time)(ptr) = schema.TimeScale(field.Scale).
			FromUnix(*(*int64)(unsafe.Pointer(&buf[0])))
		buf = buf[8:]

	case OC_I128:
		*(*num.Int128)(ptr) = num.Int128FromBytes(buf[:16])
		buf = buf[16:]

	case OC_I256:
		*(*num.Int256)(ptr) = num.Int256FromBytes(buf[:32])
		buf = buf[32:]

	case OC_D32:
		(*(*num.Decimal32)(ptr)).Set(*(*int32)(unsafe.Pointer(&buf[0])))
		(*(*num.Decimal32)(ptr)).SetScale(field.Scale)
		buf = buf[4:]

	case OC_D64:
		(*(*num.Decimal64)(ptr)).Set(*(*int64)(unsafe.Pointer(&buf[0])))
		(*(*num.Decimal64)(ptr)).SetScale(field.Scale)
		buf = buf[8:]

	case OC_D128:
		(*(*num.Decimal128)(ptr)).Set(num.Int128FromBytes(buf[:16]))
		(*(*num.Decimal128)(ptr)).SetScale(field.Scale)
		buf = buf[16:]

	case OC_D256:
		(*(*num.Decimal256)(ptr)).Set(num.Int256FromBytes(buf[:32]))
		(*(*num.Decimal256)(ptr)).SetScale(field.Scale)
		buf = buf[32:]

	case OC_ENUM:
		u16 := *(*uint16)(unsafe.Pointer(&buf[0]))
		buf = buf[2:]
		val, ok := field.Enum.Value(u16)
		if !ok {
			panic(fmt.Errorf("field[%s]: invalid enum value %d, have %#v", field.Name, u16, field.Enum))
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

	case OC_LIST:
		l := *(*uint32)(unsafe.Pointer(&buf[0]))
		buf = buf[4:]
		if l > 0 {
			// use sub-encoder for schema
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
				layout := d.layout.Children[uint32(field.Id)]
				rspace := reflect.MakeSlice(reflect.SliceOf(layout.Type), n, n)
				slice.Data = rspace.UnsafePointer()
				slice.Len = n
				slice.Cap = n
			} else {
				// reuse existing slice
				slice.Len = n
			}

			// call decoder
			sub.decodeNestedSlice(slice.Data, n, buf[:l])

			buf = buf[l:]
		}
	}
	return buf
}
