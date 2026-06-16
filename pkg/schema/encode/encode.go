// Copyright (c) 2024-2026 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package encode

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"reflect"
	"sync"
	"time"
	"unsafe"

	"blockwatch.cc/knoxdb/pkg/num"
	"blockwatch.cc/knoxdb/pkg/schema"
	"blockwatch.cc/knoxdb/pkg/schema/enum"
	sreflect "blockwatch.cc/knoxdb/pkg/schema/reflect"
)

type EncoderT[T any] struct {
	*Encoder
}

func NewEncoderFor[T any](opts ...schema.Option) *EncoderT[T] {
	s, err := sreflect.SchemaFor[T](opts...)
	if err != nil {
		panic(err)
	}
	l, err := sreflect.LayoutFor[T]()
	if err != nil {
		panic(err)
	}
	return &EncoderT[T]{
		NewEncoderWithLayout(s, l),
	}
}

func (e *EncoderT[T]) Encode(val any, buf *bytes.Buffer) ([]byte, error) {
	if val == nil {
		return nil, schema.ErrNilValue
	}
	switch v := val.(type) {
	case *T:
		return e.Encoder.Encode(val, buf)
	case []*T:
		return e.encodePtrBatch(val, buf)
	case []T:
		return e.EncodeBatch(val, buf)
	case T:
		return e.Encoder.Encode(&v, buf)
	default:
		return nil, schema.ErrInvalidValueType
	}
}

func (e *EncoderT[T]) Close() {
	e.Encoder.Close()
	e.Encoder = nil
}

var encoderPool = sync.Pool{}

type Encoder struct {
	schema  *schema.Schema
	layout  *sreflect.Layout
	buf     *bytes.Buffer
	opcodes []OpCode
	nested  map[uint32]*Encoder
}

func NewEncoder(s *schema.Schema) *Encoder {
	return NewEncoderWithLayout(s, nil)
}

func NewEncoderWithLayout(s *schema.Schema, l *sreflect.Layout) *Encoder {
	var enc *Encoder
	if eif := encoderPool.Get(); eif != nil {
		enc = eif.(*Encoder)
	} else {
		enc = &Encoder{}
	}
	enc.schema = s
	enc.layout = l
	enc.opcodes = CompileCodecs(s)
	if l != nil && len(l.Children) > 0 && enc.nested == nil {
		enc.nested = make(map[uint32]*Encoder)
	}
	return enc
}

func (e *Encoder) initLayout(val any) error {
	if e.layout != nil {
		return nil
	}
	l, err := sreflect.LayoutOf(val, e.schema)
	if err != nil {
		return err
	}
	e.layout = l
	if len(l.Children) > 0 && e.nested == nil {
		e.nested = make(map[uint32]*Encoder)
	}
	return nil
}

func (e *Encoder) Close() {
	e.schema = nil
	e.layout = nil
	e.opcodes = nil
	for _, v := range e.nested {
		v.Close()
	}
	clear(e.nested)
	encoderPool.Put(e)
}

func (e *Encoder) Schema() *schema.Schema {
	return e.schema
}

func (e *Encoder) Offset(i int) uintptr {
	return e.layout.Offsets[i]
}

func (e *Encoder) NewBuffer(sz int) *bytes.Buffer {
	return e.schema.NewBuffer(sz)
}

func (e *Encoder) Encode(val any, buf *bytes.Buffer) ([]byte, error) {
	// redirect to marshaler when implemented
	if m, ok := val.(schema.Marshaler); ok {
		// ensure we have a target buffer
		if buf == nil {
			buf = e.useBuffer(1)
		}

		// use temp writer
		w := schema.NewWriter(e.schema, buf)
		defer w.Close()
		if err := m.MarshalSchema(w); err != nil {
			return nil, err
		}
		w.Next()
		return buf.Bytes(), nil
	}

	// validate
	rval := reflect.ValueOf(val)
	if rval.Kind() == reflect.Slice {
		return e.EncodeBatch(val, buf)
	}
	if rval.Kind() != reflect.Pointer {
		return nil, fmt.Errorf("encode: expected pointer type, have %s", rval.Type())
	}

	// ensure Go type layout is resolved
	if err := e.initLayout(val); err != nil {
		return nil, err
	}

	// ensure the type actually matches our layout
	if rval.Elem().Type() != e.layout.Type {
		return nil, fmt.Errorf("encode: type mismatch: expected %s, have %s", e.layout.Type, rval.Type())
	}

	// ensure we have a target buffer
	if buf == nil {
		buf = e.useBuffer(1)
	}

	// process opcodes
	if err := e.encodeSlice(rval.UnsafePointer(), 1, buf); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (e *Encoder) EncodeBatch(src any, buf *bytes.Buffer) ([]byte, error) {
	// validate value
	if src == nil {
		return nil, schema.ErrNilValue
	}
	rslice := reflect.Indirect(reflect.ValueOf(src))
	if !rslice.IsValid() || rslice.Kind() != reflect.Slice {
		return nil, schema.ErrInvalidValueType
	}

	// redirect
	etyp := rslice.Type().Elem()
	if etyp.Kind() == reflect.Pointer {
		return e.encodePtrBatch(src, buf)
	}

	// return nil when slice is empty
	if rslice.Len() == 0 {
		return nil, nil
	}

	// ensure Go type layout is resolved
	if err := e.initLayout(src); err != nil {
		return nil, err
	}

	// ensure target buffer is allocated
	if buf == nil {
		buf = e.useBuffer(rslice.Len())
	}

	// redirect to marshaler if implemented by elem type
	if e.layout.Marshaler != nil {
		err := e.marshalSlice(rslice.UnsafePointer(), rslice.Len(), buf)
		if err != nil {
			return nil, err
		}
		return buf.Bytes(), nil
	}

	// ensure the type actually matches our layout
	if etyp != e.layout.Type {
		return nil, fmt.Errorf("encode: type mismatch: expected %s, have %s", e.layout.Type, etyp)
	}

	// process slice elements
	if err := e.encodeSlice(rslice.UnsafePointer(), rslice.Len(), buf); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (e *Encoder) encodePtrBatch(src any, buf *bytes.Buffer) ([]byte, error) {
	// validate
	if src == nil {
		return nil, schema.ErrNilValue
	}
	rslice := reflect.Indirect(reflect.ValueOf(src))
	if !rslice.IsValid() ||
		rslice.Kind() != reflect.Slice ||
		rslice.Type().Elem().Kind() != reflect.Pointer {
		return nil, schema.ErrInvalidValueType
	}

	// return nil when slice is empty
	if rslice.Len() == 0 {
		return nil, nil
	}

	// ensure Go type layout is resolved
	if err := e.initLayout(src); err != nil {
		return nil, err
	}

	// ensure target buffer is allocated
	if buf == nil {
		buf = e.useBuffer(rslice.Len())
	}

	// redirect to marshaler if implemented by elem type
	if e.layout.Marshaler != nil {
		err := e.marshalPtrSlice(rslice.UnsafePointer(), rslice.Len(), buf)
		if err != nil {
			return nil, err
		}
		return buf.Bytes(), nil
	}

	// ensure the type actually matches our layout
	if etyp := rslice.Type().Elem().Elem(); etyp != e.layout.Type {
		return nil, fmt.Errorf("encode: type mismatch: expected %s, have %s", e.layout.Type, etyp)
	}

	// process slice elements
	if err := e.encodePtrSlice(rslice.UnsafePointer(), rslice.Len(), buf); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (e *Encoder) useBuffer(n int) *bytes.Buffer {
	if e.buf == nil {
		e.buf = e.NewBuffer(n)
	}
	e.buf.Reset()
	return e.buf
}

func (e *Encoder) marshalSlice(base unsafe.Pointer, l int, buf *bytes.Buffer) error {
	var mtyp schema.Marshaler
	w := schema.NewWriter(e.schema, buf)
	for range l {
		*(*iface)(unsafe.Pointer(&mtyp)) = iface{
			itab: e.layout.Marshaler, // T's interface table
			data: base,               // ptr to each []T elements
		}
		if err := mtyp.MarshalSchema(w); err != nil {
			return fmt.Errorf("%s: %w", e.layout.Type.Name(), err)
		}
		base = unsafe.Add(base, e.layout.Size) // add struct size
		w.Next()
	}
	w.Close()
	return nil
}

func (e *Encoder) marshalPtrSlice(base unsafe.Pointer, l int, buf *bytes.Buffer) error {
	var mtyp schema.Marshaler
	w := schema.NewWriter(e.schema, buf)
	for range l {
		*(*iface)(unsafe.Pointer(&mtyp)) = iface{
			itab: e.layout.Marshaler,       // T's interface table
			data: *(*unsafe.Pointer)(base), // deref []*T elements
		}
		if err := mtyp.MarshalSchema(w); err != nil {
			return fmt.Errorf("%s: %w", e.layout.Type.Name(), err)
		}
		base = unsafe.Add(base, ptrSize) // add ptr size
		w.Next()
	}
	w.Close()
	return nil
}

func (e *Encoder) encodeSlice(base unsafe.Pointer, baseLen int, buf *bytes.Buffer) error {
	// use opcodes to encode fields
	for range baseLen {
		for op, code := range e.opcodes {
			if code == OC_SKIP {
				continue
			}
			ptr := unsafe.Add(base, e.layout.Offsets[op])
			err := e.writeField(buf, code, e.schema.Fields[op], ptr)
			if err != nil {
				return fmt.Errorf("%s: %w", e.schema.Fields[op].Name, err)
			}
		}
		base = unsafe.Add(base, e.layout.Size)
	}
	return nil
}

func (e *Encoder) encodePtrSlice(base unsafe.Pointer, baseLen int, buf *bytes.Buffer) error {
	for range baseLen {
		for op, code := range e.opcodes {
			if code == OC_SKIP {
				continue
			}
			// deref []*T elements
			ptr := unsafe.Add(*(*unsafe.Pointer)(base), e.layout.Offsets[op])
			err := e.writeField(buf, code, e.schema.Fields[op], ptr)
			if err != nil {
				return fmt.Errorf("%s: %w", e.schema.Fields[op].Name, err)
			}
		}
		base = unsafe.Add(base, ptrSize) // add ptr size
	}
	return nil
}

// writes data for a field in native machine byte order layout
func (e *Encoder) writeField(buf *bytes.Buffer, code OpCode, field *schema.Field, ptr unsafe.Pointer) (err error) {
	switch code {
	default:
		// int, uint, float, bool
		buf.Write(unsafe.Slice((*byte)(ptr), field.Type.Size()))

	case OC_FIXBYTES:
		buf.Write(unsafe.Slice((*byte)(ptr), field.Scale))

	case OC_FIXSTRING:
		s := *(*string)(ptr)
		buf.Write(unsafe.Slice(unsafe.StringData(s), field.Scale))

	case OC_STRING:
		// 1 byte len
		s := *(*string)(ptr)
		if len(s) > schema.MAX_STRING {
			return schema.ErrLongValue
		}
		buf.WriteByte(byte(len(s)))
		buf.WriteString(s)

	case OC_BYTES:
		// 1 byte len
		b := *(*[]byte)(ptr)
		if len(b) > schema.MAX_BYTES {
			return schema.ErrLongValue
		}
		buf.WriteByte(byte(len(b)))
		buf.Write(b)

	case OC_TEXT:
		// 4 byte len
		s := *(*string)(ptr)
		writeU32(buf, len(s))
		buf.WriteString(s)

	case OC_BLOB:
		// 4 byte len
		b := *(*[]byte)(ptr)
		writeU32(buf, len(b))
		buf.Write(b)

	case OC_TIMESTAMP, OC_TIME, OC_DATE:
		tm := *(*time.Time)(ptr)
		writeU64(buf, uint64(schema.TimeScale(field.Scale).ToUnix(tm)))

	case OC_DURATION:
		d := *(*time.Duration)(ptr)
		writeU64(buf, uint64(schema.TimeScale(field.Scale).Int64(d)))

	case OC_I256:
		v := *(*num.Int256)(ptr)
		buf.Write(v.Bytes())

	case OC_I128:
		v := *(*num.Int128)(ptr)
		buf.Write(v.Bytes())

	case OC_D32:
		buf.Write(unsafe.Slice((*byte)(ptr), 4))

	case OC_D64:
		buf.Write(unsafe.Slice((*byte)(ptr), 8))

	case OC_D128:
		v := *(*num.Decimal128)(ptr)
		buf.Write(v.Int128().Bytes())

	case OC_D256:
		v := *(*num.Decimal256)(ptr)
		buf.Write(v.Int256().Bytes())

	case OC_ENUM:
		if field.Enum == nil {
			return schema.ErrEnumUndefined
		}
		code, ok := field.Enum.Code(*(*string)(ptr))
		if !ok {
			return enum.ErrEnumNoCode
		}
		writeU16(buf, code)

	case OC_BIGINT:
		// 1 byte len
		v := *(*num.Big)(ptr)
		b := v.Bytes()
		if len(b) > 255 {
			return schema.ErrLongValue
		}
		buf.WriteByte(byte(len(b)))
		buf.Write(b)

	case OC_UNION:
		// 1 byte len
		v := *(*schema.UnionValue)(ptr)
		err = v.MarshalBuffer(buf, binary.LittleEndian)

	case OC_LIST:
		// 4 byte len
		ofs := buf.Len()
		writeU32(buf, 0)

		// use encoder for child schema
		sub, ok := e.nested[uint32(field.Id)]
		if !ok {
			layout := e.layout.Children[uint32(field.Id)]
			sub = NewEncoderWithLayout(field.Child, layout)
			e.nested[uint32(field.Id)] = sub
		}

		// map pointer back to slice type
		slice := (*sliceType)(ptr)

		// check if type implements marshaler
		if sub.layout.Marshaler != nil {
			err = sub.marshalSlice(slice.Data, slice.Len, buf)
		} else {
			err = sub.encodeSlice(slice.Data, slice.Len, buf)
		}

		// patch len in bytes
		*(*uint32)(unsafe.Pointer(&buf.Bytes()[ofs])) = uint32(buf.Len() - ofs - 4)

	case OC_INVALID, OC_MAP, OC_VARIANT:
		err = fmt.Errorf("encode: unsupported value type %s", field.Type)
	}
	return
}
