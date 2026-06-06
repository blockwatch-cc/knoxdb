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
	"blockwatch.cc/knoxdb/pkg/schema/enum"
	sreflect "blockwatch.cc/knoxdb/pkg/schema/reflect"
)

type EncoderT[T any] struct {
	enc *Encoder
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
		enc: NewEncoderWithLayout(s, l),
	}
}

func (e *EncoderT[T]) Schema() *schema.Schema {
	return e.enc.schema
}

func (e *EncoderT[T]) Offset(i int) uintptr {
	return e.enc.Offset(i)
}

func (e *EncoderT[T]) NewBuffer(sz int) *bytes.Buffer {
	return e.enc.schema.NewBuffer(sz)
}

func (e *EncoderT[T]) Encode(val T, buf *bytes.Buffer) ([]byte, error) {
	return e.enc.Encode(&val, buf)
}

func (e *EncoderT[T]) EncodePtr(val *T, buf *bytes.Buffer) ([]byte, error) {
	return e.enc.Encode(val, buf)
}

func (e *EncoderT[T]) EncodeSlice(slice []T, buf *bytes.Buffer) ([]byte, error) {
	return e.enc.EncodeSlice(&slice, buf)
}

func (e *EncoderT[T]) EncodePtrSlice(slice []*T, buf *bytes.Buffer) ([]byte, error) {
	return e.enc.EncodeSlice(&slice, buf)
}

var encoderPool = sync.Pool{}

type Encoder struct {
	schema  *schema.Schema
	layout  *sreflect.Layout
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
	// validate
	rval := reflect.Indirect(reflect.ValueOf(val))
	if rval.Kind() == reflect.Slice {
		return e.EncodeSlice(val, buf)
	}

	// ensure Go type layout is resolved
	if err := e.initLayout(val); err != nil {
		return nil, err
	}

	// ensure the type actually matches our layout
	if rval.Type() != e.layout.Type {
		return nil, fmt.Errorf("encode: type mismatch: expected %s, have %s", e.layout.Type, rval.Type())
	}

	// ensure we have a target buffer
	if buf == nil {
		buf = e.NewBuffer(1)
	}

	// process opcodes
	base := rval.Addr().UnsafePointer()
	for op, code := range e.opcodes {
		if code == OC_SKIP {
			continue
		}
		ptr := unsafe.Add(base, e.layout.Offsets[op])
		err := e.writeField(buf, code, e.schema.Fields[op], ptr)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", e.schema.Fields[op].Name, err)
		}
	}
	return buf.Bytes(), nil
}

func (e *Encoder) EncodeSlice(slice any, buf *bytes.Buffer) ([]byte, error) {
	// validate value
	if slice == nil {
		return nil, schema.ErrNilValue
	}
	rslice := reflect.Indirect(reflect.ValueOf(slice))
	if !rslice.IsValid() || rslice.Kind() != reflect.Slice {
		return nil, schema.ErrInvalidValueType
	}

	// redirect
	etyp := rslice.Type().Elem()
	if etyp.Kind() == reflect.Pointer {
		return e.EncodePtrSlice(slice, buf)
	}

	// ensure Go type layout is resolved
	if err := e.initLayout(slice); err != nil {
		return nil, err
	}

	// ensure the type actually matches our layout
	if etyp != e.layout.Type {
		return nil, fmt.Errorf("encode: type mismatch: expected %s, have %s", e.layout.Type, etyp)
	}

	// ensure target buffer is allocated
	if buf == nil {
		buf = e.NewBuffer(rslice.Len())
	}

	base := rslice.UnsafePointer()
	for range rslice.Len() {
		for op, code := range e.opcodes {
			if code == OC_SKIP {
				continue
			}
			ptr := unsafe.Add(base, e.layout.Offsets[op])
			err := e.writeField(buf, code, e.schema.Fields[op], ptr)
			if err != nil {
				return nil, fmt.Errorf("%s: %w", e.schema.Fields[op].Name, err)
			}
		}
		base = unsafe.Add(base, e.layout.Size)
	}
	return buf.Bytes(), nil
}

func (e *Encoder) EncodePtrSlice(slice any, buf *bytes.Buffer) ([]byte, error) {
	// validate
	if slice == nil {
		return nil, schema.ErrNilValue
	}
	rslice := reflect.Indirect(reflect.ValueOf(slice))
	if !rslice.IsValid() ||
		rslice.Kind() != reflect.Slice ||
		rslice.Type().Elem().Kind() != reflect.Pointer {
		return nil, schema.ErrInvalidValueType
	}

	// ensure Go type layout is resolved
	if err := e.initLayout(slice); err != nil {
		return nil, err
	}

	// ensure the type actually matches our layout
	if etyp := rslice.Type().Elem().Elem(); etyp != e.layout.Type {
		return nil, fmt.Errorf("encode: type mismatch: expected %s, have %s", e.layout.Type, etyp)
	}

	// ensure target buffer is allocated
	if buf == nil {
		buf = e.NewBuffer(rslice.Len())
	}

	// process slice elementys
	for i := range rslice.Len() {
		base := rslice.Index(i).UnsafePointer()
		for op, code := range e.opcodes {
			if code == OC_SKIP {
				continue
			}
			ptr := unsafe.Add(base, e.layout.Offsets[op])
			err := e.writeField(buf, code, e.schema.Fields[op], ptr)
			if err != nil {
				return nil, fmt.Errorf("%s: %w", e.schema.Fields[op].Name, err)
			}
		}
	}
	return buf.Bytes(), nil
}

func (e *Encoder) encodeNestedSlice(base unsafe.Pointer, baseLen int, buf *bytes.Buffer) error {
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

		// call encoder
		err = sub.encodeNestedSlice(slice.Data, slice.Len, buf)

		// patch len in bytes
		*(*uint32)(unsafe.Pointer(&buf.Bytes()[ofs])) = uint32(buf.Len() - ofs - 4)
	}
	return
}

// A Go slice header up until at least Go 1.26 assuming
// Go's internal representation remains unchanged.
//
// This is currently the only way to cast an unsafe.Pointer
// to a struct field offset back to the slice at this offset
// because reflect.SliceAt requires data pointer and length.
type sliceType struct {
	Data unsafe.Pointer
	Len  int
	Cap  int
}
