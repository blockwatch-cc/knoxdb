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

func (e *EncoderT[T]) Encode(buf *bytes.Buffer, vals ...*T) error {
	// noop when no values
	if len(vals) == 0 {
		return nil
	}

	// redirect to marshaler if implemented by elem type
	if e.layout.Marshaler != nil {
		return e.marshalPtrSlice(unsafe.Pointer(&vals[0]), len(vals), buf)
	}

	// process slice elements
	return e.encodePtrSlice(unsafe.Pointer(&vals[0]), len(vals), buf)
}

func (e *EncoderT[T]) Close() {
	e.Encoder.Close()
	e.Encoder = nil
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

// Encode encodes single values of type T and *T into buffer.
// Non-pointer types T must implement schema.Marshaler or will
// fail otherwise because call by value interfaces are non-addressable
// in Go. Users must pass a non-nil buffer which will be used for
// appending data. When properly dimensioned encode is allocation-free.
func (e *Encoder) Encode(buf *bytes.Buffer, val any) error {
	// noop when value is nil
	if val == nil {
		return nil
	}

	// redirect to marshaler when implemented
	if m, ok := val.(schema.Marshaler); ok {
		// use temp writer
		w := schema.NewWriter(e.schema, buf)
		defer w.Close()
		if err := m.MarshalSchema(w); err != nil {
			return err
		}

		// skip unwritten fields (just in case)
		w.Next()
		return nil
	}

	// validate type
	rval := reflect.ValueOf(val)
	if rval.Kind() != reflect.Pointer || rval.Elem().Kind() != reflect.Struct {
		return fmt.Errorf("encode: expected struct pointer type, have %s", rval.Type())
	}

	// ensure Go type layout is resolved
	if err := e.initLayout(val); err != nil {
		return err
	}

	// ensure the type actually matches our layout
	if rval.Elem().Type() != e.layout.Type {
		return fmt.Errorf("encode: type mismatch: expected %s, have %s", e.layout.Type, rval.Type())
	}

	// process opcodes
	return e.encodeSlice(rval.UnsafePointer(), 1, buf)
}

// EncodeBatch encodes values of type []T and []*T into buffer.
// Users must pass a non-nil buffer which will be used for appending
// data. When properly dimensioned encode is allocation-free.
func (e *Encoder) EncodeBatch(buf *bytes.Buffer, val any) error {
	// noop when value is nil
	if val == nil {
		return nil
	}

	// validate slice type
	rslice := reflect.Indirect(reflect.ValueOf(val))
	if !rslice.IsValid() || rslice.Kind() != reflect.Slice {
		return schema.ErrInvalidValueType
	}

	// noop when slice is empty
	if rslice.Len() == 0 {
		return nil
	}

	// redirect when slice elements ar pointers
	etyp := rslice.Type().Elem()
	if etyp.Kind() == reflect.Pointer {
		return e.encodePtrBatch(buf, val)
	}

	// ensure Go type layout is resolved
	if err := e.initLayout(val); err != nil {
		return err
	}

	// redirect to marshaler if implemented by elem type
	if e.layout.Marshaler != nil {
		return e.marshalSlice(rslice.UnsafePointer(), rslice.Len(), buf)
	}

	// ensure the type actually matches our layout
	if etyp != e.layout.Type {
		return fmt.Errorf("encode: type mismatch: expected %s, have %s", e.layout.Type, etyp)
	}

	// process slice elements
	return e.encodeSlice(rslice.UnsafePointer(), rslice.Len(), buf)
}

func (e *Encoder) encodePtrBatch(buf *bytes.Buffer, val any) error {
	// ensure Go type layout is resolved
	if err := e.initLayout(val); err != nil {
		return err
	}

	rslice := reflect.Indirect(reflect.ValueOf(val))

	// redirect to marshaler if implemented by elem type
	if e.layout.Marshaler != nil {
		return e.marshalPtrSlice(rslice.UnsafePointer(), rslice.Len(), buf)
	}

	// ensure the type actually matches our layout
	if etyp := rslice.Type().Elem().Elem(); etyp != e.layout.Type {
		return fmt.Errorf("encode: type mismatch: expected %s, have %s", e.layout.Type, etyp)
	}

	// process slice elements
	return e.encodePtrSlice(rslice.UnsafePointer(), rslice.Len(), buf)
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
		// deref []*T elements
		elem := *(*unsafe.Pointer)(base)
		if elem == nil {
			return schema.ErrNilValue
		}
		for op, code := range e.opcodes {
			if code == OC_SKIP {
				continue
			}
			ptr := unsafe.Add(elem, e.layout.Offsets[op])
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
		err = v.MarshalBuffer(buf)

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
