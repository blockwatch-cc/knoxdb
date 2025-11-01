// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package schema

import (
	"bytes"
	"fmt"
	"reflect"
	"time"
	"unsafe"

	"blockwatch.cc/knoxdb/pkg/num"
)

type GenericEncoder[T any] struct {
	enc *Encoder
}

func NewGenericEncoder[T any]() *GenericEncoder[T] {
	s, err := GenericSchema[T]()
	if err != nil {
		panic(err)
	}
	return &GenericEncoder[T]{
		enc: NewEncoder(s),
	}
}

func (e *GenericEncoder[T]) Schema() *Schema {
	return e.enc.schema
}

func (e *GenericEncoder[T]) WithEnums(reg *EnumRegistry) *GenericEncoder[T] {
	e.enc.WithEnums(reg)
	return e
}

func (e *GenericEncoder[T]) NewBuffer(sz int) *bytes.Buffer {
	return e.enc.schema.NewBuffer(sz)
}

func (e *GenericEncoder[T]) Encode(val T, buf *bytes.Buffer) ([]byte, error) {
	return e.enc.Encode(&val, buf)
}

func (e *GenericEncoder[T]) EncodePtr(val *T, buf *bytes.Buffer) ([]byte, error) {
	return e.enc.Encode(val, buf)
}

func (e *GenericEncoder[T]) EncodeSlice(slice []T, buf *bytes.Buffer) ([]byte, error) {
	return e.enc.EncodeSlice(&slice, buf)
}

func (e *GenericEncoder[T]) EncodePtrSlice(slice []*T, buf *bytes.Buffer) ([]byte, error) {
	return e.enc.EncodeSlice(&slice, buf)
}

type Encoder struct {
	schema *Schema
	enums  *EnumRegistry
}

func NewEncoder(s *Schema) *Encoder {
	enums := s.Enums.Load()
	if enums == nil {
		enums = GlobalRegistry
	}
	return &Encoder{
		schema: s,
		enums:  enums,
	}
}

func (e *Encoder) WithEnums(reg *EnumRegistry) *Encoder {
	e.enums = reg
	return e
}

func (e *Encoder) Schema() *Schema {
	return e.schema
}

func (e *Encoder) NewBuffer(sz int) *bytes.Buffer {
	return e.schema.NewBuffer(sz)
}

func (e *Encoder) Encode(val any, buf *bytes.Buffer) ([]byte, error) {
	rval := reflect.Indirect(reflect.ValueOf(val))
	if rval.Kind() == reflect.Slice {
		return e.EncodeSlice(val, buf)
	}
	base := rval.Addr().UnsafePointer()
	if buf == nil {
		buf = e.NewBuffer(1)
	}
	var err error
	for op, code := range e.schema.Encode {
		if code == OpCodeSkip {
			continue
		}
		field := e.schema.Fields[op]
		ptr := unsafe.Add(base, field.Offset)
		err = writeField(buf, code, field, ptr, e.enums)
		if err != nil {
			return nil, err
		}
	}
	return buf.Bytes(), nil
}

func (e *Encoder) EncodeSlice(slice any, buf *bytes.Buffer) ([]byte, error) {
	if slice == nil {
		return nil, ErrNilValue
	}
	rslice := reflect.Indirect(reflect.ValueOf(slice))
	if !rslice.IsValid() || rslice.Kind() != reflect.Slice {
		return nil, ErrInvalidValue
	}
	etyp := rslice.Type().Elem()
	if etyp.Kind() == reflect.Pointer {
		return e.EncodePtrSlice(slice, buf)
	}
	sz := etyp.Size()
	base := rslice.UnsafePointer()
	if buf == nil {
		buf = e.NewBuffer(rslice.Len())
	}

	var err error
	for i, l := 0, rslice.Len(); i < l; i++ {
		for op, code := range e.schema.Encode {
			if code == OpCodeSkip {
				continue
			}
			field := e.schema.Fields[op]
			ptr := unsafe.Add(base, field.Offset)
			err = writeField(buf, code, field, ptr, e.enums)
			if err != nil {
				return nil, err
			}
		}
		base = unsafe.Add(base, sz)
	}
	return buf.Bytes(), nil
}

func (e *Encoder) EncodePtrSlice(slice any, buf *bytes.Buffer) ([]byte, error) {
	if slice == nil {
		return nil, ErrNilValue
	}
	rslice := reflect.Indirect(reflect.ValueOf(slice))
	if !rslice.IsValid() ||
		rslice.Kind() != reflect.Slice ||
		rslice.Type().Elem().Kind() != reflect.Pointer {
		return nil, ErrInvalidValue
	}
	if buf == nil {
		buf = e.NewBuffer(rslice.Len())
	}
	var err error
	for i, l := 0, rslice.Len(); i < l; i++ {
		base := rslice.Index(i).UnsafePointer()
		for op, code := range e.schema.Encode {
			if code == OpCodeSkip {
				continue
			}
			field := e.schema.Fields[op]
			ptr := unsafe.Add(base, field.Offset)
			err = writeField(buf, code, field, ptr, e.enums)
			if err != nil {
				return nil, err
			}
		}
	}
	return buf.Bytes(), nil
}

func writeField(buf *bytes.Buffer, code OpCode, field *Field, ptr unsafe.Pointer, enums *EnumRegistry) error {
	var (
		err error
		sz  [4]byte
	)
	switch code {
	default:
		// int, uint, float, bool
		_, err = buf.Write(unsafe.Slice((*byte)(ptr), field.Size))

	case OpCodeFixedBytes:
		_, err = buf.Write(unsafe.Slice((*byte)(ptr), field.Fixed))

	case OpCodeFixedString:
		s := *(*string)(ptr)
		_, err = buf.Write(unsafe.Slice(unsafe.StringData(s), field.Fixed))

	case OpCodeString:
		s := *(*string)(ptr)
		LE.PutUint32(sz[:], uint32(len(s)))
		buf.Write(sz[:])
		_, err = buf.WriteString(s)

	case OpCodeBytes:
		b := *(*[]byte)(ptr)
		LE.PutUint32(sz[:], uint32(len(b)))
		buf.Write(sz[:])
		_, err = buf.Write(b)

	case OpCodeTimestamp, OpCodeTime, OpCodeDate:
		tm := *(*time.Time)(ptr)
		var b [8]byte
		LE.PutUint64(b[:], uint64(TimeScale(field.Scale).ToUnix(tm)))
		_, err = buf.Write(b[:])

	case OpCodeInt256:
		v := *(*num.Int256)(ptr)
		_, err = buf.Write(v.Bytes())

	case OpCodeInt128:
		v := *(*num.Int128)(ptr)
		_, err = buf.Write(v.Bytes())

	case OpCodeDecimal32:
		_, err = buf.Write(unsafe.Slice((*byte)(ptr), 4))

	case OpCodeDecimal64:
		_, err = buf.Write(unsafe.Slice((*byte)(ptr), 8))

	case OpCodeDecimal128:
		v := *(*num.Decimal128)(ptr)
		_, err = buf.Write(v.Int128().Bytes())

	case OpCodeDecimal256:
		v := *(*num.Decimal256)(ptr)
		_, err = buf.Write(v.Int256().Bytes())

	case OpCodeEnum:
		if enums == nil {
			return ErrEnumUndefined
		}
		enum, ok := enums.Lookup(field.Name)
		if !ok {
			return ErrEnumUndefined
		}
		v := *(*string)(ptr)
		code, ok := enum.Code(v)
		if !ok {
			return fmt.Errorf("%s: invalid enum value %q", field.Name, v)
		}
		var b [2]byte
		LE.PutUint16(b[:], code)
		_, err = buf.Write(b[:])

	case OpCodeBigInt:
		v := *(*num.Big)(ptr)
		b := v.Bytes()
		LE.PutUint32(sz[:], uint32(len(b)))
		buf.Write(sz[:])
		_, err = buf.Write(b)
	}
	return err
}
