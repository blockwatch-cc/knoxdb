// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package schema

import (
	"bytes"
	"encoding"
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
	schema  *Schema
	needsif bool
}

func NewEncoder(s *Schema) *Encoder {
	var needsif bool
	for _, c := range s.encode {
		if c.NeedsInterface() {
			needsif = true
			break
		}
	}
	return &Encoder{
		schema:  s,
		needsif: needsif,
	}
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
	if e.needsif {
		for op, code := range e.schema.encode {
			field := e.schema.fields[op]
			if code.NeedsInterface() {
				err = writeReflectField(buf, code, rval.FieldByIndex(field.path).Interface())
			} else {
				ptr := unsafe.Add(base, field.offset)
				err = writeField(buf, code, field, ptr)
			}
			if err != nil {
				return nil, err
			}
		}

	} else {
		for op, code := range e.schema.encode {
			field := e.schema.fields[op]
			ptr := unsafe.Add(base, field.offset)
			err = writeField(buf, code, field, ptr)
			if err != nil {
				return nil, err
			}
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
	if e.needsif {
		for i, l := 0, rslice.Len(); i < l; i++ {
			rval := rslice.Index(i)
			for op, code := range e.schema.encode {
				field := e.schema.fields[op]
				if !code.NeedsInterface() {
					ptr := unsafe.Add(base, field.offset)
					err = writeField(buf, code, field, ptr)
				} else {
					err = writeReflectField(buf, code, rval.FieldByIndex(field.path).Interface())
				}
				if err != nil {
					return nil, err
				}
			}
			base = unsafe.Add(base, sz)
		}
	} else {
		for i, l := 0, rslice.Len(); i < l; i++ {
			for op, code := range e.schema.encode {
				field := e.schema.fields[op]
				ptr := unsafe.Add(base, field.offset)
				err = writeField(buf, code, field, ptr)
				if err != nil {
					return nil, err
				}
			}
			base = unsafe.Add(base, sz)
		}
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
	if e.needsif {
		for i, l := 0, rslice.Len(); i < l; i++ {
			rval := rslice.Index(i)
			base := rval.UnsafePointer()
			for op, code := range e.schema.encode {
				field := e.schema.fields[op]
				if !code.NeedsInterface() {
					ptr := unsafe.Add(base, field.offset)
					err = writeField(buf, code, field, ptr)
				} else {
					err = writeReflectField(buf, code, rval.Elem().FieldByIndex(field.path).Interface())
				}
				if err != nil {
					return nil, err
				}
			}
		}
	} else {
		for i, l := 0, rslice.Len(); i < l; i++ {
			base := rslice.Index(i).UnsafePointer()
			for op, code := range e.schema.encode {
				field := e.schema.fields[op]
				ptr := unsafe.Add(base, field.offset)
				err = writeField(buf, code, field, ptr)
				if err != nil {
					return nil, err
				}
			}
		}
	}
	return buf.Bytes(), nil
}

func writeReflectField(buf *bytes.Buffer, code OpCode, rval any) error {
	var (
		err error
		b   []byte
	)
	switch code {
	case OpCodeMarshalBinary:
		b, err = rval.(encoding.BinaryMarshaler).MarshalBinary()
		if err != nil {
			return err
		}
		buf.Write(Uint32Bytes(uint32(len(b))))
		_, err = buf.Write(b)

	case OpCodeMarshalText:
		b, err = rval.(encoding.TextMarshaler).MarshalText()
		if err != nil {
			return err
		}
		buf.Write(Uint32Bytes(uint32(len(b))))
		_, err = buf.Write(b)

	case OpCodeStringer:
		s := rval.(fmt.Stringer).String()
		buf.Write(Uint32Bytes(uint32(len(s))))
		_, err = buf.Write(unsafe.Slice(unsafe.StringData(s), len(s)))
	}
	return err
}

func writeField(buf *bytes.Buffer, code OpCode, field Field, ptr unsafe.Pointer) error {
	var err error
	switch code {
	default:
		// int, uint, float, bool
		_, err = buf.Write(unsafe.Slice((*byte)(ptr), field.wireSize))

	case OpCodeFixedArray:
		_, err = buf.Write(unsafe.Slice((*byte)(ptr), field.fixed))

	case OpCodeFixedString:
		s := *(*string)(ptr)
		_, err = buf.Write(unsafe.Slice(unsafe.StringData(s), field.fixed))

	case OpCodeFixedBytes:
		b := *(*[]byte)(ptr)
		_, err = buf.Write(b[:field.fixed])

	case OpCodeString:
		s := *(*string)(ptr)
		buf.Write(Uint32Bytes(uint32(len(s))))
		_, err = buf.WriteString(s)

	case OpCodeBytes:
		b := *(*[]byte)(ptr)
		buf.Write(Uint32Bytes(uint32(len(b))))
		_, err = buf.Write(b)

	case OpCodeDateTime:
		tm := *(*time.Time)(ptr)
		_, err = buf.Write(Uint64Bytes(uint64(tm.UnixNano())))

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
		v := *(*Enum)(ptr)
		var lut EnumLUT
		lut, err = LookupEnum(field.name)
		if err != nil {
			return fmt.Errorf("%s: %v", field.name, err)
		}
		code, ok := lut.Code(v)
		if !ok {
			return fmt.Errorf("%s: invalid enum value %q", field.name, v)
		}
		buf.Write(Uint16Bytes(code))
	}
	return err
}
