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

func (e *GenericEncoder[T]) Encode(buf *bytes.Buffer, val T) {
	e.enc.Encode(buf, &val)
}

func (e *GenericEncoder[T]) EncodePtr(buf *bytes.Buffer, val *T) {
	e.enc.Encode(buf, val)
}

func (e *GenericEncoder[T]) EncodeSlice(buf *bytes.Buffer, slice []T) {
	e.enc.EncodeSlice(buf, &slice)
}

func (e *GenericEncoder[T]) EncodePtrSlice(buf *bytes.Buffer, slice []*T) {
	e.enc.EncodeSlice(buf, &slice)
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

func (e *Encoder) Encode(buf *bytes.Buffer, val any) {
	rval := reflect.Indirect(reflect.ValueOf(val))
	base := rval.Addr().UnsafePointer()
	if e.needsif {
		for op, code := range e.schema.encode {
			field := e.schema.fields[op]
			if code.NeedsInterface() {
				writeReflectField(buf, code, rval.FieldByIndex(field.path).Interface())
			} else {
				ptr := unsafe.Add(base, field.offset)
				writeField(buf, code, field, ptr)
			}
		}

	} else {
		for op, code := range e.schema.encode {
			field := e.schema.fields[op]
			ptr := unsafe.Add(base, field.offset)
			writeField(buf, code, field, ptr)
		}
	}
}

func (e *Encoder) EncodeSlice(buf *bytes.Buffer, slice any) {
	rslice := reflect.Indirect(reflect.ValueOf(slice))
	base := rslice.UnsafePointer()
	etyp := rslice.Type().Elem()
	sz := etyp.Size()
	isPtr := etyp.Kind() == reflect.Pointer
	if isPtr {
		sz = etyp.Elem().Size()
	}
	num := rslice.Len()

	if isPtr {
		if !e.needsif {
			for i, l := 0, num; i < l; i++ {
				base = rslice.Index(i).UnsafePointer()
				for op, code := range e.schema.encode {
					field := e.schema.fields[op]
					ptr := unsafe.Add(base, field.offset)
					writeField(buf, code, field, ptr)
				}
			}
		} else {
			for i, l := 0, num; i < l; i++ {
				rval := rslice.Index(i)
				base = rval.UnsafePointer()
				for op, code := range e.schema.encode {
					field := e.schema.fields[op]
					if !code.NeedsInterface() {
						ptr := unsafe.Add(base, field.offset)
						writeField(buf, code, field, ptr)
					} else {
						writeReflectField(buf, code, rval.Elem().FieldByIndex(field.path).Interface())
					}
				}
			}
		}
	} else {
		if !e.needsif {
			for i, l := 0, num; i < l; i++ {
				for op, code := range e.schema.encode {
					field := e.schema.fields[op]
					ptr := unsafe.Add(base, field.offset)
					writeField(buf, code, field, ptr)
				}
				base = unsafe.Add(base, sz)
			}
		} else {
			for i, l := 0, num; i < l; i++ {
				rval := rslice.Index(i)
				for op, code := range e.schema.encode {
					field := e.schema.fields[op]
					if !code.NeedsInterface() {
						ptr := unsafe.Add(base, field.offset)
						writeField(buf, code, field, ptr)
					} else {
						writeReflectField(buf, code, rval.FieldByIndex(field.path).Interface())
					}
				}
				base = unsafe.Add(base, sz)
			}
		}
	}
}

func (e *Encoder) EncodePtrSlice(buf *bytes.Buffer, slice any) {
	rslice := reflect.Indirect(reflect.ValueOf(slice))
	if e.needsif {
		for i, l := 0, rslice.Len(); i < l; i++ {
			rval := reflect.Indirect(rslice.Index(i))
			base := rval.UnsafePointer()
			for op, code := range e.schema.encode {
				field := e.schema.fields[op]
				if !code.NeedsInterface() {
					ptr := unsafe.Add(base, field.offset)
					writeField(buf, code, field, ptr)
				} else {
					writeReflectField(buf, code, rval.FieldByIndex(field.path).Interface())
				}
			}
		}
	} else {
		for i, l := 0, rslice.Len(); i < l; i++ {
			rval := reflect.Indirect(rslice.Index(i))
			base := rval.UnsafePointer()
			for op, code := range e.schema.encode {
				field := e.schema.fields[op]
				ptr := unsafe.Add(base, field.offset)
				writeField(buf, code, field, ptr)
			}
		}
	}
}

func writeReflectField(buf *bytes.Buffer, code OpCode, rval any) {
	switch code {
	case OpCodeMarshalBinary:
		b, _ := rval.(encoding.BinaryMarshaler).MarshalBinary()
		buf.Write(Uint32Bytes(uint32(len(b))))
		buf.Write(b)

	case OpCodeMarshalText:
		b, _ := rval.(encoding.TextMarshaler).MarshalText()
		buf.Write(Uint32Bytes(uint32(len(b))))
		buf.Write(b)

	case OpCodeStringer:
		s := rval.(fmt.Stringer).String()
		buf.Write(Uint32Bytes(uint32(len(s))))
		buf.Write(unsafe.Slice(unsafe.StringData(s), len(s)))
	}
}

func writeField(buf *bytes.Buffer, code OpCode, field Field, ptr unsafe.Pointer) {
	switch code {
	default:
		// int, uint, float, bool
		buf.Write(unsafe.Slice((*byte)(ptr), field.dataSize))

	case OpCodeFixedArray:
		buf.Write(unsafe.Slice((*byte)(ptr), field.fixed))

	case OpCodeFixedString:
		s := *(*string)(ptr)
		buf.Write(unsafe.Slice(unsafe.StringData(s), field.fixed))

	case OpCodeFixedBytes:
		b := *(*[]byte)(ptr)
		buf.Write(b[:field.fixed])

	case OpCodeString:
		s := *(*string)(ptr)
		buf.Write(Uint32Bytes(uint32(len(s))))
		buf.WriteString(s)

	case OpCodeBytes:
		b := *(*[]byte)(ptr)
		buf.Write(Uint32Bytes(uint32(len(b))))
		buf.Write(b)

	case OpCodeDateTime:
		tm := *(*time.Time)(ptr)
		buf.Write(Uint64Bytes(uint64(tm.UnixNano())))

	case OpCodeInt256:
		v := *(*num.Int256)(ptr)
		buf.Write(v.Bytes())

	case OpCodeInt128:
		v := *(*num.Int128)(ptr)
		buf.Write(v.Bytes())

	case OpCodeDecimal32:
		buf.Write(unsafe.Slice((*byte)(ptr), 4))

	case OpCodeDecimal64:
		buf.Write(unsafe.Slice((*byte)(ptr), 8))

	case OpCodeDecimal128:
		v := *(*num.Decimal128)(ptr)
		buf.Write(v.Int128().Bytes())

	case OpCodeDecimal256:
		v := *(*num.Decimal256)(ptr)
		buf.Write(v.Int256().Bytes())
	}
}
