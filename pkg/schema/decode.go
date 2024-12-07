// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package schema

import (
	"bytes"
	"encoding"
	"fmt"
	"io"
	"reflect"
	"time"
	"unsafe"

	"blockwatch.cc/knoxdb/pkg/num"
)

type GenericDecoder[T any] struct {
	dec *Decoder
}

func NewGenericDecoder[T any]() *GenericDecoder[T] {
	s, err := GenericSchema[T]()
	if err != nil {
		panic(err)
	}
	return &GenericDecoder[T]{
		dec: NewDecoder(s),
	}
}

func (d *GenericDecoder[T]) Schema() *Schema {
	return d.dec.schema
}

func (d *GenericDecoder[T]) WithEnums(reg EnumRegistry) *GenericDecoder[T] {
	d.dec.WithEnums(reg)
	return d
}

// Read reads wire encoded data from r and decodes into val based on
// the schema for T.
//
// When wire size is fixed we can read and decode in one step.
// Otherwise we take a slow path that reads variable length data
// as length fields are encountered. This issues multiple calls
// to the underlying reader, likely a host-side io.Stream.
//
// Reading is staged through an internal decoder buffer
// with an inital size of minWireSize bytes. This buffer gets
// extended whenever a dynamic data type length is found so
// that it contains at least the bytes for the dynamic data
// plus all fixed bytes for following fields a that time.
// Because the buffer may grow and reallocate it is NOT SAFE
// to reference memory for strings and byte slices and hence
// we make explicit copies. Moreover, a copy is necessary to
// safely retain returned objects since the internal buffer is
// re-used between calls.
func (d *GenericDecoder[T]) Read(r io.Reader) (val *T, err error) {
	val = new(T)
	err = d.dec.Read(r, val)
	return
}

func (d *GenericDecoder[T]) Decode(buf []byte, val *T) (*T, error) {
	if val == nil {
		val = new(T)
	}
	err := d.dec.Decode(buf, val)
	return val, err
}

func (d *GenericDecoder[T]) DecodeSlice(buf []byte, res []T) ([]T, error) {
	if res == nil {
		// We slightly over-allocate the result slice when data contains
		// long strings/bytes, however this single allocation is still
		// much more performant than growing the slice multiple times.
		// For fixed-size schemas, the single allocation is all we need.
		res = make([]T, len(buf)/max(d.dec.schema.minWireSize, 1))
	}
	n, err := d.dec.DecodeSlice(buf, res)
	if err != nil {
		return nil, err
	}
	return res[:n], nil
}

type Decoder struct {
	schema  *Schema
	enums   EnumRegistry
	needsif bool
	buf     *bytes.Buffer
}

func NewDecoder(s *Schema) *Decoder {
	var needsif bool
	for _, c := range s.decode {
		if c.NeedsInterface() {
			needsif = true
			break
		}
	}
	return &Decoder{
		schema:  s,
		needsif: needsif,
		enums:   enumRegistry,
		buf:     bytes.NewBuffer(make([]byte, 0, s.maxWireSize)),
	}
}

func (d *Decoder) Schema() *Schema {
	return d.schema
}

func (d *Decoder) WithEnums(reg EnumRegistry) *Decoder {
	d.enums = reg
	return d
}

// Read reads wire encoded data from r and decodes into val based on
// the schema for T.
//
// When wire size is fixed we can read and decode in one step.
// Otherwise we take a slow path that reads variable length data
// as length fields are encountered. This issues multiple calls
// to the underlying reader, likely a host-side io.Stream.
//
// Reading is staged through an internal decoder buffer
// with an inital size of minWireSize bytes. This buffer gets
// extended whenever a dynamic data type length is found so
// that it contains at least the bytes for the dynamic data
// plus all fixed bytes for following fields a that time.
// Because the buffer may grow and reallocate it is NOT SAFE
// to reference memory for strings and byte slices and hence
// we make explicit copies. Moreover, a copy is necessary to
// safely retain returned objects since the internal buffer is
// re-used between calls.
func (d *Decoder) Read(r io.Reader, val any) error {
	// reset decoder buffer
	d.buf.Reset()

	// read first chunk of data (this is sufficient when schema is fixed size)
	n, err := io.CopyN(d.buf, r, int64(d.schema.minWireSize))
	if err != nil {
		return err
	}
	if n != int64(d.schema.minWireSize) {
		return ErrShortBuffer
	}

	// fast path decode fixed size data
	if d.schema.isFixedSize {
		return d.Decode(d.buf.Bytes(), val)
	}

	// slow path decode with additional read calls (may reallocate buffer!)
	if val == nil {
		return ErrNilValue
	}
	rval := reflect.Indirect(reflect.ValueOf(val))
	base := rval.Addr().UnsafePointer()

	for op, code := range d.schema.decode {
		field := d.schema.fields[op]
		ptr := unsafe.Add(base, field.offset)
		switch code {
		default:
			// int, uint, float, bool
			_, err = d.buf.Read(unsafe.Slice((*byte)(ptr), field.wireSize))

		case OpCodeSkip:
			// noop

		case OpCodeFixedArray:
			_, err = d.buf.Read(unsafe.Slice((*byte)(ptr), field.fixed))

		case OpCodeFixedString:
			// explicit copy
			*(*string)(ptr) = string(d.buf.Next(int(field.fixed)))

		case OpCodeFixedBytes:
			// explicit copy
			*(*[]byte)(ptr) = bytes.Clone(d.buf.Next(int(field.fixed)))

		case OpCodeString:
			l, _ := ReadUint32(d.buf.Next(4))
			n, err = io.CopyN(d.buf, r, int64(l)) // may realloc!
			if err != nil {
				return err
			}
			if n != int64(l) {
				return ErrShortBuffer
			}
			// explicit copy
			*(*string)(ptr) = string(d.buf.Next(int(l)))

		case OpCodeBytes:
			l, _ := ReadUint32(d.buf.Next(4))
			n, err = io.CopyN(d.buf, r, int64(l)) // may realloc!
			if err != nil {
				return err
			}
			if n != int64(l) {
				return ErrShortBuffer
			}
			// explicit copy
			*(*[]byte)(ptr) = bytes.Clone(d.buf.Next(int(l)))

		case OpCodeUnmarshalBinary, OpCodeUnmarshalText:
			l, _ := ReadUint32(d.buf.Next(4))
			n, err = io.CopyN(d.buf, r, int64(l)) // may realloc!
			if err != nil {
				return err
			}
			if n != int64(l) {
				return ErrShortBuffer
			}

			// need reflection to access interface
			dst := field.StructValue(rval)
			ri := dst.Addr().Interface()
			if code == OpCodeUnmarshalBinary {
				err = ri.(encoding.BinaryUnmarshaler).UnmarshalBinary(d.buf.Next(int(l)))
			} else {
				err = ri.(encoding.TextUnmarshaler).UnmarshalText(d.buf.Next(int(l)))
			}

		case OpCodeDateTime:
			ts, _ := ReadInt64(d.buf.Next(8))
			*(*time.Time)(ptr) = time.Unix(0, ts).UTC()

		case OpCodeInt128:
			*(*num.Int128)(ptr) = num.Int128FromBytes(d.buf.Next(16))

		case OpCodeInt256:
			*(*num.Int256)(ptr) = num.Int256FromBytes(d.buf.Next(32))

		case OpCodeDecimal32:
			i32, _ := ReadInt32(d.buf.Next(4))
			(*(*num.Decimal32)(ptr)).Set(i32)
			(*(*num.Decimal32)(ptr)).SetScale(field.Scale())

		case OpCodeDecimal64:
			i64, _ := ReadInt64(d.buf.Next(8))
			(*(*num.Decimal64)(ptr)).Set(i64)
			(*(*num.Decimal64)(ptr)).SetScale(field.scale)

		case OpCodeDecimal128:
			(*(*num.Decimal128)(ptr)).Set(num.Int128FromBytes(d.buf.Next(16)))
			(*(*num.Decimal128)(ptr)).SetScale(field.scale)

		case OpCodeDecimal256:
			(*(*num.Decimal256)(ptr)).Set(num.Int256FromBytes(d.buf.Next(32)))
			(*(*num.Decimal256)(ptr)).SetScale(field.scale)

		case OpCodeEnum:
			u16, _ := ReadUint16(d.buf.Next(2))
			if enum, ok := d.enums.Lookup(field.name); ok {
				val, ok := enum.Value(u16)
				if !ok {
					err = fmt.Errorf("%s: invalid enum value %d", field.name, u16)
				}
				*(*string)(ptr) = string(val)
			} else {
				err = fmt.Errorf("translation for enum %q not registered", field.name)
			}
		}

		if err != nil {
			return err
		}
	}
	return nil
}

func (d *Decoder) Decode(buf []byte, val any) error {
	if val == nil {
		return ErrNilValue
	}
	rval := reflect.Indirect(reflect.ValueOf(val))
	base := rval.Addr().UnsafePointer()
	if d.needsif {
		for op, code := range d.schema.decode {
			if code == OpCodeSkip {
				continue
			}
			field := &d.schema.fields[op]
			if code.NeedsInterface() {
				dst := field.StructValue(rval)
				buf = readReflectField(code, dst.Addr().Interface(), buf)
			} else {
				ptr := unsafe.Add(base, field.offset)
				buf = readField(code, field, ptr, buf, d.enums)
			}
		}
	} else {
		for op, code := range d.schema.decode {
			if code == OpCodeSkip {
				continue
			}
			field := &d.schema.fields[op]
			ptr := unsafe.Add(base, field.offset)
			buf = readField(code, field, ptr, buf, d.enums)
		}
	}
	return nil
}

func (d *Decoder) DecodeSlice(buf []byte, slice any) (int, error) {
	if slice == nil {
		return 0, ErrNilValue
	}
	rslice := reflect.Indirect(reflect.ValueOf(slice))
	base := rslice.UnsafePointer()
	sz := rslice.Type().Elem().Size()
	num := rslice.Len()
	ops := d.schema.decode

	var i int
	if d.needsif {
		for i = 0; i < num && len(buf) > 0; i++ {
			rval := rslice.Index(i)
			for op, code := range ops {
				if code == OpCodeSkip {
					continue
				}
				field := &d.schema.fields[op]
				if code.NeedsInterface() {
					dst := field.StructValue(rval)
					buf = readReflectField(code, dst.Addr().Interface(), buf)
				} else {
					ptr := unsafe.Add(base, field.offset)
					buf = readField(code, field, ptr, buf, d.enums)
				}
			}
			base = unsafe.Add(base, sz)
		}
	} else {
		for i = 0; i < num && len(buf) > 0; i++ {
			for op, code := range ops {
				if code == OpCodeSkip {
					continue
				}
				field := &d.schema.fields[op]
				ptr := unsafe.Add(base, field.offset)
				buf = readField(code, field, ptr, buf, d.enums)
			}
			base = unsafe.Add(base, sz)
		}
	}
	return i, nil
}

func readReflectField(code OpCode, rval any, buf []byte) []byte {
	switch code {
	case OpCodeUnmarshalBinary:
		l, n := ReadUint32(buf)
		buf = buf[n:]
		if l > 0 {
			_ = buf[l-1]
			_ = rval.(encoding.BinaryUnmarshaler).UnmarshalBinary(buf[:l])
			buf = buf[l:]
		}

	case OpCodeUnmarshalText:
		l, n := ReadUint32(buf)
		buf = buf[n:]
		if l > 0 {
			_ = buf[l-1]
			_ = rval.(encoding.TextUnmarshaler).UnmarshalText(buf[:l])
			buf = buf[l:]
		}
	}
	return buf
}

func readField(code OpCode, field *Field, ptr unsafe.Pointer, buf []byte, enums EnumRegistry) []byte {
	switch code {

	case OpCodeInt64, OpCodeUint64, OpCodeFloat64:
		_ = buf[7]
		*(*uint64)(ptr) = *(*uint64)(unsafe.Pointer(&buf[0]))
		buf = buf[8:]

	case OpCodeInt32, OpCodeUint32, OpCodeFloat32:
		_ = buf[3]
		*(*uint32)(ptr) = *(*uint32)(unsafe.Pointer(&buf[0]))
		buf = buf[4:]

	case OpCodeInt16, OpCodeUint16:
		_ = buf[1]
		*(*uint16)(ptr) = *(*uint16)(unsafe.Pointer(&buf[0]))
		buf = buf[2:]

	case OpCodeInt8, OpCodeUint8, OpCodeBool:
		_ = buf[0]
		*(*uint8)(ptr) = *(*uint8)(unsafe.Pointer(&buf[0]))
		buf = buf[1:]

	case OpCodeFixedArray:
		_ = buf[field.fixed-1]
		copy(unsafe.Slice((*byte)(ptr), field.fixed), buf[:field.fixed])
		buf = buf[field.fixed:]

	case OpCodeFixedString:
		_ = buf[field.fixed-1]
		*(*string)(ptr) = unsafe.String(unsafe.SliceData(buf), field.fixed)
		buf = buf[field.fixed:]

	case OpCodeFixedBytes:
		_ = buf[field.fixed-1]
		*(*[]byte)(ptr) = buf[:field.fixed]
		buf = buf[field.fixed:]

	case OpCodeString:
		l, n := ReadUint32(buf)
		buf = buf[n:]
		if l > 0 {
			_ = buf[l-1]
			*(*string)(ptr) = unsafe.String(unsafe.SliceData(buf), l)
			buf = buf[l:]
		}

	case OpCodeBytes:
		l, n := ReadUint32(buf)
		buf = buf[n:]
		if l > 0 {
			_ = buf[l-1]
			*(*[]byte)(ptr) = buf[:l]
			buf = buf[l:]
		}

	case OpCodeDateTime:
		ts, n := ReadInt64(buf)
		*(*time.Time)(ptr) = time.Unix(0, ts).UTC()
		buf = buf[n:]

	case OpCodeInt128:
		_ = buf[15]
		*(*num.Int128)(ptr) = num.Int128FromBytes(buf[:16])
		buf = buf[16:]

	case OpCodeInt256:
		_ = buf[31]
		*(*num.Int256)(ptr) = num.Int256FromBytes(buf[:32])
		buf = buf[32:]

	case OpCodeDecimal32:
		i32, n := ReadInt32(buf)
		(*(*num.Decimal32)(ptr)).Set(i32)
		(*(*num.Decimal32)(ptr)).SetScale(field.Scale())
		buf = buf[n:]

	case OpCodeDecimal64:
		i64, n := ReadInt64(buf)
		(*(*num.Decimal64)(ptr)).Set(i64)
		(*(*num.Decimal64)(ptr)).SetScale(field.scale)
		buf = buf[n:]

	case OpCodeDecimal128:
		_ = buf[15]
		(*(*num.Decimal128)(ptr)).Set(num.Int128FromBytes(buf[:16]))
		(*(*num.Decimal128)(ptr)).SetScale(field.scale)
		buf = buf[16:]

	case OpCodeDecimal256:
		_ = buf[31]
		(*(*num.Decimal256)(ptr)).Set(num.Int256FromBytes(buf[:32]))
		(*(*num.Decimal256)(ptr)).SetScale(field.scale)
		buf = buf[32:]

	case OpCodeEnum:
		u16, n := ReadUint16(buf)
		buf = buf[n:]
		enum, ok := enums.Lookup(field.name)
		if !ok {
			panic(fmt.Errorf("translation for enum %q not registered", field.name))
		}
		val, ok := enum.Value(u16)
		if !ok {
			panic(fmt.Errorf("%s: invalid enum value %d", field.name, u16))
		}
		*(*string)(ptr) = string(val) // FIXME: may break when enum dict grows
	}
	return buf
}
