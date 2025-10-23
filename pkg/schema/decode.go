// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package schema

import (
	"bytes"
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

func (d *GenericDecoder[T]) WithEnums(reg *EnumRegistry) *GenericDecoder[T] {
	d.dec.WithEnums(reg)
	return d
}

func (d *GenericDecoder[T]) Read(r io.Reader) (val *T, err error) {
	val = new(T)
	err = d.dec.Read(r, val)
	return
}

func (d *GenericDecoder[T]) Decode(buf []byte, val *T) (*T, error) {
	if val == nil {
		val = new(T)
	}
	d.dec.DecodePtr(buf, unsafe.Pointer(val))
	return val, nil
}

func (d *GenericDecoder[T]) DecodeSlice(buf []byte, res []T) ([]T, error) {
	if res == nil {
		// We slightly over-allocate the result slice when data contains
		// long strings/bytes, however this single allocation is still
		// much more performant than growing the slice multiple times.
		// For fixed-size schemas, a single allocation is all we need.
		res = make([]T, len(buf)/max(d.dec.schema.MinWireSize, 1))
	}
	var n int
	for n = range res {
		if len(buf) == 0 {
			break
		}
		buf = d.dec.DecodePtr(buf, unsafe.Pointer(&res[n]))
	}
	return res[:n], nil
}

type Decoder struct {
	schema *Schema
	enums  *EnumRegistry
	buf    *bytes.Buffer
}

func NewDecoder(s *Schema) *Decoder {
	enums := s.Enums
	if enums == nil {
		enums = &GlobalRegistry
	}
	return &Decoder{
		schema: s,
		enums:  enums,
		buf:    bytes.NewBuffer(make([]byte, 0, s.MaxWireSize)),
	}
}

func (d *Decoder) Schema() *Schema {
	return d.schema
}

func (d *Decoder) WithEnums(reg *EnumRegistry) *Decoder {
	d.enums = reg
	return d
}

// Read reads wire encoded data from r and decodes into a
// new heap allocated elemen of type T.
//
// When wire size is fixed we can read and decode in one step.
// Otherwise we take a slow path that reads variable length data
// as length fields are encountered. This requires multiple calls
// to the underlying reader.
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
	n, err := io.CopyN(d.buf, r, int64(d.schema.MinWireSize))
	if err != nil {
		return err
	}
	if n != int64(d.schema.MinWireSize) {
		return ErrShortBuffer
	}

	// fast path decode fixed size data
	if d.schema.IsFixedSize {
		return d.Decode(d.buf.Bytes(), val)
	}

	// slow path decode with additional read calls (may reallocate buffer!)
	if val == nil {
		return ErrNilValue
	}
	rval := reflect.Indirect(reflect.ValueOf(val))
	base := rval.Addr().UnsafePointer()

	for op, code := range d.schema.Decode {
		field := d.schema.Fields[op]
		ptr := unsafe.Add(base, field.Offset)
		switch code {
		default:
			// int, uint, float, bool
			_, err = d.buf.Read(unsafe.Slice((*byte)(ptr), field.Size))

		case OpCodeSkip:
			// noop

		case OpCodeFixedBytes:
			_, err = d.buf.Read(unsafe.Slice((*byte)(ptr), field.Fixed))

		case OpCodeFixedString:
			// explicit copy
			*(*string)(ptr) = string(d.buf.Next(int(field.Fixed)))

		case OpCodeString:
			l := LE.Uint32(d.buf.Next(4))
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
			l := LE.Uint32(d.buf.Next(4))
			n, err = io.CopyN(d.buf, r, int64(l)) // may realloc!
			if err != nil {
				return err
			}
			if n != int64(l) {
				return ErrShortBuffer
			}
			// explicit copy
			*(*[]byte)(ptr) = bytes.Clone(d.buf.Next(int(l)))

		case OpCodeTimestamp, OpCodeTime, OpCodeDate:
			ts := int64(LE.Uint64(d.buf.Next(8)))
			*(*time.Time)(ptr) = TimeScale(field.Scale).FromUnix(ts)

		case OpCodeInt128:
			*(*num.Int128)(ptr) = num.Int128FromBytes(d.buf.Next(16))

		case OpCodeInt256:
			*(*num.Int256)(ptr) = num.Int256FromBytes(d.buf.Next(32))

		case OpCodeDecimal32:
			(*(*num.Decimal32)(ptr)).Set(int32(LE.Uint32(d.buf.Next(4))))
			(*(*num.Decimal32)(ptr)).SetScale(field.Scale)

		case OpCodeDecimal64:
			(*(*num.Decimal64)(ptr)).Set(int64(LE.Uint64(d.buf.Next(8))))
			(*(*num.Decimal64)(ptr)).SetScale(field.Scale)

		case OpCodeDecimal128:
			(*(*num.Decimal128)(ptr)).Set(num.Int128FromBytes(d.buf.Next(16)))
			(*(*num.Decimal128)(ptr)).SetScale(field.Scale)

		case OpCodeDecimal256:
			(*(*num.Decimal256)(ptr)).Set(num.Int256FromBytes(d.buf.Next(32)))
			(*(*num.Decimal256)(ptr)).SetScale(field.Scale)

		case OpCodeEnum:
			u16 := LE.Uint16(d.buf.Next(2))
			if enum, ok := d.enums.Lookup(field.Name); ok {
				val, ok := enum.Value(u16)
				if !ok {
					err = fmt.Errorf("%s: invalid enum value %d", field.Name, u16)
				}
				*(*string)(ptr) = val
			} else {
				err = fmt.Errorf("translation for enum %q not registered", field.Name)
			}
		case OpCodeBigInt:
			// read as raw bytes and create num.Big
			l := LE.Uint32(d.buf.Next(4))
			n, err = io.CopyN(d.buf, r, int64(l)) // may realloc!
			if err != nil {
				return err
			}
			if n != int64(l) {
				return ErrShortBuffer
			}
			err = (*num.Big)(ptr).UnmarshalBinary(d.buf.Next(int(l)))
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
	d.DecodePtr(buf, base)
	return nil
}

func (d *Decoder) DecodePtr(buf []byte, base unsafe.Pointer) []byte {
	for op, code := range d.schema.Decode {
		if code == OpCodeSkip {
			continue
		}
		field := d.schema.Fields[op]
		ptr := unsafe.Add(base, field.Offset)
		buf = readField(code, field, ptr, buf, d.enums)
	}
	return buf
}

func (d *Decoder) DecodeSlice(buf []byte, slice any) (int, error) {
	if slice == nil {
		return 0, ErrNilValue
	}
	rslice := reflect.Indirect(reflect.ValueOf(slice))
	base := rslice.UnsafePointer()
	sz := rslice.Type().Elem().Size()
	num := rslice.Len()

	var i int
	for i = 0; i < num && len(buf) > 0; i++ {
		for op, code := range d.schema.Decode {
			if code == OpCodeSkip {
				continue
			}
			field := d.schema.Fields[op]
			ptr := unsafe.Add(base, field.Offset)
			buf = readField(code, field, ptr, buf, d.enums)
		}
		base = unsafe.Add(base, sz)
	}
	return i, nil
}

func readField(code OpCode, field *Field, ptr unsafe.Pointer, buf []byte, enums *EnumRegistry) []byte {
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

	case OpCodeFixedBytes:
		_ = buf[field.Fixed-1]
		copy(unsafe.Slice((*byte)(ptr), field.Fixed), buf[:field.Fixed])
		buf = buf[field.Fixed:]

	case OpCodeFixedString:
		_ = buf[field.Fixed-1]
		*(*string)(ptr) = unsafe.String(unsafe.SliceData(buf), field.Fixed)
		buf = buf[field.Fixed:]

	case OpCodeString:
		l := LE.Uint32(buf)
		buf = buf[4:]
		if l > 0 {
			_ = buf[l-1]
			*(*string)(ptr) = unsafe.String(unsafe.SliceData(buf), l)
			buf = buf[l:]
		}

	case OpCodeBytes:
		l := LE.Uint32(buf)
		buf = buf[4:]
		if l > 0 {
			_ = buf[l-1]
			*(*[]byte)(ptr) = buf[:l]
			buf = buf[l:]
		}

	case OpCodeTimestamp, OpCodeTime, OpCodeDate:
		ts := int64(LE.Uint64(buf))
		*(*time.Time)(ptr) = TimeScale(field.Scale).FromUnix(ts)
		buf = buf[8:]

	case OpCodeInt128:
		_ = buf[15]
		*(*num.Int128)(ptr) = num.Int128FromBytes(buf[:16])
		buf = buf[16:]

	case OpCodeInt256:
		_ = buf[31]
		*(*num.Int256)(ptr) = num.Int256FromBytes(buf[:32])
		buf = buf[32:]

	case OpCodeDecimal32:
		(*(*num.Decimal32)(ptr)).Set(int32(LE.Uint32(buf)))
		(*(*num.Decimal32)(ptr)).SetScale(field.Scale)
		buf = buf[4:]

	case OpCodeDecimal64:
		(*(*num.Decimal64)(ptr)).Set(int64(LE.Uint64(buf)))
		(*(*num.Decimal64)(ptr)).SetScale(field.Scale)
		buf = buf[8:]

	case OpCodeDecimal128:
		_ = buf[15]
		(*(*num.Decimal128)(ptr)).Set(num.Int128FromBytes(buf[:16]))
		(*(*num.Decimal128)(ptr)).SetScale(field.Scale)
		buf = buf[16:]

	case OpCodeDecimal256:
		_ = buf[31]
		(*(*num.Decimal256)(ptr)).Set(num.Int256FromBytes(buf[:32]))
		(*(*num.Decimal256)(ptr)).SetScale(field.Scale)
		buf = buf[32:]

	case OpCodeEnum:
		if enums == nil {
			panic(fmt.Errorf("nil enum registry when decoding enum %q", field.Name))
		}
		u16 := LE.Uint16(buf)
		buf = buf[2:]
		enum, ok := enums.Lookup(field.Name)
		if !ok {
			panic(fmt.Errorf("translation for enum %q not registered", field.Name))
		}
		val, ok := enum.Value(u16)
		if !ok {
			panic(fmt.Errorf("%s: invalid enum value %d, have %#v", field.Name, u16, enum))
		}
		*(*string)(ptr) = val // FIXME: may break when enum dict grows

	case OpCodeBigInt:
		l := LE.Uint32(buf)
		buf = buf[4:]
		if l > 0 {
			_ = buf[l-1]
			_ = (*num.Big)(ptr).UnmarshalBinary(buf[:l])
			buf = buf[l:]
		}
	}
	return buf
}
