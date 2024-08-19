// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package schema

import (
	"bytes"
	"encoding"
	"fmt"
	"reflect"
	"strconv"
	"time"
	"unsafe"

	"blockwatch.cc/knoxdb/pkg/num"
)

type OpCode byte

const (
	OpCodeInvalid         OpCode = iota // 0x0  0
	OpCodeInt8                          // 0x1  1
	OpCodeInt16                         // 0x2  2
	OpCodeInt32                         // 0x3  3
	OpCodeInt64                         // 0x4  4
	OpCodeUint8                         // 0x5  5
	OpCodeUint16                        // 0x6  6
	OpCodeUint32                        // 0x7  7
	OpCodeUint64                        // 0x8  8
	OpCodeFloat32                       // 0x9  9
	OpCodeFloat64                       // 0xA  10
	OpCodeBool                          // 0xB  11
	OpCodeFixedArray                    // 0xC  12
	OpCodeFixedString                   // 0xD  13
	OpCodeFixedBytes                    // 0xE  14
	OpCodeString                        // 0xF  15
	OpCodeBytes                         // 0x10 16
	OpCodeDateTime                      // 0x11 17
	OpCodeInt128                        // 0x12 18
	OpCodeInt256                        // 0x13 19
	OpCodeDecimal32                     // 0x14 20
	OpCodeDecimal64                     // 0x15 21
	OpCodeDecimal128                    // 0x16 22
	OpCodeDecimal256                    // 0x17 23
	OpCodeMarshalBinary                 // 0x18 24
	OpCodeMarshalText                   // 0x19 25
	OpCodeStringer                      // 0x1A 26
	OpCodeUnmarshalBinary               // 0x1B 27
	OpCodeUnmarshalText                 // 0x1C 28
)

var (
	opCodeStrings = "_i8_i16_i32_i64_u8_u16_u32_u64_f32_f64_bool_fixarr_fixstr_fixbyte_str_byte_dtime_i128_i256_d32_d64_d128_d256_mshbin_mshtxt_mshstr_ushbin_ushtxt"
	opCodeIdx     = [...][2]int{
		{0, 1},                            // invalid
		{1, 3}, {4, 7}, {8, 11}, {12, 15}, // int
		{16, 18}, {19, 22}, {23, 26}, {27, 30}, // uint
		{31, 34}, {35, 38}, // float
		{39, 43},                     // bool
		{44, 50}, {51, 57}, {58, 65}, // fixed
		{66, 69}, {70, 74}, // string, bytes
		{75, 80},           // datetime
		{81, 85}, {86, 90}, // i128/256
		{91, 94}, {95, 98}, {99, 103}, {104, 108}, // decimals
		{109, 115}, {116, 122}, {123, 129}, // marshalers
		{130, 136}, {137, 143}, // unmarshalers
	}
)

func (c OpCode) String() string {
	if int(c) >= len(opCodeIdx) {
		return "opcode_" + strconv.Itoa(int(c))
	}
	idx := opCodeIdx[c]
	return opCodeStrings[idx[0]:idx[1]]
}

func (c OpCode) NeedsInterface() bool {
	return c >= OpCodeMarshalBinary
}

type Encoder[T any] struct {
	schema  *Schema
	needsif bool
}

func NewEncoder[T any]() *Encoder[T] {
	s, err := GenericSchema[T]()
	if err != nil {
		panic(err)
	}
	var needsif bool
	for _, c := range s.encode {
		if c.NeedsInterface() {
			needsif = true
			break
		}
	}
	return &Encoder[T]{
		schema:  s,
		needsif: needsif,
	}
}

func (e *Encoder[T]) Schema() *Schema {
	return e.schema
}

func (e *Encoder[T]) NewBuffer(sz int) *bytes.Buffer {
	return e.schema.NewBuffer(sz)
}

func (e *Encoder[T]) Encode(buf *bytes.Buffer, val T) {
	e.EncodePtr(buf, &val)
}

func (e *Encoder[T]) EncodePtr(buf *bytes.Buffer, val *T) {
	base := unsafe.Pointer(val)
	if e.needsif {
		rval := reflect.Indirect(reflect.ValueOf(val))
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

func (e *Encoder[T]) EncodeSlice(buf *bytes.Buffer, slice []T) {
	if !e.needsif {
		for i := range slice {
			base := unsafe.Pointer(&slice[i])
			for op, code := range e.schema.encode {
				field := e.schema.fields[op]
				ptr := unsafe.Add(base, field.offset)
				writeField(buf, code, field, ptr)
			}
		}
	} else {
		for i := range slice {
			base := unsafe.Pointer(&slice[i])
			rval := reflect.ValueOf(slice[i])
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
	}
}

func (e *Encoder[T]) EncodePtrSlice(buf *bytes.Buffer, slice []*T) {
	if e.needsif {
		for i := range slice {
			base := unsafe.Pointer(slice[i])
			rval := reflect.Indirect(reflect.ValueOf(slice[i]))
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
		for i := range slice {
			base := unsafe.Pointer(slice[i])
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
