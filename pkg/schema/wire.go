// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package schema

import (
	"encoding"
	"fmt"
	"io"
	"math"
	"reflect"
	"unsafe"
)

func Uint64Bytes(v uint64) []byte {
	var buf [8]byte
	*(*uint64)(unsafe.Pointer(&buf[0])) = v
	return buf[:]
}

func Uint32Bytes(v uint32) []byte {
	var buf [4]byte
	*(*uint32)(unsafe.Pointer(&buf[0])) = v
	return buf[:]
}

func Uint16Bytes(v uint16) []byte {
	var buf [2]byte
	*(*uint16)(unsafe.Pointer(&buf[0])) = v
	return buf[:]
}

func Uint8Bytes(v uint8) []byte {
	var buf [1]byte
	*(*uint8)(unsafe.Pointer(&buf[0])) = v
	return buf[:]
}

func ReadInt64(buf []byte) (int64, int) {
	_ = buf[7]
	return *(*int64)(unsafe.Pointer(&buf[0])), 8
}

func ReadInt32(buf []byte) (int32, int) {
	_ = buf[3]
	return *(*int32)(unsafe.Pointer(&buf[0])), 4
}

func ReadInt16(buf []byte) (int16, int) {
	_ = buf[1]
	return *(*int16)(unsafe.Pointer(&buf[0])), 2
}

func ReadInt8(buf []byte) (int8, int) {
	_ = buf[0]
	return *(*int8)(unsafe.Pointer(&buf[0])), 1
}

func ReadUint64(buf []byte) (uint64, int) {
	_ = buf[7]
	return *(*uint64)(unsafe.Pointer(&buf[0])), 8
}

func ReadUint32(buf []byte) (uint32, int) {
	_ = buf[3]
	return *(*uint32)(unsafe.Pointer(&buf[0])), 4
}

func ReadUint16(buf []byte) (uint16, int) {
	_ = buf[1]
	return *(*uint16)(unsafe.Pointer(&buf[0])), 2
}

func ReadUint8(buf []byte) (uint8, int) {
	_ = buf[0]
	return *(*uint8)(unsafe.Pointer(&buf[0])), 1
}

// Type cast while encoding to wire format. This accepts all
// integer types as source an will convert them to the
// wire format selected by code.
func EncodeInt(w io.Writer, code OpCode, val any) (err error) {
	var u64 uint64
	switch v := val.(type) {
	case int:
		u64 = uint64(v)
	case int8:
		u64 = uint64(v)
	case int16:
		u64 = uint64(v)
	case int32:
		u64 = uint64(v)
	case int64:
		u64 = uint64(v)
	case uint:
		u64 = uint64(v)
	case uint8:
		u64 = uint64(v)
	case uint16:
		u64 = uint64(v)
	case uint32:
		u64 = uint64(v)
	case uint64:
		u64 = v
	default:
		return ErrInvalidValueType
	}
	switch code {
	case OpCodeInt8, OpCodeUint8:
		_, err = w.Write(Uint8Bytes(uint8(u64)))
	case OpCodeInt16, OpCodeUint16:
		_, err = w.Write(Uint16Bytes(uint16(u64)))
	case OpCodeInt32, OpCodeUint32:
		_, err = w.Write(Uint32Bytes(uint32(u64)))
	case OpCodeInt64, OpCodeUint64:
		_, err = w.Write(Uint64Bytes(u64))
	}
	return
}

// Type cast while encoding to wire format. This accepts all
// float types as source an will convert them to the
// wire format selected by code.
func EncodeFloat(w io.Writer, code OpCode, val any) (err error) {
	var f64 float64
	switch v := val.(type) {
	case float64:
		f64 = v
	case float32:
		f64 = float64(v)
	default:
		return ErrInvalidValueType
	}
	switch code {
	case OpCodeFloat32:
		_, err = w.Write(Uint32Bytes(math.Float32bits(float32(f64))))
	case OpCodeFloat64:
		_, err = w.Write(Uint64Bytes(math.Float64bits(f64)))
	}
	return
}

func EncodeBytes(w io.Writer, val any, fixed uint16) (err error) {
	var b []byte
	// type cast values
	switch v := val.(type) {
	case encoding.BinaryMarshaler:
		b, err = v.MarshalBinary()

	case encoding.TextMarshaler:
		b, err = v.MarshalText()

	case fmt.Stringer:
		b = []byte(v.String())

	case string:
		b = []byte(v)

	case []byte:
		b = v

	default:
		// use reflect for array types
		rv := reflect.Indirect(reflect.ValueOf(val))
		if rv.Type().Kind() == reflect.Array && rv.Type().Elem().Kind() == reflect.Uint8 {
			b = rv.Bytes()
		} else {
			err = ErrInvalidValueType
		}
	}
	if err != nil {
		return
	}

	// handle fixed values
	if fixed > 0 {
		if len(b) < int(fixed) {
			return ErrShortValue
		}
		_, err = w.Write(b[:fixed])
	} else {
		_, err = w.Write(b)
	}

	return
}

func EncodeBool(w io.Writer, b bool) (err error) {
	if b {
		_, err = w.Write([]byte{1})
	} else {
		_, err = w.Write([]byte{0})
	}
	return
}
