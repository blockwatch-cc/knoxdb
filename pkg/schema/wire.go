// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package schema

import (
	"encoding"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"reflect"
)

// Type cast while encoding to wire format. This accepts all
// integer types as source an will convert them to the
// wire format selected by code.
func EncodeInt(w io.Writer, code OpCode, val any, layout binary.ByteOrder) (err error) {
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
	err = ErrInvalidValueType
	switch code {
	case OpCodeInt8, OpCodeUint8:
		_, err = w.Write([]byte{uint8(u64)})
	case OpCodeInt16, OpCodeUint16:
		var buf [2]byte
		layout.PutUint16(buf[:], uint16(u64))
		_, err = w.Write(buf[:])
	case OpCodeInt32, OpCodeUint32:
		var buf [4]byte
		layout.PutUint32(buf[:], uint32(u64))
		_, err = w.Write(buf[:])
	case OpCodeInt64, OpCodeUint64:
		var buf [8]byte
		layout.PutUint64(buf[:], u64)
		_, err = w.Write(buf[:])
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
		var b [4]byte
		LE.PutUint32(b[:], math.Float32bits(float32(f64)))
		_, err = w.Write(b[:])
	case OpCodeFloat64:
		var b [8]byte
		LE.PutUint64(b[:], math.Float64bits(f64))
		_, err = w.Write(b[:])
	}
	return
}

func EncodeBytes(w io.Writer, val any, fixed uint16, layout binary.ByteOrder) (err error) {
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
		var buf [4]byte
		layout.PutUint32(buf[:], uint32(len(b)))
		_, err = w.Write(buf[:])
		if err == nil {
			_, err = w.Write(b)
		}
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
