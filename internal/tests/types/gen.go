// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package tests

import (
	"bytes"
	"encoding/binary"
	"reflect"

	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/num"
	"golang.org/x/exp/constraints"
)

var Generators = []Generator{
	NumGenerator[int8]{},
	NumGenerator[int16]{},
	NumGenerator[int32]{},
	NumGenerator[int64]{},
	NumGenerator[uint8]{},
	NumGenerator[uint16]{},
	NumGenerator[uint32]{},
	NumGenerator[uint64]{},
	FloatGenerator[float32]{},
	FloatGenerator[float64]{},
	BytesGenerator{},
	BoolsGenerator{},
	Int128Generator{},
	Int256Generator{},
}

type Generator interface {
	Name() string
	Type() types.BlockType
	MakeValue(int) any
	MakeSlice(...int) any
}

// int, uint
var _ Generator = (*NumGenerator[int])(nil)

type NumGenerator[T constraints.Integer] struct{}

func (_ NumGenerator[T]) Type() types.BlockType {
	var t T
	switch reflect.TypeOf(t).Kind() {
	case reflect.Int64:
		return types.BlockInt64
	case reflect.Int32:
		return types.BlockInt32
	case reflect.Int16:
		return types.BlockInt16
	case reflect.Int8:
		return types.BlockInt8
	case reflect.Uint64:
		return types.BlockUint64
	case reflect.Uint32:
		return types.BlockUint32
	case reflect.Uint16:
		return types.BlockUint16
	case reflect.Uint8:
		return types.BlockUint8
	default:
		return 0
	}
}

func (_ NumGenerator[T]) Name() string {
	var t T
	return reflect.ValueOf(t).Type().String()
}

func (_ NumGenerator[T]) MakeValue(n int) any {
	return T(n)
}

func (_ NumGenerator[T]) MakeSlice(n ...int) any {
	s := make([]T, len(n))
	for i := range n {
		s[i] = T(n[i])
	}
	return s
}

// float
var _ Generator = (*FloatGenerator[float64])(nil)

type FloatGenerator[T constraints.Float] struct{}

func (_ FloatGenerator[T]) Type() types.BlockType {
	var t T
	switch reflect.TypeOf(t).Kind() {
	case reflect.Float64:
		return types.BlockFloat64
	case reflect.Float32:
		return types.BlockFloat32
	default:
		return 0
	}
}

func (_ FloatGenerator[T]) Name() string {
	var t T
	return reflect.ValueOf(t).Type().String()
}

func (_ FloatGenerator[T]) MakeValue(n int) any {
	return T(n) + T(0.5) // util.RandFloat64())
}

func (_ FloatGenerator[T]) MakeSlice(n ...int) any {
	s := make([]T, len(n))
	for i := range n {
		s[i] = T(n[i]) + T(0.5) // util.RandFloat64())
	}
	return s
}

// []byte
var _ Generator = (*BytesGenerator)(nil)

type BytesGenerator struct{}

func (_ BytesGenerator) Type() types.BlockType {
	return types.BlockBytes
}

func (_ BytesGenerator) Name() string {
	return "bytes"
}

func (_ BytesGenerator) MakeValue(n int) any {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], uint64(n))
	return b[:]
}

func (_ BytesGenerator) MakeSlice(n ...int) any {
	s := make([][]byte, len(n))
	for i := range n {
		var b [8]byte
		binary.BigEndian.PutUint64(b[:], uint64(n[i]))
		s[i] = bytes.Clone(b[:])
	}
	return s
}

// bool
var _ Generator = (*BoolsGenerator)(nil)

type BoolsGenerator struct{}

func (_ BoolsGenerator) Type() types.BlockType {
	return types.BlockBool
}

func (_ BoolsGenerator) Name() string {
	return "bool"
}

func (_ BoolsGenerator) MakeValue(n int) any {
	return n%2 == 0
}

func (_ BoolsGenerator) MakeSlice(n ...int) any {
	s := make([]bool, len(n))
	for i := range n {
		s[i] = n[i]%2 == 0
	}
	return s
}

// int128
var _ Generator = (*Int128Generator)(nil)

type Int128Generator struct{}

func (_ Int128Generator) Type() types.BlockType {
	return types.BlockInt128
}

func (_ Int128Generator) Name() string {
	return "i128"
}

func (_ Int128Generator) MakeValue(n int) any {
	return num.Int128FromInt64(int64(n))
}

func (_ Int128Generator) MakeSlice(n ...int) any {
	s := make([]num.Int128, len(n))
	for i := range n {
		s[i] = num.Int128FromInt64(int64(n[i]))
	}
	return s
}

// int256
var _ Generator = (*Int256Generator)(nil)

type Int256Generator struct{}

func (_ Int256Generator) Type() types.BlockType {
	return types.BlockInt256
}

func (_ Int256Generator) Name() string {
	return "i256"
}

func (_ Int256Generator) MakeValue(n int) any {
	return num.Int256FromInt64(int64(n))
}

func (_ Int256Generator) MakeSlice(n ...int) any {
	s := make([]num.Int256, len(n))
	for i := range n {
		s[i] = num.Int256FromInt64(int64(n[i]))
	}
	return s
}
