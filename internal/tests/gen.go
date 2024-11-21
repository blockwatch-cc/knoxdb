// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package tests

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"

	"blockwatch.cc/knoxdb/pkg/num"
	"blockwatch.cc/knoxdb/pkg/util"
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
	StringsGenerator{},
	BoolsGenerator{},
	Int128Generator{},
	Int256Generator{},
}

type Generator interface {
	MakeValue(int) any
	MakeSlice(...int) any
}

// int, uint
var _ Generator = (*NumGenerator[int])(nil)

type NumGenerator[T constraints.Integer] struct{}

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

func (_ FloatGenerator[T]) MakeValue(n int) any {
	return T(n) + T(util.RandFloat64())
}

func (_ FloatGenerator[T]) MakeSlice(n ...int) any {
	s := make([]T, len(n))
	for i := range n {
		s[i] = T(n[i]) + T(util.RandFloat64())
	}
	return s
}

// []byte
var _ Generator = (*BytesGenerator)(nil)

type BytesGenerator struct{}

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

// string
var _ Generator = (*StringsGenerator)(nil)

type StringsGenerator struct{}

func (_ StringsGenerator) MakeValue(n int) any {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], uint64(n))
	return hex.EncodeToString(b[:])
}

func (_ StringsGenerator) MakeSlice(n ...int) any {
	s := make([]string, len(n))
	for i := range n {
		var b [8]byte
		binary.BigEndian.PutUint64(b[:], uint64(n[i]))
		s[i] = hex.EncodeToString(b[:])
	}
	return s
}

// bool
var _ Generator = (*BoolsGenerator)(nil)

type BoolsGenerator struct{}

func (_ BoolsGenerator) MakeValue(n int) any {
	return n > 0
}

func (_ BoolsGenerator) MakeSlice(n ...int) any {
	s := make([]bool, len(n))
	for i := range n {
		s[i] = n[i] > 0
	}
	return s
}

// int128
var _ Generator = (*Int128Generator)(nil)

type Int128Generator struct{}

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
