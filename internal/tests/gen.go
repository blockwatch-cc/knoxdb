// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package tests

import (
	"bytes"
	"encoding/binary"
	"math"
	"reflect"

	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/num"
	"blockwatch.cc/knoxdb/pkg/util"
)

const BENCH_WIDTH = 60

var (
	f64 = math.Float64frombits
	f32 = math.Float32frombits
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
var _ Generator = (*NumGenerator[int32])(nil)

type NumGenerator[T types.Integer] struct{}

func (_ NumGenerator[T]) Type() types.BlockType {
	switch any(T(0)).(type) {
	case int64:
		return types.BlockInt64
	case int32:
		return types.BlockInt32
	case int16:
		return types.BlockInt16
	case int8:
		return types.BlockInt8
	case uint64:
		return types.BlockUint64
	case uint32:
		return types.BlockUint32
	case uint16:
		return types.BlockUint16
	case uint8:
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

type FloatGenerator[T types.Float] struct{}

func (_ FloatGenerator[T]) Type() types.BlockType {
	switch any(T(0)).(type) {
	case float64:
		return types.BlockFloat64
	case float32:
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

// Generic Generator Functions

// creates n sequential values
func GenSeq[T types.Number](n int) []T {
	res := make([]T, n)
	for i := range res {
		res[i] = T(i)
	}
	return res
}

func GenRange[T types.Number](start, end T) []T {
	result := make([]T, int(end-start))
	for i := range result {
		result[i] = start + T(i)
	}
	return result
}

// creates n constants of value v
func GenConst[T types.Number](n int, v T) []T {
	res := make([]T, n)
	for i := range res {
		res[i] = v
	}
	return res
}

// creates n random values
func GenRnd[T types.Number](n int) []T {
	var res []T
	switch any(T(0)).(type) {
	case int64:
		res = util.ReinterpretSlice[int64, T](util.RandIntsn[int64](n, 1<<BENCH_WIDTH-1))
	case int32:
		res = util.ReinterpretSlice[int32, T](util.RandIntsn[int32](n, 1<<(BENCH_WIDTH/2-1)))
	case int16:
		res = util.ReinterpretSlice[int16, T](util.RandInts[int16](n))
	case int8:
		res = util.ReinterpretSlice[int8, T](util.RandInts[int8](n))
	case uint64:
		res = util.ReinterpretSlice[uint64, T](util.RandUintsn[uint64](n, 1<<BENCH_WIDTH-1))
	case uint32:
		res = util.ReinterpretSlice[uint32, T](util.RandUintsn[uint32](n, 1<<(BENCH_WIDTH/2-1)))
	case uint16:
		res = util.ReinterpretSlice[uint16, T](util.RandUints[uint16](n))
	case uint8:
		res = util.ReinterpretSlice[uint8, T](util.RandUints[uint8](n))
	case float64:
		res = util.ReinterpretSlice[float64, T](util.RandFloatsn[float64](n, 1<<BENCH_WIDTH-1))
	case float32:
		res = util.ReinterpretSlice[float32, T](util.RandFloatsn[float32](n, 1<<(BENCH_WIDTH/2)-1))
	}
	return res
}

// creates n random values with bit width of up to w
func GenRndBits[T types.Number](n, w int) []T {
	var res []T
	switch any(T(0)).(type) {
	case int64:
		res = util.ReinterpretSlice[int64, T](util.RandIntsn[int64](n, 1<<w-1))
	case int32:
		res = util.ReinterpretSlice[int32, T](util.RandIntsn[int32](n, 1<<w-1))
	case int16:
		res = util.ReinterpretSlice[int16, T](util.RandIntsn[int16](n, 1<<w-1))
	case int8:
		res = util.ReinterpretSlice[int8, T](util.RandIntsn[int8](n, 1<<w-1))
	case uint64:
		res = util.ReinterpretSlice[uint64, T](util.RandUintsn[uint64](n, 1<<w-1))
	case uint32:
		res = util.ReinterpretSlice[uint32, T](util.RandUintsn[uint32](n, 1<<w-1))
	case uint16:
		res = util.ReinterpretSlice[uint16, T](util.RandUintsn[uint16](n, 1<<w-1))
	case uint8:
		res = util.ReinterpretSlice[uint8, T](util.RandUintsn[uint8](n, 1<<w-1))
	case float64:
		res = util.ReinterpretSlice[float64, T](util.RandFloatsn[float64](n, f64(1<<w-1)))
	case float32:
		res = util.ReinterpretSlice[float32, T](util.RandFloatsn[float32](n, f32(1<<w-1)))
	}
	return res
}

// creates n values with cardinality c (i.e. u unique values)
func GenDups[T types.Number](n, u int) []T {
	c := max(n/u, 1)
	res := make([]T, n)
	switch any(T(0)).(type) {
	case int64:
		unique := util.RandIntsn[int64](c, 1<<BENCH_WIDTH-1)
		for i := range res {
			res[i] = T(unique[util.RandIntn(c)])
		}
	case int32:
		unique := util.RandIntsn[int32](c, 1<<(BENCH_WIDTH/2-1))
		for i := range res {
			res[i] = T(unique[util.RandIntn(c)])
		}
	case int16:
		unique := util.RandInts[int16](c)
		for i := range res {
			res[i] = T(unique[util.RandIntn(c)])
		}
	case int8:
		unique := util.RandInts[int8](c)
		for i := range res {
			res[i] = T(unique[util.RandIntn(c)])
		}
	case uint64:
		unique := util.RandUintsn[uint64](c, 1<<BENCH_WIDTH-1)
		for i := range res {
			res[i] = T(unique[util.RandIntn(c)])
		}
	case uint32:
		unique := util.RandUintsn[uint32](c, 1<<(BENCH_WIDTH/2-1))
		for i := range res {
			res[i] = T(unique[util.RandIntn(c)])
		}
	case uint16:
		unique := util.RandUints[uint16](c)
		for i := range res {
			res[i] = T(unique[util.RandIntn(c)])
		}
	case uint8:
		unique := util.RandUints[uint8](c)
		for i := range res {
			res[i] = T(unique[util.RandIntn(c)])
		}
	case float64:
		unique := util.RandFloatsn[float64](c, 1<<BENCH_WIDTH-1)
		for i := range res {
			res[i] = T(unique[util.RandIntn(c)])
		}
	case float32:
		unique := util.RandFloatsn[float32](c, 1<<(BENCH_WIDTH/2)-1)
		for i := range res {
			res[i] = T(unique[util.RandIntn(c)])
		}
	}
	return res
}

// creates n values with run length r
func GenRuns[T types.Number](n, r int) []T {
	res := make([]T, 0, n)
	sz := (n + r - 1) / r
	switch any(T(0)).(type) {
	case int64:
		for _, v := range util.RandIntsn[int64](sz, 1<<BENCH_WIDTH-1) {
			for range r {
				if len(res) == n {
					break
				}
				res = append(res, T(v))
			}
		}
	case int32:
		for _, v := range util.RandIntsn[int32](sz, 1<<(BENCH_WIDTH/2-1)) {
			for range r {
				if len(res) == n {
					break
				}
				res = append(res, T(v))
			}
		}
	case int16:
		for _, v := range util.RandInts[int16](sz) {
			for range r {
				if len(res) == n {
					break
				}
				res = append(res, T(v))
			}
		}
	case int8:
		for _, v := range util.RandInts[int8](sz) {
			for range r {
				if len(res) == n {
					break
				}
				res = append(res, T(v))
			}
		}
	case uint64:
		for _, v := range util.RandUintsn[uint64](sz, 1<<BENCH_WIDTH-1) {
			for range r {
				if len(res) == n {
					break
				}
				res = append(res, T(v))
			}
		}
	case uint32:
		for _, v := range util.RandUintsn[uint32](sz, 1<<(BENCH_WIDTH/2-1)) {
			for range r {
				if len(res) == n {
					break
				}
				res = append(res, T(v))
			}
		}
	case uint16:
		for _, v := range util.RandUints[uint16](sz) {
			for range r {
				if len(res) == n {
					break
				}
				res = append(res, T(v))
			}
		}
	case uint8:
		for _, v := range util.RandUints[uint8](sz) {
			for range r {
				if len(res) == n {
					break
				}
				res = append(res, T(v))
			}
		}
	case float64:
		for _, v := range util.RandFloatsn[float64](sz, 1<<BENCH_WIDTH-1) {
			for range r {
				if len(res) == n {
					break
				}
				res = append(res, T(v))
			}
		}
	case float32:
		for _, v := range util.RandFloatsn[float32](sz, 1<<(BENCH_WIDTH/2)-1) {
			for range r {
				if len(res) == n {
					break
				}
				res = append(res, T(v))
			}
		}
	}
	return res
}

// creates n values with u% values equal to x
func GenEqual[T types.Number](n, u int) ([]T, T) {
	res := make([]T, n)
	var x T
	switch any(T(0)).(type) {
	case int64:
		x = T(util.RandInt64n(1<<BENCH_WIDTH - 1))
		for i := range res {
			if util.RandIntn(100) <= u {
				res[i] = x
			} else {
				res[i] = T(util.RandInt64n(1<<BENCH_WIDTH - 1))
			}
		}
	case int32:
		x = T(util.RandInt32n(1<<(BENCH_WIDTH/2) - 1))
		for i := range res {
			if util.RandIntn(100) <= u {
				res[i] = x
			} else {
				res[i] = T(util.RandInt32n(1<<(BENCH_WIDTH/2) - 1))
			}
		}
	case int16:
		x = T(util.RandInt64n(1<<16 - 1))
		for i := range res {
			if util.RandIntn(100) <= u {
				res[i] = x
			} else {
				res[i] = T(util.RandInt64n(1<<16 - 1))
			}
		}
	case int8:
		x = T(util.RandInt64n(1<<8 - 1))
		for i := range res {
			if util.RandIntn(100) <= u {
				res[i] = x
			} else {
				res[i] = T(util.RandInt64n(1<<8 - 1))
			}
		}
	case uint64:
		x = T(util.RandUint64n(1<<BENCH_WIDTH - 1))
		for i := range res {
			if util.RandIntn(100) <= u {
				res[i] = x
			} else {
				res[i] = T(util.RandUint64n(1<<BENCH_WIDTH - 1))
			}
		}
	case uint32:
		x = T(util.RandUint32n(1<<(BENCH_WIDTH/2) - 1))
		for i := range res {
			if util.RandIntn(100) <= u {
				res[i] = x
			} else {
				res[i] = T(util.RandUint32n(1<<(BENCH_WIDTH/2) - 1))
			}
		}
	case uint16:
		x = T(util.RandUint64n(1<<16 - 1))
		for i := range res {
			if util.RandIntn(100) <= u {
				res[i] = x
			} else {
				res[i] = T(util.RandUint64n(1<<16 - 1))
			}
		}
	case uint8:
		x = T(util.RandUint64n(1<<8 - 1))
		for i := range res {
			if util.RandIntn(100) <= u {
				res[i] = x
			} else {
				res[i] = T(util.RandUint64n(1<<8 - 1))
			}
		}
	case float64:
		x = T(util.RandFloat64() * float64(1<<BENCH_WIDTH-1))
		for i := range res {
			if util.RandIntn(100) <= u {
				res[i] = x
			} else {
				res[i] = T(util.RandFloat64() * float64(1<<BENCH_WIDTH-1))
			}
		}
	case float32:
		x = T(util.RandFloat32() * float32(1<<(BENCH_WIDTH/2)-1))
		for i := range res {
			if util.RandIntn(100) <= u {
				res[i] = x
			} else {
				res[i] = T(util.RandFloat32() * float32(1<<(BENCH_WIDTH/2)-1))
			}
		}
	}
	return res, x
}
