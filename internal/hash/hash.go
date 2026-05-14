// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package hash

import (
	"math"
	"unsafe"

	"blockwatch.cc/knoxdb/pkg/num"
	"blockwatch.cc/knoxdb/pkg/util"
	"github.com/zeebo/xxh3"
)

type Hasher interface {
	Size() int
	BlockSize() int
	Reset()
	Sum(b []byte) []byte
	Sum64() uint64
	Write([]byte) (int, error)
	WriteString(string) (int, error)
}

var (
	Hash   = xxh3.Hash       // []byte
	Vec8   = xxh3_u8_purego  // []uint8
	Vec16  = xxh3_u16_purego // []uint16
	Vec32  = xxh3_u32_purego // []uint32, AVX2, AVX512
	Vec64  = xxh3_u64_purego // []uint64, AVX2, AVX512
	Uint64 = xxh3_u64
	Uint32 = xxh3_u32
	Uint16 = xxh3_u16
	Uint8  = xxh3_u8

	Zero = Hash([]byte{0})
	One  = Hash([]byte{1})
)

func New() Hasher {
	var h xxh3.Hasher
	return &h
}

func Float64(v float64) uint64 {
	u := math.Float64bits(v)
	return Hash((*[8]byte)(unsafe.Pointer(&u))[:])
}

func Float32(v float32) uint64 {
	u := math.Float32bits(v)
	return Hash((*[4]byte)(unsafe.Pointer(&u))[:])
}

func Int128(v num.Int128) uint64 {
	return Hash(v.Bytes())
}

func Int256(v num.Int256) uint64 {
	return Hash(v.Bytes())
}

type Number interface {
	int64 | int32 | int16 | int8 | uint64 | uint32 | uint16 | uint8 | float64 | float32
}

func HashT[T Number](v T) uint64 {
	switch any(T(0)).(type) {
	case uint64:
		return Uint64(uint64(v))
	case uint32:
		return Uint32(uint32(v))
	case uint16:
		return Uint16(uint16(v))
	case uint8:
		return Uint8(uint8(v))
	case int64:
		return Uint64(uint64(v))
	case int32:
		return Uint32(uint32(v))
	case int16:
		return Uint16(uint16(v))
	case int8:
		return Uint8(uint8(v))
	case float64:
		return Float64(float64(v))
	case float32:
		return Float32(float32(v))
	default:
		return 0
	}
}

func makeSlice(dst []uint64, n int) []uint64 {
	if cap(dst) <= n {
		return make([]uint64, n)
	} else {
		return dst[:n]
	}
}

func Vec(src any, dst []uint64) []uint64 {
	if src == nil {
		if dst == nil {
			return nil
		}
		return dst[:0]
	}
	var res []uint64
	switch v := src.(type) {
	case [][]byte:
		res = makeSlice(dst, len(v))
		for i := range res {
			res[i] = Hash(v[i])
		}
	case []string:
		res = makeSlice(dst, len(v))
		for i := range res {
			res[i] = Hash(util.UnsafeGetBytes(v[i]))
		}
	case []uint64:
		res = Vec64(v, makeSlice(dst, len(v)))
	case []uint32:
		res = Vec32(v, makeSlice(dst, len(v)))
	case []uint16:
		res = Vec16(v, makeSlice(dst, len(v)))
	case []uint8:
		res = Vec8(v, makeSlice(dst, len(v)))
	case []int64:
		res = Vec64(util.ReinterpretSlice[int64, uint64](v), makeSlice(dst, len(v)))
	case []int32:
		res = Vec32(util.ReinterpretSlice[int32, uint32](v), makeSlice(dst, len(v)))
	case []int16:
		res = Vec16(util.ReinterpretSlice[int16, uint16](v), makeSlice(dst, len(v)))
	case []int8:
		res = Vec8(util.ReinterpretSlice[int8, uint8](v), makeSlice(dst, len(v)))
	case []float64:
		res = Vec64(util.ReinterpretSlice[float64, uint64](v), makeSlice(dst, len(v)))
	case []float32:
		res = Vec32(util.ReinterpretSlice[float32, uint32](v), makeSlice(dst, len(v)))
	case []bool:
		res = makeSlice(dst, len(v))
		for i := range res {
			if v[i] {
				res[i] = One
			} else {
				res[i] = Zero
			}
		}
	case []num.Int256:
		res = makeSlice(dst, len(v))
		for i := range res {
			res[i] = Hash(v[i].Bytes())
		}
	case []num.Int128:
		res = makeSlice(dst, len(v))
		for i := range res {
			res[i] = Hash(v[i].Bytes())
		}
	}
	return res
}
