// Copyright (c) 2020-2026 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc, alex@blockwatch.cc

package filter

import (
	"math"
	"unsafe"

	"blockwatch.cc/knoxdb/pkg/num"
	"blockwatch.cc/knoxdb/pkg/slicex"
	"blockwatch.cc/knoxdb/pkg/util"
	"github.com/zeebo/xxh3"
)

func Hash(buf []byte) uint64 {
	return xxh3.Hash(buf)
}

func HashUint8(v uint8) uint64 {
	return xxh3.Hash((*[1]byte)(unsafe.Pointer(&v))[:])
}

func HashInt8(v int8) uint64 {
	return xxh3.Hash((*[1]byte)(unsafe.Pointer(&v))[:])
}

func HashUint16(v uint16) uint64 {
	return xxh3.Hash((*[2]byte)(unsafe.Pointer(&v))[:])
}

func HashInt16(v int16) uint64 {
	return xxh3.Hash((*[2]byte)(unsafe.Pointer(&v))[:])
}

func HashUint32(v uint32) uint64 {
	return xxh3.Hash((*[4]byte)(unsafe.Pointer(&v))[:])
}

func HashInt32(v int32) uint64 {
	return xxh3.Hash((*[4]byte)(unsafe.Pointer(&v))[:])
}

func HashUint64(v uint64) uint64 {
	return xxh3.Hash((*[8]byte)(unsafe.Pointer(&v))[:])
}

func HashInt64(v int64) uint64 {
	return xxh3.Hash((*[8]byte)(unsafe.Pointer(&v))[:])
}

func HashFloat64(v float64) uint64 {
	u := math.Float64bits(v)
	return xxh3.Hash((*[8]byte)(unsafe.Pointer(&u))[:])
}

func HashFloat32(v float32) uint64 {
	u := math.Float32bits(v)
	return xxh3.Hash((*[4]byte)(unsafe.Pointer(&u))[:])
}

func HashInt128(v num.Int128) uint64 {
	return xxh3.Hash(v.Bytes())
}

func HashInt256(v num.Int256) uint64 {
	return xxh3.Hash(v.Bytes())
}

type Number interface {
	int64 | int32 | int16 | int8 | uint64 | uint32 | uint16 | uint8 | float64 | float32
}

func HashT[T Number](v T) uint64 {
	switch any(T(0)).(type) {
	case uint64:
		return HashUint64(uint64(v))
	case uint32:
		return HashUint32(uint32(v))
	case uint16:
		return HashUint16(uint16(v))
	case uint8:
		return HashUint8(uint8(v))
	case int64:
		return HashUint64(uint64(v))
	case int32:
		return HashUint32(uint32(v))
	case int16:
		return HashUint16(uint16(v))
	case int8:
		return HashUint8(uint8(v))
	case float64:
		return HashFloat64(float64(v))
	case float32:
		return HashFloat32(float32(v))
	default:
		return 0
	}
}

func HashMulti(src any) []uint64 {
	if src == nil {
		return nil
	}
	var res []uint64
	switch v := src.(type) {
	case [][]byte:
		res := make([]uint64, len(v))
		for i := range res {
			res[i] = Hash(v[i])
		}
	case []string:
		res = make([]uint64, len(v))
		for i := range res {
			res[i] = Hash(util.UnsafeGetBytes(v[i]))
		}
	case []uint64:
		res = make([]uint64, len(v))
		for i := range res {
			res[i] = HashUint64(v[i])
		}
	case []uint32:
		res = make([]uint64, len(v))
		for i := range res {
			res[i] = HashUint32(v[i])
		}
	case []uint16:
		res = make([]uint64, len(v))
		for i := range res {
			res[i] = HashUint16(v[i])
		}
	case []uint8:
		res = make([]uint64, len(v))
		for i := range res {
			res[i] = HashUint8(v[i])
		}
	case []int64:
		res = make([]uint64, len(v))
		for i := range res {
			res[i] = HashInt64(v[i])
		}
	case []int32:
		res = make([]uint64, len(v))
		for i := range res {
			res[i] = HashInt32(v[i])
		}
	case []int16:
		res = make([]uint64, len(v))
		for i := range res {
			res[i] = HashInt16(v[i])
		}
	case []int8:
		res = make([]uint64, len(v))
		for i := range res {
			res[i] = HashInt8(v[i])
		}
	case []float64:
		res = make([]uint64, len(v))
		for i := range res {
			res[i] = HashFloat64(v[i])
		}
	case []float32:
		res = make([]uint64, len(v))
		for i := range res {
			res[i] = HashFloat32(v[i])
		}
	case []bool:
		v = slicex.UniqueBools(v)
		res = make([]uint64, len(v))
		for i := range res {
			if v[i] {
				res[i] = HashUint8(1)
			} else {
				res[i] = HashUint8(0)
			}
		}
	case []num.Int256:
		res = make([]uint64, len(v))
		for i := range res {
			buf := v[i].Bytes32()
			res[i] = Hash(buf[:])
		}
	case []num.Int128:
		res = make([]uint64, len(v))
		for i := range res {
			buf := v[i].Bytes16()
			res[i] = Hash(buf[:])
		}
	}
	return res
}
