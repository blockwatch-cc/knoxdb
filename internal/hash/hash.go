// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package hash

import (
	"encoding/binary"
	"math"

	"blockwatch.cc/knoxdb/internal/hash/xxhash"
	"blockwatch.cc/knoxdb/internal/hash/xxhash32"
	"blockwatch.cc/knoxdb/pkg/num"
	"blockwatch.cc/knoxdb/pkg/util"
)

type HashValue [2]uint32

const XxHash32Seed = 1312

func Hash(data []byte) HashValue {
	return HashValue{xxhash32.Checksum(data, XxHash32Seed), xxhash32.Checksum(data, 0)}
}

func HashUint16(v uint16) HashValue {
	var buf [2]byte
	binary.LittleEndian.PutUint16(buf[:], v)
	return HashValue{
		xxhash32.Checksum(buf[:], XxHash32Seed),
		xxhash32.Checksum(buf[:], 0),
	}
}

func HashInt16(v int16) HashValue {
	var buf [2]byte
	binary.LittleEndian.PutUint16(buf[:], uint16(v))
	return HashValue{
		xxhash32.Checksum(buf[:], XxHash32Seed),
		xxhash32.Checksum(buf[:], 0),
	}
}

func HashUint32(v uint32) HashValue {
	return HashValue{
		xxhash.Hash32u32(v, XxHash32Seed),
		xxhash.Hash32u32(v, 0),
	}
}

func HashInt32(v int32) HashValue {
	return HashValue{
		xxhash.Hash32u32(uint32(v), XxHash32Seed),
		xxhash.Hash32u32(uint32(v), 0),
	}
}

func HashUint64(v uint64) HashValue {
	return HashValue{
		xxhash.Hash32u64(v, XxHash32Seed),
		xxhash.Hash32u64(v, 0),
	}
}

func HashInt64(v int64) HashValue {
	return HashValue{
		xxhash.Hash32u64(uint64(v), XxHash32Seed),
		xxhash.Hash32u64(uint64(v), 0),
	}
}

func HashFloat64(v float64) HashValue {
	u := math.Float64bits(v)
	return HashValue{
		xxhash.Hash32u64(u, XxHash32Seed),
		xxhash.Hash32u64(u, 0),
	}
}

func HashFloat32(v float32) HashValue {
	u := math.Float32bits(v)
	return HashValue{
		xxhash.Hash32u32(u, XxHash32Seed),
		xxhash.Hash32u32(u, 0),
	}
}

func HashInt128(v num.Int128) HashValue {
	buf := v.Bytes16()
	return Hash(buf[:])
}

func HashInt256(v num.Int256) HashValue {
	buf := v.Bytes32()
	return Hash(buf[:])
}

func HashAny(val any) HashValue {
	if val == nil {
		return HashValue{}
	}
	switch v := val.(type) {
	case []byte:
		return Hash(v)
	case string:
		return Hash(util.UnsafeGetBytes(v))
	case uint:
		return HashUint64(uint64(v))
	case uint64:
		return HashUint64(v)
	case uint32:
		return HashUint32(v)
	case uint16:
		return HashUint16(v)
	case uint8:
		return Hash([]byte{v})
	case int:
		return HashInt64(int64(v))
	case int64:
		return HashInt64(v)
	case int32:
		return HashInt32(v)
	case int16:
		return HashInt16(v)
	case int8:
		return Hash([]byte{uint8(v)})
	case float64:
		return HashFloat64(v)
	case float32:
		return HashFloat32(v)
	case bool:
		if v {
			return Hash([]byte{1})
		} else {
			return Hash([]byte{0})
		}
	case num.Int256:
		buf := v.Bytes32()
		return Hash(buf[:])
	case num.Int128:
		buf := v.Bytes16()
		return Hash(buf[:])
	default:
		return HashValue{}
	}
}

func HashAnySlice(val any) []HashValue {
	if val == nil {
		return nil
	}
	var res []HashValue
	switch v := val.(type) {
	case [][]byte:
		res = make([]HashValue, len(v))
		for i := range res {
			res[i] = Hash(v[i])
		}
	case []string:
		res = make([]HashValue, len(v))
		for i := range res {
			res[i] = Hash(util.UnsafeGetBytes(v[i]))
		}
	case []uint:
		res = make([]HashValue, len(v))
		for i := range res {
			res[i] = HashUint64(uint64(v[i]))
		}
	case []uint64:
		res = make([]HashValue, len(v))
		for i := range res {
			res[i] = HashUint64(v[i])
		}
	case []uint32:
		res = make([]HashValue, len(v))
		for i := range res {
			res[i] = HashUint32(v[i])
		}
	case []uint16:
		res = make([]HashValue, len(v))
		for i := range res {
			res[i] = HashUint16(v[i])
		}
	case []uint8:
		res = make([]HashValue, len(v))
		for i := range res {
			res[i] = Hash([]byte{v[i]})
		}
	case []int:
		res = make([]HashValue, len(v))
		for i := range res {
			res[i] = HashInt64(int64(v[i]))
		}
	case []int64:
		res = make([]HashValue, len(v))
		for i := range res {
			res[i] = HashInt64(v[i])
		}
	case []int32:
		res = make([]HashValue, len(v))
		for i := range res {
			res[i] = HashInt32(v[i])
		}
	case []int16:
		res = make([]HashValue, len(v))
		for i := range res {
			res[i] = HashInt16(v[i])
		}
	case []int8:
		res = make([]HashValue, len(v))
		for i := range res {
			res[i] = Hash([]byte{uint8(v[i])})
		}
	case []float64:
		res = make([]HashValue, len(v))
		for i := range res {
			res[i] = HashFloat64(v[i])
		}
	case []float32:
		res = make([]HashValue, len(v))
		for i := range res {
			res[i] = HashFloat32(v[i])
		}
	case []bool:
		res = make([]HashValue, len(v))
		h0, h1 := Hash([]byte{0}), Hash([]byte{1})
		for i := range res {
			if v[i] {
				res[i] = h0
			} else {
				res[i] = h1
			}
		}
	case []num.Int256:
		res = make([]HashValue, len(v))
		for i := range res {
			buf := v[i].Bytes32()
			res[i] = Hash(buf[:])
		}
	case []num.Int128:
		res = make([]HashValue, len(v))
		for i := range res {
			buf := v[i].Bytes16()
			res[i] = Hash(buf[:])
		}
	}
	return res
}

func (h HashValue) Uint64() uint64 {
	return uint64(h[0])<<32 | uint64(h[1])
}

func Uint64Values(h []HashValue) []uint64 {
	u64 := make([]uint64, len(h))
	for i, v := range h {
		u64[i] = v.Uint64()
	}
	return u64
}
