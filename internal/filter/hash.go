// Copyright (c) 2020-2025 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc, alex@blockwatch.cc

package filter

import (
    "math"

    "blockwatch.cc/knoxdb/internal/hash/xxhash"
    "blockwatch.cc/knoxdb/internal/types"
    "blockwatch.cc/knoxdb/pkg/num"
    "blockwatch.cc/knoxdb/pkg/slicex"
    "blockwatch.cc/knoxdb/pkg/util"
)

const XxHash32Seed = 1312

type HashValue [2]uint32

func (h HashValue) Uint64() uint64 {
    return uint64(h[0])<<32 | uint64(h[1])
}

func Hash(buf []byte) HashValue {
    return HashValue{
        xxhash.Sum32(buf, XxHash32Seed),
        xxhash.Sum32(buf, 0),
    }
}

func HashUint8(v uint8) HashValue {
    return HashValue{
        xxhash.Hash32u32(uint32(v), XxHash32Seed),
        xxhash.Hash32u32(uint32(v), 0),
    }
}

func HashInt8(v int8) HashValue {
    return HashValue{
        xxhash.Hash32u32(uint32(v), XxHash32Seed),
        xxhash.Hash32u32(uint32(v), 0),
    }
}

func HashUint16(v uint16) HashValue {
    return HashValue{
        xxhash.Hash32u32(uint32(v), XxHash32Seed),
        xxhash.Hash32u32(uint32(v), 0),
    }
}

func HashInt16(v int16) HashValue {
    return HashValue{
        xxhash.Hash32u32(uint32(v), XxHash32Seed),
        xxhash.Hash32u32(uint32(v), 0),
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
    return Hash(v.Bytes())
}

func HashInt256(v num.Int256) HashValue {
    return Hash(v.Bytes())
}

func HashT[T types.Number](v T) HashValue {
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
        return HashValue{}
    }
}

func HashMulti(src any) []HashValue {
    if src == nil {
        return nil
    }
    var res []HashValue
    switch v := src.(type) {
    case [][]byte:
        res := make([]HashValue, len(v))
        for i := range res {
            res[i] = Hash(v[i])
        }
    case []string:
        res = make([]HashValue, len(v))
        for i := range res {
            res[i] = Hash(util.UnsafeGetBytes(v[i]))
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
        v = slicex.UniqueBools(v)
        res = make([]HashValue, len(v))
        for i := range res {
            if v[i] {
                res[i] = HashUint8(1)
            } else {
                res[i] = HashUint8(0)
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
