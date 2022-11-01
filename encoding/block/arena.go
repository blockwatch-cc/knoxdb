// Copyright (c) 2018-2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package block

import (
    "math/bits"
    "sync"

    "blockwatch.cc/knoxdb/encoding/dedup"
    "blockwatch.cc/knoxdb/vec"
)

type Allocator interface {
    Alloc(int) any
    Free(any)
}

// 1k (10) .. 128k (18) = 8 sync.Pools
type allocator[T any] struct {
    pools [8]*sync.Pool
}

const (
    minAllocClass = 10
    maxAllocClass = 17
)

func (a *allocator[T]) Alloc(sz int) any {
    class := 63 - bits.LeadingZeros(uint(sz))
    if bits.OnesCount(uint(sz)) > 1 {
        class++
    }
    if class < minAllocClass || class > maxAllocClass {
        return make([]T, 0, sz)
    }
    idx := class - minAllocClass
    if a.pools[idx] == nil {
        a.pools[idx] = &sync.Pool{
            New: func() any { return make([]T, 0, 1<<class) },
        }
    }
    return a.pools[idx].Get()
}

func (a *allocator[T]) Free(val any) {
    slice, ok := val.([]T)
    if !ok {
        return
    }
    sz := cap(slice)

    // don't recycle out of bounds or non-power of 2 slices
    class := 63 - bits.LeadingZeros(uint(sz))
    if class < minAllocClass || class > maxAllocClass || bits.OnesCount(uint(sz)) > 1 {
        return
    }
    idx := class - minAllocClass
    if a.pools[idx] != nil {
        a.pools[idx].Put(slice[:0])
    }
}

type bitSetAllocator struct{}

func (a bitSetAllocator) Alloc(sz int) any {
    return vec.NewBitset(sz).Reset()
}

func (a bitSetAllocator) Free(val any) {
    b, ok := val.(*vec.Bitset)
    if ok {
        b.Close()
    }
}

type dedupAllocator struct{}

func (a dedupAllocator) Alloc(sz int) any {
    return dedup.NewByteArray(sz)
}

func (a dedupAllocator) Free(val any) {
    dd, ok := val.(dedup.ByteArray)
    if ok {
        dd.Release()
    }
}

var arena = NewArena()

type Arena struct {
    alloc [16]Allocator
}

func NewArena() *Arena {
    return &Arena{
        alloc: [16]Allocator{
            &allocator[int64]{},      // BlockTime
            &allocator[int64]{},      // BlockInt64
            &allocator[uint64]{},     // BlockUint64
            &allocator[float64]{},    // BlockFloat64
            &bitSetAllocator{},       // BlockBool !! unsused, blocks alloc direct
            &dedupAllocator{},        // BlockString !! unsused, blocks alloc direct
            &dedupAllocator{},        // BlockBytes !! unsused, blocks alloc direct
            &allocator[int32]{},      // BlockInt32
            &allocator[int16]{},      // BlockInt16
            &allocator[int8]{},       // BlockInt8
            &allocator[uint32]{},     // BlockUint32
            &allocator[uint16]{},     // BlockUint16
            &allocator[uint8]{},      // BlockUint8
            &allocator[float32]{},    // BlockFloat32
            &allocator[vec.Int128]{}, // BlockInt128 !! blocks use strides
            &allocator[vec.Int256]{}, // BlockInt256 !! blocks use strides
        },
    }
}

func (a *Arena) Alloc(typ BlockType, sz int) interface{} {
    return a.alloc[int(typ)].Alloc(sz)
}

func (a *Arena) Free(typ BlockType, val interface{}) {
    if val == nil {
        return
    }
    a.alloc[int(typ)].Free(val)
}