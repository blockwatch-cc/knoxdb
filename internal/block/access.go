// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package block

import (
	"fmt"
	"unsafe"

	"blockwatch.cc/knoxdb/internal/bitset"
	"blockwatch.cc/knoxdb/internal/dedup"
	"blockwatch.cc/knoxdb/pkg/num"
)

type Number interface {
	int64 | int32 | int16 | int8 | uint64 | uint32 | uint16 | uint8 | float64 | float32
}

type BlockAccessor[T Number] struct {
	block *Block
	sz    int
}

func NewBlockAccessor[T Number](b *Block) BlockAccessor[T] {
	return BlockAccessor[T]{
		block: b,
		sz:    b.typ.Size(),
	}
}

func (a BlockAccessor[T]) Get(n int) (t T) {
	if n >= a.block.len {
		panic(fmt.Errorf("get: block bounds out of range [:%d] with length %d", n, a.block.len))
	}
	ptr := unsafe.Add(a.block.ptr, n*a.sz)
	return *(*T)(ptr)
}

func (a BlockAccessor[T]) Set(n int, v T) {
	if n >= a.block.len {
		panic(fmt.Errorf("set: block bounds out of range [:%d] with length %d", n, a.block.len))
	}
	ptr := unsafe.Add(a.block.ptr, n*a.sz)
	*(*T)(ptr) = v
	a.block.dirty = true
}

func (a BlockAccessor[T]) Append(v T) {
	if a.block.len == a.block.cap {
		panic(fmt.Errorf("append: block capacity exhausted [:%d:%d]", a.block.len, a.block.cap))
	}
	ptr := unsafe.Add(a.block.ptr, a.block.len*a.sz)
	*(*T)(ptr) = v
	a.block.len++
	a.block.dirty = true
}

func (a BlockAccessor[T]) Less(i, j int) bool {
	if i >= a.block.len {
		panic(fmt.Errorf("get: block bounds out of range [:%d] with length %d", i, a.block.len))
	}
	if j >= a.block.len {
		panic(fmt.Errorf("get: block bounds out of range [:%d] with length %d", j, a.block.len))
	}
	ipos, jpos := i*a.sz, j*a.sz
	iptr := unsafe.Add(a.block.ptr, ipos)
	jptr := unsafe.Add(a.block.ptr, jpos)
	return *(*T)(iptr) < *(*T)(jptr)
}

func (a BlockAccessor[T]) Swap(i, j int) {
	if i >= a.block.len {
		panic(fmt.Errorf("get: block bounds out of range [:%d] with length %d", i, a.block.len))
	}
	if j >= a.block.len {
		panic(fmt.Errorf("get: block bounds out of range [:%d] with length %d", j, a.block.len))
	}
	ipos, jpos := i*a.sz, j*a.sz
	iptr := unsafe.Add(a.block.ptr, ipos)
	jptr := unsafe.Add(a.block.ptr, jpos)
	*(*T)(iptr), *(*T)(jptr) = *(*T)(jptr), *(*T)(iptr)
	a.block.dirty = true
}

func (a BlockAccessor[T]) Cmp(i, j int) int {
	if i >= a.block.len {
		panic(fmt.Errorf("get: block bounds out of range [:%d] with length %d", i, a.block.len))
	}
	if j >= a.block.len {
		panic(fmt.Errorf("get: block bounds out of range [:%d] with length %d", j, a.block.len))
	}
	ival := *(*T)(unsafe.Add(a.block.ptr, i*a.sz))
	jval := *(*T)(unsafe.Add(a.block.ptr, j*a.sz))
	switch {
	case ival == jval:
		return 0
	case ival < jval:
		return -1
	default:
		return 1
	}
}

func (a BlockAccessor[T]) Slice() []T {
	return unsafe.Slice((*T)(a.block.ptr), a.block.len)
}

func (a BlockAccessor[T]) FullSlice() []T {
	return unsafe.Slice((*T)(a.block.ptr), a.block.cap)
}

func (b *Block) Int64() BlockAccessor[int64] {
	return BlockAccessor[int64]{b, 8}
}

func (b *Block) Int32() BlockAccessor[int32] {
	return BlockAccessor[int32]{b, 4}
}

func (b *Block) Int16() BlockAccessor[int16] {
	return BlockAccessor[int16]{b, 2}
}

func (b *Block) Int8() BlockAccessor[int8] {
	return BlockAccessor[int8]{b, 1}
}

func (b *Block) Uint64() BlockAccessor[uint64] {
	return BlockAccessor[uint64]{b, 8}
}

func (b *Block) Uint32() BlockAccessor[uint32] {
	return BlockAccessor[uint32]{b, 4}
}

func (b *Block) Uint16() BlockAccessor[uint16] {
	return BlockAccessor[uint16]{b, 2}
}

func (b *Block) Uint8() BlockAccessor[uint8] {
	return BlockAccessor[uint8]{b, 1}
}

func (b *Block) Float64() BlockAccessor[float64] {
	return BlockAccessor[float64]{b, 8}
}

func (b *Block) Float32() BlockAccessor[float32] {
	return BlockAccessor[float32]{b, 4}
}

func (b *Block) Int128() *num.Int128Stride {
	return (*num.Int128Stride)(b.ptr)
}

func (b *Block) Int256() *num.Int256Stride {
	return (*num.Int256Stride)(b.ptr)
}

func (b *Block) Bytes() dedup.ByteArray {
	return (*(*dedup.ByteArray)(b.ptr))
}

func (b *Block) Bool() *bitset.Bitset {
	return (*bitset.Bitset)(b.ptr)
}
