// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package block

import (
	"fmt"
	"unsafe"

	"blockwatch.cc/knoxdb/internal/bitset"
	"blockwatch.cc/knoxdb/internal/dedup"
	"blockwatch.cc/knoxdb/internal/types"
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

func (a *BlockAccessor[T]) Close() {
	a.block = nil
	a.sz = 0
}

func (a BlockAccessor[T]) Get(n int) (t T) {
	if a.block == nil {
		return
	}
	if n >= a.block.len {
		panic(fmt.Errorf("get: block bounds out of range [:%d] with length %d", n, a.block.len))
	}
	ptr := unsafe.Add(a.block.ptr, n*a.sz)
	return *(*T)(ptr)
}

func (a BlockAccessor[T]) Set(n int, v T) {
	if a.block == nil {
		return
	}
	if n >= a.block.len {
		panic(fmt.Errorf("set: block bounds out of range [:%d] with length %d", n, a.block.len))
	}
	ptr := unsafe.Add(a.block.ptr, n*a.sz)
	*(*T)(ptr) = v
	a.block.dirty = true
}

func (a BlockAccessor[T]) Append(v T) {
	if a.block == nil {
		return
	}
	if a.block.len == a.block.cap {
		panic(fmt.Errorf("append: block capacity exhausted [:%d:%d]", a.block.len, a.block.cap))
	}
	ptr := unsafe.Add(a.block.ptr, a.block.len*a.sz)
	*(*T)(ptr) = v
	a.block.len++
	a.block.dirty = true
}

func (a BlockAccessor[T]) Less(i, j int) bool {
	if a.block == nil {
		return false
	}
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
	if a.block == nil {
		return
	}
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
	if a.block == nil {
		return 0
	}
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
	if a.block == nil {
		return make([]T, 0)
	}
	return unsafe.Slice((*T)(a.block.ptr), a.block.len)
}

func (a BlockAccessor[T]) FullSlice() []T {
	if a.block == nil {
		return make([]T, 0)
	}
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

func (b *Block) Append(val any) {
	switch b.typ {
	case BlockTime, BlockInt64:
		b.Int64().Append(val.(int64))
	case types.BlockInt32:
		b.Int32().Append(val.(int32))
	case types.BlockInt16:
		b.Int16().Append(val.(int16))
	case types.BlockInt8:
		b.Int8().Append(val.(int8))
	case types.BlockUint64:
		b.Uint64().Append(val.(uint64))
	case types.BlockUint32:
		b.Uint32().Append(val.(uint32))
	case types.BlockUint16:
		b.Uint16().Append(val.(uint16))
	case types.BlockUint8:
		b.Uint8().Append(val.(uint8))
	case types.BlockFloat64:
		b.Float64().Append(val.(float64))
	case types.BlockFloat32:
		b.Float32().Append(val.(float32))
	case types.BlockBool:
		b.Bool().Append(val.(bool))
	case types.BlockBytes:
		b.Bytes().Append(val.([]byte))
	case types.BlockInt128:
		b.Int128().Append(val.(num.Int128))
	case types.BlockInt256:
		b.Int256().Append(val.(num.Int256))
	}
}

func (b *Block) Get(row int) any {
	switch b.typ {
	case BlockTime, BlockInt64:
		return b.Int64().Get(row)
	case types.BlockInt32:
		return b.Int32().Get(row)
	case types.BlockInt16:
		return b.Int16().Get(row)
	case types.BlockInt8:
		return b.Int8().Get(row)
	case types.BlockUint64:
		return b.Uint64().Get(row)
	case types.BlockUint32:
		return b.Uint32().Get(row)
	case types.BlockUint16:
		return b.Uint16().Get(row)
	case types.BlockUint8:
		return b.Uint8().Get(row)
	case types.BlockFloat64:
		return b.Float64().Get(row)
	case types.BlockFloat32:
		return b.Float32().Get(row)
	case types.BlockBool:
		return b.Bool().IsSet(row)
	case types.BlockBytes:
		return b.Bytes().Elem(row)
	case types.BlockInt128:
		return b.Int128().Elem(row)
	case types.BlockInt256:
		return b.Int256().Elem(row)
	default:
		return nil
	}
}

func (b *Block) Set(row int, val any) {
	switch b.typ {
	case BlockTime, BlockInt64:
		b.Int64().Set(row, val.(int64))
	case types.BlockInt32:
		b.Int32().Set(row, val.(int32))
	case types.BlockInt16:
		b.Int16().Set(row, val.(int16))
	case types.BlockInt8:
		b.Int8().Set(row, val.(int8))
	case types.BlockUint64:
		b.Uint64().Set(row, val.(uint64))
	case types.BlockUint32:
		b.Uint32().Set(row, val.(uint32))
	case types.BlockUint16:
		b.Uint16().Set(row, val.(uint16))
	case types.BlockUint8:
		b.Uint8().Set(row, val.(uint8))
	case types.BlockFloat64:
		b.Float64().Set(row, val.(float64))
	case types.BlockFloat32:
		b.Float32().Set(row, val.(float32))
	case types.BlockBool:
		if val.(bool) {
			b.Bool().Set(row)
		} else {
			b.Bool().Clear(row)
		}
	case types.BlockBytes:
		b.Bytes().Set(row, val.([]byte))
	case types.BlockInt128:
		b.Int128().Set(row, val.(num.Int128))
	case types.BlockInt256:
		b.Int256().Set(row, val.(num.Int256))
	}
}
