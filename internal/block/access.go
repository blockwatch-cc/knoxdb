// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package block

import (
	"errors"
	"unsafe"

	"blockwatch.cc/knoxdb/internal/bitset"
	"blockwatch.cc/knoxdb/internal/cmp"
	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/assert"
	"blockwatch.cc/knoxdb/pkg/num"
	"blockwatch.cc/knoxdb/pkg/stringx"
	"blockwatch.cc/knoxdb/pkg/util"
)

var (
	ErrBlockOutOfBounds     = errors.New("block: out of bounds access")
	ErrBlockNotMaterialized = errors.New("block: not materialized")
)

// BlockAccessor provides typed access to materialized block contents.
// A materialized block is a byte array cast to the correct numeric type,
// hence this accessor is a slim wrapper around this array.
type BlockAccessor[T types.Number] struct {
	block *Block
}

func NewBlockAccessor[T types.Number](b *Block) BlockAccessor[T] {
	return BlockAccessor[T]{
		block: b,
	}
}

func (a BlockAccessor[T]) Close() {
	// keep struct receiver access across all funcs, close cannot write
	// a.block = nil
}

func (a BlockAccessor[T]) Len() int {
	return a.block.Len()
}

func (a BlockAccessor[T]) Size() int {
	return a.block.Size()
}

func (a BlockAccessor[T]) Get(n int) (t T) {
	assert.Always(a.block != nil, "get: nil block")
	assert.Always(n < int(a.block.len), "get: block bounds out of range", "n", n, "len", a.block.len)
	if n >= int(a.block.len) {
		panic(ErrBlockOutOfBounds)
	}
	ptr := unsafe.Add(unsafe.Pointer(a.block.buf), n*int(a.block.sz))
	return *(*T)(ptr)
}

func (a BlockAccessor[T]) Set(n int, v T) {
	assert.Always(a.block != nil, "set: nil block")
	assert.Always(a.block.IsMaterialized(), "set: block not materialized")
	assert.Always(n < int(a.block.len), "set: block bounds out of range", "n", n, "len", a.block.len)
	if n >= int(a.block.len) {
		panic(ErrBlockOutOfBounds)
	}
	ptr := unsafe.Add(unsafe.Pointer(a.block.buf), n*int(a.block.sz))
	*(*T)(ptr) = v
	a.block.SetDirty()
}

func (a BlockAccessor[T]) Append(v T) {
	assert.Always(a.block != nil, "append: nil block")
	assert.Always(a.block.IsMaterialized(), "append: block not materialized")
	assert.Always(a.block.len < a.block.cap, "append: block capacity exhausted", "len", a.block.len, "cap", a.block.cap)
	if a.block.len >= a.block.cap {
		panic(ErrBlockOutOfBounds)
	}
	ptr := unsafe.Add(unsafe.Pointer(a.block.buf), a.block.len*uint32(a.block.sz))
	*(*T)(ptr) = v
	a.block.len++
	a.block.SetDirty()
}

func (a BlockAccessor[T]) Cmp(i, j int) int {
	assert.Always(a.block != nil, "cmp: nil block")
	len := int(a.block.len)
	assert.Always(i < len, "cmp: block bounds out of range", "i", i, "len", a.block.len)
	assert.Always(j < len, "cmp: block bounds out of range", "j", j, "len", a.block.len)
	if i >= len || j >= len {
		panic(ErrBlockOutOfBounds)
	}
	ptr := unsafe.Pointer(a.block.buf)
	ival := *(*T)(unsafe.Add(ptr, i*int(a.block.sz)))
	jval := *(*T)(unsafe.Add(ptr, j*int(a.block.sz)))
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
	assert.Always(a.block != nil, "slice: nil block")
	assert.Always(a.block.IsMaterialized(), "slice: block not materialized")
	return unsafe.Slice((*T)(unsafe.Pointer(a.block.buf)), a.block.len)
}

// Matcher selects the appropriate match implementation. Its likely unused
// for materialized blocks since query.Matcher functions handle all cases
// with less overhead (avoid func table lookups for each block). Keeping
// this implementation for reference and completeness.
func (a BlockAccessor[T]) Matcher() types.NumberMatcher[T] {
	assert.Always(a.block != nil, "slice: nil block")
	assert.Always(a.block.IsMaterialized(), "matcher: block not materialized")
	return cmp.NewMatcher[T](a.Slice())
}

func (a BlockAccessor[T]) AppendTo(dst []T, sel []uint32) []T {
	assert.Always(a.block != nil, "appendTo: nil block")
	assert.Always(a.block.IsMaterialized(), "appendTo: block not materialized")
	ptr := unsafe.Pointer(a.block.buf)
	if sel == nil {
		dst = append(dst, unsafe.Slice((*T)(ptr), a.block.len)...)
	} else {
		sz := uint32(a.block.sz)
		for _, v := range sel {
			dst = append(dst, *(*T)(unsafe.Add(ptr, v*sz)))
		}
	}
	return dst
}

func (a BlockAccessor[T]) MinMax() (T, T) {
	return util.MinMax[T](unsafe.Slice((*T)(unsafe.Pointer(a.block.buf)), a.block.len)...)
}

// ---------------------------------------------
// Block Accessors
//

func (b *Block) Int64() types.NumberAccessor[int64] {
	if b.IsMaterialized() {
		return NewBlockAccessor[int64](b)
	}
	return b.any.(types.NumberAccessor[int64])
}

func (b *Block) Int32() types.NumberAccessor[int32] {
	if b.IsMaterialized() {
		return NewBlockAccessor[int32](b)
	}
	return b.any.(types.NumberAccessor[int32])
}

func (b *Block) Int16() types.NumberAccessor[int16] {
	if b.IsMaterialized() {
		return NewBlockAccessor[int16](b)
	}
	return b.any.(types.NumberAccessor[int16])
}

func (b *Block) Int8() types.NumberAccessor[int8] {
	if b.IsMaterialized() {
		return NewBlockAccessor[int8](b)
	}
	return b.any.(types.NumberAccessor[int8])
}

func (b *Block) Uint64() types.NumberAccessor[uint64] {
	if b.IsMaterialized() {
		return NewBlockAccessor[uint64](b)
	}
	return b.any.(types.NumberAccessor[uint64])
}

func (b *Block) Uint32() types.NumberAccessor[uint32] {
	if b.IsMaterialized() {
		return NewBlockAccessor[uint32](b)
	}
	return b.any.(types.NumberAccessor[uint32])
}

func (b *Block) Uint16() types.NumberAccessor[uint16] {
	if b.IsMaterialized() {
		return NewBlockAccessor[uint16](b)
	}
	return b.any.(types.NumberAccessor[uint16])
}

func (b *Block) Uint8() types.NumberAccessor[uint8] {
	if b.IsMaterialized() {
		return NewBlockAccessor[uint8](b)
	}
	return b.any.(types.NumberAccessor[uint8])
}

func (b *Block) Float64() types.NumberAccessor[float64] {
	if b.IsMaterialized() {
		return NewBlockAccessor[float64](b)
	}
	return b.any.(types.NumberAccessor[float64])
}

func (b *Block) Float32() types.NumberAccessor[float32] {
	if b.IsMaterialized() {
		return NewBlockAccessor[float32](b)
	}
	return b.any.(types.NumberAccessor[float32])
}

// -------------------------------------

// TODO: can we wrap these types into an accessor interface of type T?
// type WrappedAccessor[E any] struct {
// 	types.VectorAccessor[E]
// }

// func (a *WrappedAccessor[E]) Get(i int) E                                     { return a.Get(i) }
// func (a *WrappedAccessor[E]) Set(i int, val E)                                { a.Set(i, val) }
// func (a *WrappedAccessor[E]) Delete(i int)                                    { a.Delete(i) }
// func (a *WrappedAccessor[E]) Close()                                          {}
// func (a *WrappedAccessor[E]) Append(val E)                                    { a.Append(val) }
// func (a *WrappedAccessor[E]) AppendTo(to types.VectorSetter[E], sel []uint32) { a.AppendTo(to, sel) }
// func (a *WrappedAccessor[E]) Len() int                                        { return a.Len() }
// func (a *WrappedAccessor[E]) Size() int                                       { return a.Size() }
// func (a *WrappedAccessor[E]) Iterator() iter.Seq2[int, E]                     { return a.Iterator() }

// TODO: is there a benefit in hiding concrete implementations for
// StringPool, Bitset, IntXXXStride ?
// var (
// 	_ types.VectorGetter[[]byte]     = (*stringx.StringPool)(nil)
// 	_ types.VectorGetter[num.Int128] = (*num.Int128Stride)(nil)
// 	_ types.VectorGetter[num.Int256] = (*num.Int256Stride)(nil)
// 	_ types.VectorGetter[bool]       = (*bitset.Bitset)(nil)
// )

// -------------------------------------

// TODO: handle compressed containers here
// func Matcher() (types.VectorMatcher[[]byte], bool)
// func Matcher() (types.VectorMatcher[bool], bool)
// func Matcher() (types.VectorMatcher[num.Int128], bool)
// func Matcher() (types.VectorMatcher[num.Int256], bool)

// TODO: need Int128Accessor type (can we use VectorAccessor[num.Int128]?)

func (b *Block) Int128() *num.Int128Stride {
	if b.IsMaterialized() {
		return b.any.(*num.Int128Stride)
	}
	return nil // TODO
}

func (b *Block) Int256() *num.Int256Stride {
	if b.IsMaterialized() {
		return b.any.(*num.Int256Stride)
	}
	return nil // TODO
}

func (b *Block) Bytes() *stringx.StringPool {
	if b.IsMaterialized() {
		return b.any.(*stringx.StringPool)
	}
	// return b.any.(types.StringAccessor)
	return nil // TODO
}

func (b *Block) Bool() *bitset.Bitset {
	if b.IsMaterialized() {
		return b.any.(*bitset.Bitset)
	}
	return nil // TODO
}
