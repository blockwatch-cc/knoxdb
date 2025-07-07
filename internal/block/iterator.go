// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package block

import (
	"unsafe"

	"blockwatch.cc/knoxdb/internal/types"
)

var _ types.NumberIterator[int64] = (*BlockIterator[int64])(nil)

type BlockIterator[T types.Number] struct {
	block *Block
	base  uint32
}

func NewBlockIterator[T types.Number](b *Block) *BlockIterator[T] {
	return &BlockIterator[T]{b, 0}
}

func (it *BlockIterator[T]) Len() int {
	return int(it.block.len)
}

func (it *BlockIterator[T]) Get(n int) T {
	return *(*T)(unsafe.Add(unsafe.Pointer(it.block.buf), n*int(it.block.sz)))
}

func (it *BlockIterator[T]) Seek(n int) bool {
	if n < 0 || n >= int(it.block.len) {
		it.base = it.block.len
		return false
	}
	it.base = uint32(n)
	return true
}

func (it *BlockIterator[T]) NextChunk() (*[types.CHUNK_SIZE]T, int) {
	if it.base >= it.block.len {
		return nil, 0
	}
	n := min(it.block.len-it.base, types.CHUNK_SIZE)
	ptr := unsafe.Add(unsafe.Pointer(it.block.buf), it.base*uint32(it.block.sz))
	it.base += n
	return (*[types.CHUNK_SIZE]T)(ptr), int(n)
}

func (it *BlockIterator[T]) SkipChunk() int {
	n := min(types.CHUNK_SIZE, it.block.len-it.base)
	it.base += n
	return int(n)
}

func (it *BlockIterator[T]) Close() {
	it.block = nil
	it.base = 0
}
