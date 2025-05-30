// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package stringx

import "blockwatch.cc/knoxdb/internal/types"

const (
	CHUNK_SIZE = types.CHUNK_SIZE
	CHUNK_MASK = CHUNK_SIZE - 1
)

var _ types.StringIterator = (*StringChunkIterator)(nil)

type StringChunkIterator struct {
	chunk [CHUNK_SIZE][]byte
	pool  *StringPool
	base  int
}

func NewIterator(p *StringPool) *StringChunkIterator {
	return &StringChunkIterator{
		pool: p,
		base: 0,
	}
}

func (it *StringChunkIterator) Len() int {
	return len(it.pool.ptr)
}

func (it *StringChunkIterator) Get(n int) []byte {
	return it.pool.Get(n)
}

func (it *StringChunkIterator) Seek(n int) bool {
	l := len(it.pool.ptr)
	if n < 0 || n >= l {
		it.base = l
		return false
	}
	it.base = types.ChunkBase(n)
	return true
}

func (it *StringChunkIterator) NextChunk() (*[CHUNK_SIZE][]byte, int) {
	l := len(it.pool.ptr)
	if it.base >= l {
		return nil, 0
	}

	// refill
	n := min(CHUNK_SIZE, len(it.pool.ptr)-it.base)
	for i, v := range it.pool.ptr[it.base : it.base+n] {
		ofs, sz := ptr2pair(v)
		it.chunk[i] = it.pool.buf[ofs : ofs+sz]
	}
	clear(it.chunk[n:])

	// advance base for next iteration
	it.base += n

	return &it.chunk, n
}

func (it *StringChunkIterator) SkipChunk() int {
	n := min(types.CHUNK_SIZE, len(it.pool.ptr)-it.base)
	it.base += n
	return n
}

func (it *StringChunkIterator) Close() {
	clear(it.chunk[:])
	it.pool = nil
	it.base = 0
}
