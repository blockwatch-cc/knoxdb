// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package num

const CHUNK_SIZE = 128

var _ BigIntIterator[Int128, Int128Stride] = (*Int128Iterator)(nil)

type Int128Iterator struct {
	stride *Int128Stride
	base   int
}

func NewInt128Iterator(s *Int128Stride) *Int128Iterator {
	return &Int128Iterator{s, 0}
}

func (it *Int128Iterator) Len() int {
	return it.stride.Len()
}

func (it *Int128Iterator) Get(n int) Int128 {
	return it.stride.Get(n)
}

func (it *Int128Iterator) Seek(n int) bool {
	l := it.stride.Len()
	if n < 0 || n >= l {
		it.base = l
		return false
	}
	it.base = n
	return true
}

func (it *Int128Iterator) NextChunk() (*Int128Stride, int) {
	if it.base >= it.stride.Len() {
		return nil, 0
	}
	n := int(min(it.stride.Len()-it.base, CHUNK_SIZE))
	return it.stride.Range(it.base, it.base+n), n
}

func (it *Int128Iterator) SkipChunk() int {
	n := min(CHUNK_SIZE, it.stride.Len()-it.base)
	it.base += n
	return int(n)
}

func (it *Int128Iterator) Close() {
	it.stride = nil
	it.base = 0
}
