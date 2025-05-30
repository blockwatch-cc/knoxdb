// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package num

var _ BigIntIterator[Int256, Int256Stride] = (*Int256Iterator)(nil)

type Int256Iterator struct {
	stride *Int256Stride
	base   int
}

func NewInt256Iterator(s *Int256Stride) *Int256Iterator {
	return &Int256Iterator{s, 0}
}

func (it *Int256Iterator) Len() int {
	return it.stride.Len()
}

func (it *Int256Iterator) Get(n int) Int256 {
	return it.stride.Get(n)
}

func (it *Int256Iterator) Seek(n int) bool {
	l := it.stride.Len()
	if n < 0 || n >= l {
		it.base = l
		return false
	}
	it.base = n
	return true
}

func (it *Int256Iterator) NextChunk() (*Int256Stride, int) {
	if it.base >= it.stride.Len() {
		return nil, 0
	}
	n := int(min(it.stride.Len()-it.base, CHUNK_SIZE))
	return it.stride.Range(it.base, it.base+n), n
}

func (it *Int256Iterator) SkipChunk() int {
	n := min(CHUNK_SIZE, it.stride.Len()-it.base)
	it.base += n
	return int(n)
}

func (it *Int256Iterator) Close() {
	it.stride = nil
	it.base = 0
}
