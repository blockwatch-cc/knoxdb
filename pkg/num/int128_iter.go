// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package num

import "iter"

var _ BigIntIterator[Int128] = (*Int128Iterator)(nil)

type Int128Iterator struct {
	chunk  [128]Int128
	stride *Int128Stride
	base   int
}

func NewInt128Iterator(s *Int128Stride) *Int128Iterator {
	return &Int128Iterator{stride: s, base: 0}
}

func (it *Int128Iterator) Len() int {
	return it.stride.Len()
}

func (it *Int128Iterator) Value(n int) Int128 {
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

func (it *Int128Iterator) Next() (*[128]Int128, int) {
	if it.base >= it.stride.Len() {
		return nil, 0
	}
	n := min(it.stride.Len()-it.base, CHUNK_SIZE)
	for i, v := range it.stride.Range(it.base, it.base+n).All() {
		it.chunk[i] = v
	}
	return &it.chunk, n
}

func (it *Int128Iterator) Skip() int {
	n := min(CHUNK_SIZE, it.stride.Len()-it.base)
	it.base += n
	return n
}

func (it *Int128Iterator) Close() {
	it.stride = nil
	it.base = 0
}

func (it *Int128Iterator) All() iter.Seq2[int, Int128] {
	return func(yield func(int, Int128) bool) {
		for i := range it.stride.X0 {
			if !yield(i, it.stride.Get(i)) {
				return
			}
		}
	}
}

func (it *Int128Iterator) Values() iter.Seq[Int128] {
	return func(yield func(Int128) bool) {
		for i := range it.stride.X0 {
			if !yield(it.stride.Get(i)) {
				return
			}
		}
	}
}

func (it *Int128Iterator) Select(sel []uint32) iter.Seq[Int128] {
	return func(yield func(Int128) bool) {
		for _, i := range sel {
			if !yield(it.stride.Get(int(i))) {
				return
			}
		}
	}
}
