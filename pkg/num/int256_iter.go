// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package num

import (
	"iter"
)

var _ BigIntIterator[Int256] = (*Int256Iterator)(nil)

type Int256Iterator struct {
	chunk  [128]Int256
	stride *Int256Stride
	base   int
}

func NewInt256Iterator(s *Int256Stride) *Int256Iterator {
	return &Int256Iterator{stride: s, base: 0}
}

func (it *Int256Iterator) Len() int {
	return it.stride.Len()
}

func (it *Int256Iterator) Value(n int) Int256 {
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

func (it *Int256Iterator) Next() (*[128]Int256, int) {
	if it.base >= it.stride.Len() {
		return nil, 0
	}
	n := min(it.stride.Len()-it.base, CHUNK_SIZE)
	for i, v := range it.stride.Range(it.base, it.base+n).All() {
		it.chunk[i] = v
	}
	return &it.chunk, n
}

func (it *Int256Iterator) Skip() int {
	n := min(CHUNK_SIZE, it.stride.Len()-it.base)
	it.base += n
	return n
}

func (it *Int256Iterator) Close() {
	it.stride = nil
	it.base = 0
}

func (it *Int256Iterator) All() iter.Seq2[int, Int256] {
	return func(yield func(int, Int256) bool) {
		for i := range it.stride.X0 {
			if !yield(i, it.stride.Get(i)) {
				return
			}
		}
	}
}

func (it *Int256Iterator) Values() iter.Seq[Int256] {
	return func(yield func(Int256) bool) {
		for i := range it.stride.X0 {
			if !yield(it.stride.Get(i)) {
				return
			}
		}
	}
}

func (it *Int256Iterator) Select(sel []uint32) iter.Seq[Int256] {
	return func(yield func(Int256) bool) {
		for _, i := range sel {
			if !yield(it.stride.Get(int(i))) {
				return
			}
		}
	}
}
