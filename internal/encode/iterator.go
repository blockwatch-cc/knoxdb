// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package encode

import (
	"iter"
	"slices"
	"unsafe"

	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/num"
)

const (
	CHUNK_SIZE = types.CHUNK_SIZE // = 128, must be pow2!
	CHUNK_MASK = CHUNK_SIZE - 1
)

func chunkBase(n int) int {
	return n &^ CHUNK_MASK
}

func chunkPos(n int) int {
	return n & CHUNK_MASK
}

// ---------------------------------
// Base Iterator
//

var _ types.NumberIterator[uint64] = (*BaseIterator[uint64])(nil)

type BaseIterator[T types.Number | []byte | num.Int128 | num.Int256] struct {
	chunk [CHUNK_SIZE]T
	base  int
	len   int
	ofs   int
	fill  func(int) int // implementations must overload this function
}

func (it *BaseIterator[T]) Close() {
	it.base = 0
	it.len = 0
	it.ofs = 0
	it.fill = nil
}

func (it *BaseIterator[T]) Len() int {
	return it.len
}

func (it *BaseIterator[T]) Value(n int) (t T) {
	if n < 0 || n >= it.len {
		return
	}
	if base := chunkBase(n); base != it.base {
		it.fill(base)
	}
	return it.chunk[chunkPos(n)]
}

func (it *BaseIterator[T]) Next() (*[CHUNK_SIZE]T, int) {
	// EOF
	if it.ofs >= it.len {
		return nil, 0
	}

	// refill (considering seek/skip/reset state updates)
	n := min(CHUNK_SIZE, it.len-it.base)
	if base := chunkBase(it.ofs); base != it.base {
		n = it.fill(base)
	}
	it.ofs = it.base + n

	return &it.chunk, n
}

func (it *BaseIterator[T]) Skip() int {
	n := min(CHUNK_SIZE, it.len-it.ofs)
	it.ofs += n
	return n
}

func (it *BaseIterator[T]) Seek(n int) bool {
	if n < 0 || n >= it.len {
		it.ofs = it.len
		return false
	}

	// fill on seek to an unloaded chunk
	if base := chunkBase(n); base != it.base {
		it.fill(base)
	}

	// reset ofs to n, so call to Next() delivers the chunk
	it.ofs = n
	return true
}

// helper func to implement container All
func (it *BaseIterator[T]) All() iter.Seq2[int, T] {
	return func(yield func(int, T) bool) {
		for i := range it.len {
			p := i % CHUNK_SIZE
			if p == 0 {
				it.fill(i)
			}
			if !yield(i, it.chunk[p]) {
				return
			}
		}
	}
}

// helper func to implement container Values
func (it *BaseIterator[T]) Values() iter.Seq[T] {
	return func(yield func(T) bool) {
		for i := range it.len {
			p := i % CHUNK_SIZE
			if p == 0 {
				it.fill(i)
			}
			if !yield(it.chunk[p]) {
				return
			}
		}
	}
}

// TODO: should be a container property
func (it *BaseIterator[T]) Select(sel []uint32) iter.Seq[T] {
	if slices.IsSorted(sel) {
		// fast path with single-pass forward only walk
		return func(yield func(T) bool) {
			var (
				c = -1
				n int
			)
			it.base = 0
			it.ofs = 0
			for _, v := range sel {
				if nc := int(v) / CHUNK_SIZE; nc > c {
					for range nc - c - 1 {
						it.Skip()
						c++
					}
					_, n = it.Next()
					if n == 0 {
						break
					}
					c++
				}
				if !yield(it.chunk[v%CHUNK_SIZE]) {
					return
				}
			}
		}
	} else {
		// slow path with amortized random batch load
		return func(yield func(T) bool) {
			for _, v := range sel {
				if !yield(it.Value(int(v))) {
					return
				}
			}
		}
	}
}

// Must be overloaed by derived implementations, left here for reference
// func (it *BaseIterator[T]) fill(base int) int {
// 	it.base = base
// 	return min(CHUNK_SIZE, it.len-it.base)
// }

// ---------------------------------
// Raw Iterator
//

var _ types.NumberIterator[uint64] = (*RawIterator[uint64])(nil)

type RawIterator[T types.Number] struct {
	vals []T
	ofs  int
}

func NewRawIterator[T types.Number](vals []T) *RawIterator[T] {
	return &RawIterator[T]{
		vals: vals,
	}
}

func (it *RawIterator[T]) Close() {
	it.vals = nil
	it.ofs = 0
}

func (it *RawIterator[T]) Reset() {
	it.ofs = 0
}

func (it *RawIterator[T]) Len() int {
	return len(it.vals)
}

func (it *RawIterator[T]) Value(n int) T {
	if n >= 0 && n < len(it.vals) {
		return it.vals[n]
	}
	return 0
}

func (it *RawIterator[T]) Next() (*[CHUNK_SIZE]T, int) {
	// EOF
	if it.ofs >= len(it.vals) {
		return nil, 0
	}
	base := chunkBase(it.ofs)
	n := min(CHUNK_SIZE, len(it.vals)-base)
	it.ofs += n
	return (*[CHUNK_SIZE]T)(unsafe.Pointer(&it.vals[base])), n
}

func (it *RawIterator[T]) Skip() int {
	n := min(CHUNK_SIZE, len(it.vals)-it.ofs)
	it.ofs += n
	return n
}

func (it *RawIterator[T]) Seek(n int) bool {
	if n < 0 || n >= len(it.vals) {
		it.ofs = len(it.vals)
		return false
	}
	// reset ofs to n, so call to Next() delivers value
	it.ofs = n
	return true
}

func (it *RawIterator[T]) All() iter.Seq2[int, T] {
	return slices.All(it.vals)
}

func (it *RawIterator[T]) Values() iter.Seq[T] {
	return slices.Values(it.vals)
}

// ---------------------------------
// Const Iterator
//

var _ types.NumberIterator[uint64] = (*ConstIterator[uint64])(nil)

type ConstIterator[T types.Number | []byte] struct {
	BaseIterator[T]
}

func NewConstIterator[T types.Number | []byte](val T, n int) *ConstIterator[T] {
	it := &ConstIterator[T]{
		BaseIterator: BaseIterator[T]{
			base: -1,
			len:  n,
		},
	}
	// fill chunk once
	it.chunk[0] = val
	for j := 1; j < len(it.chunk); j *= 2 {
		copy(it.chunk[j:], it.chunk[:j])
	}
	it.BaseIterator.fill = it.fill
	return it
}

func (it *ConstIterator[T]) fill(base int) int {
	it.base = base
	return min(CHUNK_SIZE, it.len-it.base)
}

// ---------------------------------
// Delta Iterator
//

var _ types.NumberIterator[uint64] = (*DeltaIterator[uint64])(nil)

type DeltaIterator[T types.Integer] struct {
	BaseIterator[T]
	delta T
	ffor  T
}

func NewDeltaIterator[T types.Integer](delta, ffor T, n int) *DeltaIterator[T] {
	it := &DeltaIterator[T]{
		delta: delta,
		ffor:  ffor,
		BaseIterator: BaseIterator[T]{
			base: -1,
			len:  n,
		},
	}
	it.BaseIterator.fill = it.fill
	return it
}

func (it *DeltaIterator[T]) fill(base int) int {
	it.base = base
	var i int
	for range CHUNK_SIZE / 8 {
		it.chunk[i] = T(base)*it.delta + it.ffor
		it.chunk[i+1] = T(base+1)*it.delta + it.ffor
		it.chunk[i+2] = T(base+2)*it.delta + it.ffor
		it.chunk[i+3] = T(base+3)*it.delta + it.ffor
		it.chunk[i+4] = T(base+4)*it.delta + it.ffor
		it.chunk[i+5] = T(base+5)*it.delta + it.ffor
		it.chunk[i+6] = T(base+6)*it.delta + it.ffor
		it.chunk[i+7] = T(base+7)*it.delta + it.ffor
		i += 8
		base += 8
	}
	return min(CHUNK_SIZE, it.len-it.base)
}
