// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package s8b

import (
	"iter"
	"sync"
	"unsafe"

	"blockwatch.cc/knoxdb/internal/arena"
	"blockwatch.cc/knoxdb/internal/encode/s8b/generic"
	"blockwatch.cc/knoxdb/internal/types"
)

const (
	CHUNK_SIZE = types.CHUNK_SIZE // 128, must be pow2!
)

type Iterator[T types.Integer] struct {
	chunk [CHUNK_SIZE + 60]T  // overallocate for max word size
	src   []byte              // s8b encoded uint64 words
	minv  T                   // min-FOR value
	base  int                 // index of current chunk in vector
	ofs   int                 // next base
	len   int                 // total count of encoded values in src
	align int                 // first valid value in chunk
	sz    int                 // word size
	idx   *IndexImpl[uint32]  // value offset index
	dec   *generic.Decoder[T] // embedded decoder
}

func NewIterator[T types.Integer](buf []byte, n int, minv T) *Iterator[T] {
	if n <= 0 {
		n = CountValues(buf)
	}
	it := newIterator[T]()
	it.base = -1
	it.ofs = 0
	it.src = buf
	it.len = n
	it.align = 0
	it.sz = arena.SizeFor[T]()
	it.minv = minv
	it.dec = generic.NewDecoder(minv)
	if it.idx != nil {
		it.idx = makeIndex(buf, it.idx.ends)
	} else {
		it.idx = makeIndex[uint32](buf, nil)
	}
	return it
}

func (it *Iterator[T]) Close() {
	it.src = nil
	it.minv = 0
	it.base = 0
	it.ofs = 0
	it.align = 0
	it.len = 0
	it.sz = 0
	it.dec = nil
	// keep index
	putIterator(it)
}

func (it *Iterator[T]) Reset() {
	it.ofs = 0
}

func (it *Iterator[T]) Len() int {
	return it.len
}

func (it *Iterator[T]) Value(n int) T {
	if n < 0 || n >= it.len {
		return 0
	}
	if base := types.ChunkBase(n); base != it.base {
		it.fill(base)
	}
	return it.chunk[it.align+types.ChunkPos(n)]
}

func (it *Iterator[T]) Next() (*[CHUNK_SIZE]T, int) {
	// check EOF
	if it.ofs >= it.len {
		return nil, 0
	}

	// refill (considering seek/skip/reset state updates)
	n := min(CHUNK_SIZE, it.len-it.base)
	if base := types.ChunkBase(it.ofs); base != it.base {
		n = it.fill(base)
	}
	it.ofs = it.base + n

	// adjust chunk by new alignment
	return (*[CHUNK_SIZE]T)(unsafe.Pointer(&it.chunk[it.align])), n
}

func (it *Iterator[T]) Skip() int {
	n := min(CHUNK_SIZE, it.len-it.ofs)
	it.ofs += n
	return n
}

// seek to a given value position modulo chunk size
func (it *Iterator[T]) Seek(n int) bool {
	// bounds check
	if n < 0 || n >= it.len {
		it.ofs = it.len
		return false
	}

	// fill on seek to a new chunk
	if base := types.ChunkBase(n); base != it.base {
		it.fill(base)
	}

	// reset ofs to n, so call to Next() delivers the chunk
	it.ofs = n
	return true
}

func (it *Iterator[T]) fill(base int) int {
	// find code word for this chunk base
	word, skip, ok := it.idx.Find(base)
	if !ok {
		it.align = 0
		it.ofs = it.len
		it.base = -1
		return 0
	}

	// decode words into chunk until full or at end
	var (
		cnt  int
		p    = unsafe.Pointer(&it.chunk[0])
		stop = CHUNK_SIZE + skip
	)
	for cnt < stop && word+1 <= len(it.src)/8 {
		n := it.dec.DecodeWordPtr(p, len(it.chunk)-cnt, it.src[word*8:])
		word++
		p = unsafe.Add(p, n*it.sz)
		cnt += n
	}
	it.align = skip
	it.base = base

	return min(cnt-skip, CHUNK_SIZE)
}

func (it *Iterator[T]) All() iter.Seq2[int, T] {
	return func(yield func(int, T) bool) {
		for i := range it.len {
			// refill on chunk boundary
			n := i % CHUNK_SIZE
			if n == 0 {
				it.fill(i)
			}
			// yield
			if !yield(i, it.chunk[it.align+n]) {
				break
			}
		}
		it.Reset()
	}
}

func (it *Iterator[T]) Values() iter.Seq[T] {
	return func(yield func(T) bool) {
		for i := range it.len {
			// refill on chunk boundary
			n := i % CHUNK_SIZE
			if n == 0 {
				it.fill(i)
			}
			// yield
			if !yield(it.chunk[it.align+n]) {
				break
			}
		}
		it.Reset()
	}
}

type IteratorFactory struct {
	i64ItPool sync.Pool
	i32ItPool sync.Pool
	i16ItPool sync.Pool
	i8ItPool  sync.Pool
	u64ItPool sync.Pool
	u32ItPool sync.Pool
	u16ItPool sync.Pool
	u8ItPool  sync.Pool
}

func newIterator[T types.Integer]() *Iterator[T] {
	switch any(T(0)).(type) {
	case int64:
		return itFactory.i64ItPool.Get().(*Iterator[T])
	case int32:
		return itFactory.i32ItPool.Get().(*Iterator[T])
	case int16:
		return itFactory.i16ItPool.Get().(*Iterator[T])
	case int8:
		return itFactory.i8ItPool.Get().(*Iterator[T])
	case uint64:
		return itFactory.u64ItPool.Get().(*Iterator[T])
	case uint32:
		return itFactory.u32ItPool.Get().(*Iterator[T])
	case uint16:
		return itFactory.u16ItPool.Get().(*Iterator[T])
	case uint8:
		return itFactory.u8ItPool.Get().(*Iterator[T])
	default:
		return nil
	}
}

func putIterator[T types.Integer](c *Iterator[T]) {
	switch any(T(0)).(type) {
	case int64:
		itFactory.i64ItPool.Put(c)
	case int32:
		itFactory.i32ItPool.Put(c)
	case int16:
		itFactory.i16ItPool.Put(c)
	case int8:
		itFactory.i8ItPool.Put(c)
	case uint64:
		itFactory.u64ItPool.Put(c)
	case uint32:
		itFactory.u32ItPool.Put(c)
	case uint16:
		itFactory.u16ItPool.Put(c)
	case uint8:
		itFactory.u8ItPool.Put(c)
	}
}

var itFactory = IteratorFactory{
	i64ItPool: sync.Pool{New: func() any { return new(Iterator[int64]) }},
	i32ItPool: sync.Pool{New: func() any { return new(Iterator[int32]) }},
	i16ItPool: sync.Pool{New: func() any { return new(Iterator[int16]) }},
	i8ItPool:  sync.Pool{New: func() any { return new(Iterator[int8]) }},
	u64ItPool: sync.Pool{New: func() any { return new(Iterator[uint64]) }},
	u32ItPool: sync.Pool{New: func() any { return new(Iterator[uint32]) }},
	u16ItPool: sync.Pool{New: func() any { return new(Iterator[uint16]) }},
	u8ItPool:  sync.Pool{New: func() any { return new(Iterator[uint8]) }},
}
