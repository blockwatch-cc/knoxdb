// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package s8b

import (
	"sync"

	"blockwatch.cc/knoxdb/internal/encode/s8b/generic"
	"blockwatch.cc/knoxdb/internal/types"
)

type Iterator[T types.Integer] struct {
	vals [128]T
	src  []byte
	minv T
	ofs  int // next src read offset
	len  int // total count of encoded values in src
	cnt  int // count of valid values in vals
	n    int // next value position in vals
}

func NewIterator[T types.Integer](buf []byte, n int, minv T) *Iterator[T] {
	if n <= 0 {
		n = CountValues(buf)
	}
	it := newIterator[T]()
	it.src = buf
	it.len = n
	it.minv = minv
	return it
}

func (it *Iterator[T]) Close() {
	it.src = nil
	it.ofs = 0
	it.len = 0
	it.minv = 0
	it.cnt = 0
	it.n = 0
	putIterator(it)
}

func (it *Iterator[T]) Reset() {
	it.ofs = 0
	it.cnt = 0
	it.n = 0
}

func (it *Iterator[T]) Len() int {
	return it.len
}

func (it *Iterator[T]) Get(n int) T {
	if it.Seek(n) {
		val, _ := it.Next()
		return val
	}
	return 0
}

func (it *Iterator[T]) Next() (T, bool) {
	if it.n == it.cnt {
		// EOF
		if it.ofs >= len(it.src) {
			return 0, false
		}

		// decode next encoded word
		n := generic.DecodeWord(it.vals[:], it.src[it.ofs:], it.minv)

		// sanity EOF check
		if n == 0 {
			it.ofs = len(it.src)
			return 0, false
		}

		// update pointers
		it.ofs += 8
		it.cnt = n
		it.n = 0
	}

	// advance n for next call
	it.n++

	// return value
	return it.vals[it.n-1], true
}

func (it *Iterator[T]) NextChunk() (*[128]T, int) {
	// EOF
	if it.ofs >= len(it.src) {
		return nil, 0
	}

	// TODO: update seek code before enabling this
	// attempt to fill the chunk as much as possible without overflow
	// it.n = 0
	// for it.ofs < len(it.src) && it.n+maxNPerSelector[it.src[it.ofs+7]>>4] < 128 {
	// 	n := generic.DecodeWord(it.vals[it.n:], it.src[it.ofs:])
	// 	it.ofs += 8
	// 	it.cnt += n
	// 	it.n += n
	// }

	// decode next encoded word
	n := generic.DecodeWord(it.vals[:], it.src[it.ofs:], it.minv)
	if n > 0 {
		it.ofs += 8
		it.cnt = n
		it.n = n // stay compatible with Next()
	}

	return &it.vals, it.n
}

func (it *Iterator[T]) SkipChunk() int {
	if it.ofs >= len(it.src) {
		return 0
	}
	it.ofs += 8
	it.cnt = 0
	it.n = 0
	return maxNPerSelector[it.src[it.ofs-1]>>4]
}

// seek to a given value position
func (it *Iterator[T]) Seek(n int) bool {
	if n < 0 || n >= it.len {
		it.ofs = len(it.src)
		it.n = 0
		it.cnt = 0
		return false
	}
	// TODO: identify matches in same code word and prevent expensive seek & decode
	bn, cn := generic.Seek(it.src, n)
	if bn < 0 {
		it.ofs = len(it.src)
		return false
	}
	it.ofs = bn
	it.n = 0
	it.cnt = 0
	if cn > 0 {
		it.NextChunk()
		it.n = cn
	}
	return true
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
