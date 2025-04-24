// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package s8b

import (
	"sync"

	"blockwatch.cc/knoxdb/internal/encode/s8b/generic"
	"blockwatch.cc/knoxdb/internal/types"
)

// keep in sync with main encode package
const (
	CHUNK_SIZE = 128 // must be pow2!
)

type Iterator[T types.Integer] struct {
	chunk [CHUNK_SIZE]T
	src   []byte // s8b encoded uint64 words
	minv  T      // min-FOR value
	base  int    // index of first value in chunk
	len   int    // total count of encoded values in src
	ofs   int    // next value index in vector
	cnt   int    // count of valid values in chunk
	read  int    // next src read offset
}

func NewIterator[T types.Integer](buf []byte, n int, minv T) *Iterator[T] {
	if n <= 0 {
		n = CountValues(buf)
	}
	it := newIterator[T]()
	it.base = -1
	it.src = buf
	it.len = n
	it.minv = minv
	return it
}

func (it *Iterator[T]) Close() {
	it.src = nil
	it.minv = 0
	it.base = 0
	it.ofs = 0
	it.len = 0
	it.cnt = 0
	it.read = 0
	putIterator(it)
}

func (it *Iterator[T]) Reset() {
	it.ofs = 0
}

func (it *Iterator[T]) Len() int {
	return it.len
}

func (it *Iterator[T]) Get(n int) T {
	if n < 0 || n >= it.len {
		return 0
	}
	if uint(n-it.base) >= uint(it.cnt) {
		it.fill(n)
	}
	return it.chunk[n-it.base]
}

func (it *Iterator[T]) Next() (T, bool) {
	// EOF
	if it.ofs >= it.len {
		return 0, false
	}

	// refill on chunk boundary
	if uint(it.ofs-it.base) >= uint(it.cnt) {
		it.fill(it.ofs)
	}

	// advance n for next call
	i := it.ofs - it.base
	it.ofs++

	return it.chunk[i], true
}

func (it *Iterator[T]) NextChunk() (*[CHUNK_SIZE]T, int) {
	// EOF
	if it.ofs >= it.len {
		return nil, 0
	}

	// refill from current end of chunk (note: base inits at -1)
	n := it.fill(max(0, it.ofs))
	it.ofs = it.base + n

	return &it.chunk, n
}

func (it *Iterator[T]) SkipChunk() int {
	maxn := min(CHUNK_SIZE, it.len-it.ofs)
	srcIdx := it.read + 7

	// skip next 128 (or less) values at code word granularity
	var n int
	for srcIdx < len(it.src) {
		sn := maxNPerSelector[it.src[srcIdx]>>4]
		if sn+n > maxn {
			break
		}
		n += sn
		srcIdx += 8
	}
	it.ofs += n
	return n
}

// seek to a given value position
func (it *Iterator[T]) Seek(n int) bool {
	// bounds check
	if n < 0 || n >= it.len {
		it.ofs = it.len
		return false
	}

	// fill on seek to an unloaded chunk
	if uint(n-it.base) >= uint(it.cnt) {
		it.fill(n)
	}

	// reset ofs to n, so call to Next() delivers value
	it.ofs = n
	return true
}

func (it *Iterator[T]) fill(idx int) int {
	// ideally we continue reading at start of next code word
	srcIdx, srcPos := it.read, 0

	// on seek however we may have to jump to a different codeword
	if idx != it.base+it.cnt {
		srcIdx, srcPos = generic.Seek(it.src, idx)
	}

	// attempt to fill chunk as much as possible without overflow,
	// peek into next selector to count codewords
	it.cnt = 0
	for srcIdx+7 < len(it.src) && it.cnt+maxNPerSelector[it.src[srcIdx+7]>>4] <= CHUNK_SIZE {
		n := generic.DecodeWord(it.chunk[it.cnt:], it.src[srcIdx:], it.minv)
		// if n == 0 {
		// 	panic(fmt.Errorf("decoding word %d %x sel=%d failed",
		// 		srcIdx,
		// 		binary.LittleEndian.Uint64(it.src[srcIdx:]),
		// 		it.src[srcIdx+7]>>4,
		// 	))
		// }
		srcIdx += 8
		it.cnt += n
	}

	it.read = srcIdx
	it.base = idx - srcPos // chunk starts at code word
	it.ofs = idx           // for exact seeks

	return it.cnt
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
