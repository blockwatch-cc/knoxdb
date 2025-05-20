// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package encode

import (
	"unsafe"

	"blockwatch.cc/knoxdb/internal/cmp"
	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/slicex"
	"blockwatch.cc/knoxdb/pkg/util"
)

const (
	CHUNK_SIZE = 128 // must be pow2!
	CHUNK_MASK = CHUNK_SIZE - 1
)

func chunkBase(n int) int {
	return n &^ CHUNK_MASK
}

func chunkPos(n int) int {
	return n & CHUNK_MASK
}

// Iterators allow efficient seqential and random access to
// compressed vector data. They use an internal buffer to keep
// chunks of decoded data in L1 cache to minimize costs of
// linear and (limited range) random access.
//
// Users can use Next or NextChunk for linear walks, Get for
// point access and Seek or SkipChunk for jumping.
type NumberIterator[T types.Number] interface {
	// Returns the total number of elements in this container.
	Len() int

	// Returns next element and true if valid or false on EOF.
	// Implicitly decodes the next chunk.
	Next() (T, bool)

	// Returns element at position n or zero when out of bounds.
	// Implicitly seeks and decodes the chunk containing n.
	Get(int) T

	// Seeks to position n and decodes the relevant chunk.
	// Compatible with Next and Get.
	Seek(int) bool

	// Decodes and returns the next chunk at CHUNK_SIZE boundaries
	// and the number of valid elements in the chunk. Past EOF
	// returns nil and zero n. When used in combination with Next,
	// users must first Reset the iterator before calling NextChunk.
	NextChunk() (*[CHUNK_SIZE]T, int)

	// Skips a chunk efficiently without decoding data and returns
	// the number of elements skipped or zero when at EOF. Users may
	// call skip repeatedly before requesting data from NextChunk.
	SkipChunk() int

	// Resets iterator state so that NextChunk/SkipChunk or Next
	// restart at the first vector element.
	Reset()

	// Close releases pointers and allows for efficient re-use
	// of iterators. Users are encouraged to call Close after use
	// to reduce allocations and GC overhead.
	Close()
}

func matchIt[T types.Number](it NumberIterator[T], cmpFn unsafe.Pointer, val T, bits, mask *Bitset) {
	var (
		i   int
		cnt int64
		buf = bits.Bytes()
	)

	for {
		// check mask and skip chunks if not required
		if mask != nil && !mask.ContainsRange(i, i+CHUNK_SIZE-1) {
			n := it.SkipChunk()
			i += n
			if i >= it.Len() {
				break
			}
		}

		// get next chunk, on tail n may be < CHUNK_SZIE
		src, n := it.NextChunk()
		if n == 0 {
			break
		}

		// compare
		cnt += (*(*NumberMatchFunc[T])(cmpFn))(src[:n], val, buf[i>>3:])
		i += n
	}
	bits.ResetCount(int(cnt))
	it.Close()
}

func matchRangeIt[T types.Number](it NumberIterator[T], cmpFn unsafe.Pointer, a, b T, bits, mask *Bitset) {
	var (
		i   int
		cnt int64
		buf = bits.Bytes()
	)

	for {
		// check mask and skip chunks if not required
		if mask != nil && !mask.ContainsRange(i, i+CHUNK_SIZE-1) {
			n := it.SkipChunk()
			i += n
			if i >= it.Len() {
				break
			}
		}

		// get next chunk, on tail n may be < CHUNK_SZIE
		src, n := it.NextChunk()
		if n == 0 {
			break
		}

		// compare
		cnt += (*(*NumberRangeMatchFunc[T])(cmpFn))(src[:n], a, b, buf[i>>3:])
		i += n
	}
	bits.ResetCount(int(cnt))
	it.Close()
}

var (
	floatMatch64Fn = [...]unsafe.Pointer{
		nil,                                      // FilterModeInvalid
		unsafe.Pointer(&cmp.Float64Equal),        // FilterModeEqual
		unsafe.Pointer(&cmp.Float64NotEqual),     // FilterModeNotEqual
		unsafe.Pointer(&cmp.Float64Greater),      // FilterModeGt
		unsafe.Pointer(&cmp.Float64GreaterEqual), // FilterModeGe
		unsafe.Pointer(&cmp.Float64Less),         // FilterModeLt
		unsafe.Pointer(&cmp.Float64LessEqual),    // FilterModeLe
		nil,                                      // FilterModeIn
		nil,                                      // FilterModeNotIn
		unsafe.Pointer(&cmp.Float64Between),      // FilterModeRange
	}

	floatMatch32Fn = [...]unsafe.Pointer{
		nil,                                      // FilterModeInvalid
		unsafe.Pointer(&cmp.Float32Equal),        // FilterModeEqual
		unsafe.Pointer(&cmp.Float32NotEqual),     // FilterModeNotEqual
		unsafe.Pointer(&cmp.Float32Greater),      // FilterModeGt
		unsafe.Pointer(&cmp.Float32GreaterEqual), // FilterModeGe
		unsafe.Pointer(&cmp.Float32Less),         // FilterModeLt
		unsafe.Pointer(&cmp.Float32LessEqual),    // FilterModeLe
		nil,                                      // FilterModeIn
		nil,                                      // FilterModeNotIn
		unsafe.Pointer(&cmp.Float32Between),      // FilterModeRange
	}
)

func matchFn[T types.Float](mode types.FilterMode) unsafe.Pointer {
	if util.SizeOf[T]() == 8 {
		return floatMatch64Fn[mode]
	} else {
		return floatMatch32Fn[mode]
	}
}

// ---------------------------------
// Base Iterator
//

var _ NumberIterator[uint64] = (*BaseIterator[uint64])(nil)

type BaseIterator[T types.Number] struct {
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

func (it *BaseIterator[T]) Reset() {
	it.ofs = 0
}

func (it *BaseIterator[T]) Len() int {
	return it.len
}

func (it *BaseIterator[T]) Get(n int) T {
	if n < 0 || n >= it.len {
		return 0
	}
	if base := chunkBase(n); base != it.base {
		it.fill(base)
	}
	return it.chunk[chunkPos(n)]
}

func (it *BaseIterator[T]) Next() (T, bool) {
	if it.ofs >= it.len {
		// EOF
		return 0, false
	}

	// refill on chunk boundary
	if base := chunkBase(it.ofs); base != it.base {
		it.fill(base)
	}
	i := chunkPos(it.ofs)

	// advance ofs for next call
	it.ofs++

	// return calculated value
	return it.chunk[i], true
}

func (it *BaseIterator[T]) NextChunk() (*[CHUNK_SIZE]T, int) {
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

func (it *BaseIterator[T]) SkipChunk() int {
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

	// reset ofs to n, so call to Next() delivers value
	it.ofs = n
	return true
}

// Must be overloaed by derived implementations, left here for reference
// func (it *BaseIterator[T]) fill(base int) int {
// 	it.base = base
// 	return min(CHUNK_SIZE, it.len-it.base)
// }

// ---------------------------------
// Raw Iterator
//

var _ NumberIterator[uint64] = (*RawIterator[uint64])(nil)

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

func (it *RawIterator[T]) Get(n int) T {
	if n >= 0 && n < len(it.vals) {
		return it.vals[n]
	}
	return 0
}

func (it *RawIterator[T]) Next() (T, bool) {
	if it.ofs >= len(it.vals) {
		// EOF
		return 0, false
	}

	// advance ofs for next call
	it.ofs++

	// return value
	return it.vals[it.ofs-1], true
}

func (it *RawIterator[T]) NextChunk() (*[CHUNK_SIZE]T, int) {
	// EOF
	if it.ofs >= len(it.vals) {
		return nil, 0
	}
	base := chunkBase(it.ofs)
	n := min(CHUNK_SIZE, len(it.vals)-base)
	it.ofs += n
	return (*[CHUNK_SIZE]T)(unsafe.Pointer(&it.vals[base])), n
}

func (it *RawIterator[T]) SkipChunk() int {
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

// ---------------------------------
// Const Iterator
//

var _ NumberIterator[uint64] = (*ConstIterator[uint64])(nil)

type ConstIterator[T types.Number] struct {
	BaseIterator[T]
}

func NewConstIterator[T types.Number](val T, n int) *ConstIterator[T] {
	it := &ConstIterator[T]{
		BaseIterator: BaseIterator[T]{
			base: -1,
			len:  n,
		},
	}
	slicex.Fill(it.chunk[:], val)
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

var _ NumberIterator[uint64] = (*DeltaIterator[uint64])(nil)

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
