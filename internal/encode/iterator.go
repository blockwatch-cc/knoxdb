// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package encode

import (
	"unsafe"

	"blockwatch.cc/knoxdb/internal/cmp"
	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/util"
)

const (
	CHUNK_SIZE = 128 // must be pow2!
	CHUNK_MASK = CHUNK_SIZE - 1
)

func chunkStart(n int) int {
	return n &^ CHUNK_MASK
}

type Iterator[T types.Number] interface {
	Len() int
	Next() (T, bool)
	Get(int) T
	Seek(int) bool
	NextChunk() (*[CHUNK_SIZE]T, int)
	SkipChunk() int
	Reset()
	Close()
}

func matchIt[T types.Number](it Iterator[T], cmpFn unsafe.Pointer, val T, bits, mask *Bitset) {
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

func matchRangeIt[T types.Number](it Iterator[T], cmpFn unsafe.Pointer, a, b T, bits, mask *Bitset) {
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
// Raw Iterator
//

var _ Iterator[uint64] = (*RawIterator[uint64])(nil)

type RawIterator[T types.Number] struct {
	vals []T
	ofs  int
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
	pos := it.ofs
	n := min(CHUNK_SIZE, len(it.vals)-it.ofs)
	it.ofs += n
	return (*[CHUNK_SIZE]T)(unsafe.Pointer(&it.vals[pos])), n
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

var _ Iterator[uint64] = (*ConstIterator[uint64])(nil)

type ConstIterator[T types.Number] struct {
	vals [CHUNK_SIZE]T
	ofs  int
	len  int
}

func (it *ConstIterator[T]) Close() {
	// noop
}

func (it *ConstIterator[T]) Reset() {
	it.ofs = 0
}

func (it *ConstIterator[T]) Len() int {
	return it.len
}

func (it *ConstIterator[T]) Get(n int) T {
	return it.vals[0]
}

func (it *ConstIterator[T]) Next() (T, bool) {
	if it.ofs >= it.len {
		// EOF
		return 0, false
	}

	// advance ofs for next call
	it.ofs++

	return it.vals[0], true
}

func (it *ConstIterator[T]) NextChunk() (*[CHUNK_SIZE]T, int) {
	// EOF
	if it.ofs >= it.len {
		return nil, 0
	}
	n := min(CHUNK_SIZE, it.len-it.ofs)
	it.ofs += n
	return &it.vals, n
}

func (it *ConstIterator[T]) SkipChunk() int {
	n := min(CHUNK_SIZE, it.len-it.ofs)
	it.ofs += n
	return n
}

func (it *ConstIterator[T]) Seek(n int) bool {
	if n < 0 || n >= it.len {
		it.ofs = it.len
		return false
	}
	// reset ofs to n, so call to Next() delivers value
	it.ofs = n
	return true
}

// ---------------------------------
// Raw Iterator
//

var _ Iterator[uint64] = (*DeltaIterator[uint64])(nil)

type DeltaIterator[T types.Integer] struct {
	vals  [CHUNK_SIZE]T
	delta T
	ffor  T
	ofs   int
	len   int
}

func (it *DeltaIterator[T]) Close() {
	// noop
}

func (it *DeltaIterator[T]) Reset() {
	it.ofs = 0
}

func (it *DeltaIterator[T]) Len() int {
	return it.len
}

func (it *DeltaIterator[T]) Get(n int) T {
	if n >= 0 && n < it.len {
		return T(n)*it.delta + it.ffor
	}
	return 0
}

func (it *DeltaIterator[T]) Next() (T, bool) {
	if it.ofs >= it.len {
		// EOF
		return 0, false
	}

	// refill with values at this offset
	if it.ofs&CHUNK_MASK == 0 {
		it.fill()
	}

	// advance ofs for next call
	it.ofs++

	// return value
	return T(it.ofs-1)*it.delta + it.ffor, true
}

func (it *DeltaIterator[T]) NextChunk() (*[CHUNK_SIZE]T, int) {
	// EOF
	if it.ofs >= it.len {
		return nil, 0
	}

	// refill at this offset
	it.fill()

	// calculate chunk or tail size
	n := min(CHUNK_SIZE, it.len-it.ofs)
	it.ofs += n
	return &it.vals, n
}

func (it *DeltaIterator[T]) SkipChunk() int {
	n := min(CHUNK_SIZE, it.len-it.ofs)
	it.ofs += n
	return n
}

func (it *DeltaIterator[T]) Seek(n int) bool {
	if n < 0 || n >= it.len {
		it.ofs = it.len
		return false
	}

	// calculate chunk start offsets for n and current offset
	nc := chunkStart(n)
	oc := chunkStart(it.ofs)

	// fill when seek is first call or n is in another chunk
	if nc != oc || it.ofs == 0 {
		it.ofs = nc
		it.fill()
	}

	// reset ofs to n, so call to Next() delivers value
	it.ofs = n
	return true
}

func (it *DeltaIterator[T]) fill() {
	var i int
	for range CHUNK_SIZE / 8 {
		it.vals[i] = T(i+it.ofs)*it.delta + it.ffor
		it.vals[i+1] = T(i+1+it.ofs)*it.delta + it.ffor
		it.vals[i+2] = T(i+2+it.ofs)*it.delta + it.ffor
		it.vals[i+3] = T(i+3+it.ofs)*it.delta + it.ffor
		it.vals[i+4] = T(i+4+it.ofs)*it.delta + it.ffor
		it.vals[i+5] = T(i+5+it.ofs)*it.delta + it.ffor
		it.vals[i+6] = T(i+6+it.ofs)*it.delta + it.ffor
		it.vals[i+7] = T(i+7+it.ofs)*it.delta + it.ffor
		i += 8
	}
}
