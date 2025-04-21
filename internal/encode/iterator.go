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
	Seek(n int) bool
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
