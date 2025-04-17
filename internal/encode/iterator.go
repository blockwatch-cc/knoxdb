// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package encode

import (
	"unsafe"

	"blockwatch.cc/knoxdb/internal/types"
)

type Iterator[T types.Number] interface {
	Len() int
	Next() (T, bool)
	Seek(n int) bool
	NextChunk() (*[CHUNK_SIZE]T, int)
	SkipChunk()
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
			it.SkipChunk()
			i += CHUNK_SIZE
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
			it.SkipChunk()
			i += CHUNK_SIZE
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
}
