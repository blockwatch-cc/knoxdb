// Copyright (c) 2018-2023 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package dedup

import (
	"blockwatch.cc/knoxdb/internal/arena"
)

func toSlice(a ByteArray) [][]byte {
	res := arena.AllocByteSlice(a.Len())
	for i := 0; i < a.Len(); i++ {
		res = append(res, a.Elem(i))
	}
	return res
}

func toSubSlice(a ByteArray, start, end int) [][]byte {
	res := arena.AllocByteSlice(end - start)
	for i := start; i < end; i++ {
		res = append(res, a.Elem(i))
	}
	return res
}

func recycle(buf [][]byte) {
	arena.Free(buf[:0])
}

func uvarIntLen(n int) int {
	i := 0
	for n >= 0x80 {
		n >>= 7
		i++
	}
	return i + 1
}

func heapSize(vals [][]byte) (sz int64) {
	for _, v := range vals {
		sz += int64(len(v) + 24)
	}
	return
}
