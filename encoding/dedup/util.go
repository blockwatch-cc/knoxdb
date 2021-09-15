// Copyright (c) 2018-2021 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package dedup

func toSlice(a ByteArray) [][]byte {
	var res [][]byte
	if a.Len() <= DefaultMaxPointsPerBlock {
		res = bytesPool.Get().([][]byte)[:0]
	} else {
		res = make([][]byte, 0, a.Len())
	}
	for i := 0; i < a.Len(); i++ {
		res = append(res, a.Elem(i))
	}
	return res
}

func toSubSlice(a ByteArray, start, end int) [][]byte {
	var res [][]byte
	if end-start <= DefaultMaxPointsPerBlock {
		res = bytesPool.Get().([][]byte)[:0]
	} else {
		res = make([][]byte, 0, end-start)
	}
	for i := start; i < end; i++ {
		res = append(res, a.Elem(i))
	}
	return res
}

func uvarIntLen(n int) int {
	i := 0
	for n >= 0x80 {
		n >>= 7
		i++
	}
	return i + 1
}
