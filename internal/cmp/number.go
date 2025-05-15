// Copyright (c) 2023-2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package cmp

import (
	"math/bits"

	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/util"
)

func cmp_eq[T types.Integer](src []T, val T, res []byte) int64 {
	var cnt int64
	n := len(src) / 8
	var idx int
	for i := range n {
		a1 := src[idx] == val
		a2 := src[idx+1] == val
		a3 := src[idx+2] == val
		a4 := src[idx+3] == val
		// note: bitset bytes store bits inverted for efficient index algo
		b := util.Bool2byte(a1) + util.Bool2byte(a2)<<1 + util.Bool2byte(a3)<<2 + util.Bool2byte(a4)<<3
		a1 = src[idx+4] == val
		a2 = src[idx+5] == val
		a3 = src[idx+6] == val
		a4 = src[idx+7] == val
		b += util.Bool2byte(a1)<<4 + util.Bool2byte(a2)<<5 + util.Bool2byte(a3)<<6 + util.Bool2byte(a4)<<7
		res[i] = b
		cnt += int64(bits.OnesCount8(b))
		idx += 8
	}

	// tail
	if len(src)%8 > 0 {
		for i, v := range src[idx:] {
			if v == val {
				res[n] |= 0x1 << i
				cnt++
			}
		}
	}
	return cnt
}

func cmp_ne[T types.Integer](src []T, val T, res []byte) int64 {
	var cnt int64
	n := len(src) / 8
	var idx int
	for i := range n {
		a1 := src[idx] != val
		a2 := src[idx+1] != val
		a3 := src[idx+2] != val
		a4 := src[idx+3] != val
		// note: bitset bytes store bits inverted for efficient index algo
		b := util.Bool2byte(a1) + util.Bool2byte(a2)<<1 + util.Bool2byte(a3)<<2 + util.Bool2byte(a4)<<3
		a1 = src[idx+4] != val
		a2 = src[idx+5] != val
		a3 = src[idx+6] != val
		a4 = src[idx+7] != val
		b += util.Bool2byte(a1)<<4 + util.Bool2byte(a2)<<5 + util.Bool2byte(a3)<<6 + util.Bool2byte(a4)<<7
		res[i] = b
		cnt += int64(bits.OnesCount8(b))
		idx += 8
	}

	// tail
	if len(src)%8 > 0 {
		for i, v := range src[idx:] {
			if v != val {
				res[n] |= 0x1 << i
				cnt++
			}
		}
	}
	return cnt
}

func cmp_lt[T types.Integer](src []T, val T, res []byte) int64 {
	var cnt int64
	n := len(src) / 8
	var idx int
	for i := range n {
		a1 := src[idx] < val
		a2 := src[idx+1] < val
		a3 := src[idx+2] < val
		a4 := src[idx+3] < val
		// note: bitset bytes store bits inverted for efficient index algo
		b := util.Bool2byte(a1) + util.Bool2byte(a2)<<1 + util.Bool2byte(a3)<<2 + util.Bool2byte(a4)<<3
		a1 = src[idx+4] < val
		a2 = src[idx+5] < val
		a3 = src[idx+6] < val
		a4 = src[idx+7] < val
		b += util.Bool2byte(a1)<<4 + util.Bool2byte(a2)<<5 + util.Bool2byte(a3)<<6 + util.Bool2byte(a4)<<7
		res[i] = b
		cnt += int64(bits.OnesCount8(b))
		idx += 8
	}

	// tail
	if len(src)%8 > 0 {
		for i, v := range src[idx:] {
			if v < val {
				res[n] |= 0x1 << i
				cnt++
			}
		}
	}
	return cnt
}

func cmp_le[T types.Integer](src []T, val T, res []byte) int64 {
	var cnt int64
	n := len(src) / 8
	var idx int
	for i := range n {
		a1 := src[idx] <= val
		a2 := src[idx+1] <= val
		a3 := src[idx+2] <= val
		a4 := src[idx+3] <= val
		// note: bitset bytes store bits inverted for efficient index algo
		b := util.Bool2byte(a1) + util.Bool2byte(a2)<<1 + util.Bool2byte(a3)<<2 + util.Bool2byte(a4)<<3
		a1 = src[idx+4] <= val
		a2 = src[idx+5] <= val
		a3 = src[idx+6] <= val
		a4 = src[idx+7] <= val
		b += util.Bool2byte(a1)<<4 + util.Bool2byte(a2)<<5 + util.Bool2byte(a3)<<6 + util.Bool2byte(a4)<<7
		res[i] = b
		cnt += int64(bits.OnesCount8(b))
		idx += 8
	}

	// tail
	if len(src)%8 > 0 {
		for i, v := range src[idx:] {
			if v <= val {
				res[n] |= 0x1 << i
				cnt++
			}
		}
	}
	return cnt
}

func cmp_gt[T types.Integer](src []T, val T, res []byte) int64 {
	var cnt int64
	n := len(src) / 8
	var idx int
	for i := range n {
		a1 := src[idx] > val
		a2 := src[idx+1] > val
		a3 := src[idx+2] > val
		a4 := src[idx+3] > val
		// note: bitset bytes store bits inverted for efficient index algo
		b := util.Bool2byte(a1) + util.Bool2byte(a2)<<1 + util.Bool2byte(a3)<<2 + util.Bool2byte(a4)<<3
		a1 = src[idx+4] > val
		a2 = src[idx+5] > val
		a3 = src[idx+6] > val
		a4 = src[idx+7] > val
		b += util.Bool2byte(a1)<<4 + util.Bool2byte(a2)<<5 + util.Bool2byte(a3)<<6 + util.Bool2byte(a4)<<7
		res[i] = b
		cnt += int64(bits.OnesCount8(b))
		idx += 8
	}

	// tail
	if len(src)%8 > 0 {
		for i, v := range src[idx:] {
			if v > val {
				res[n] |= 0x1 << i
				cnt++
			}
		}
	}
	return cnt
}

func cmp_ge[T types.Integer](src []T, val T, res []byte) int64 {
	var cnt int64
	n := len(src) / 8
	var idx int
	for i := range n {
		a1 := src[idx] >= val
		a2 := src[idx+1] >= val
		a3 := src[idx+2] >= val
		a4 := src[idx+3] >= val
		// note: bitset bytes store bits inverted for efficient index algo
		b := util.Bool2byte(a1) + util.Bool2byte(a2)<<1 + util.Bool2byte(a3)<<2 + util.Bool2byte(a4)<<3
		a1 = src[idx+4] >= val
		a2 = src[idx+5] >= val
		a3 = src[idx+6] >= val
		a4 = src[idx+7] >= val
		b += util.Bool2byte(a1)<<4 + util.Bool2byte(a2)<<5 + util.Bool2byte(a3)<<6 + util.Bool2byte(a4)<<7
		res[i] = b
		cnt += int64(bits.OnesCount8(b))
		idx += 8
	}

	// tail
	if len(src)%8 > 0 {
		for i, v := range src[idx:] {
			if v >= val {
				res[n] |= 0x1 << i
				cnt++
			}
		}
	}
	return cnt
}

func cmp_bw[T types.Integer, U types.Unsigned](src []T, a, b T, res []byte) int64 {
	diff := U(b - a)
	var cnt int64
	n := len(src) / 8
	var idx int
	for i := range n {
		a1 := U(src[idx]-a) <= diff
		a2 := U(src[idx+1]-a) <= diff
		a3 := U(src[idx+2]-a) <= diff
		a4 := U(src[idx+3]-a) <= diff
		// note: bitset bytes store bits inverted for efficient index algo
		x := util.Bool2byte(a1) + util.Bool2byte(a2)<<1 + util.Bool2byte(a3)<<2 + util.Bool2byte(a4)<<3
		a1 = U(src[idx+4]-a) <= diff
		a2 = U(src[idx+5]-a) <= diff
		a3 = U(src[idx+6]-a) <= diff
		a4 = U(src[idx+7]-a) <= diff
		x += util.Bool2byte(a1)<<4 + util.Bool2byte(a2)<<5 + util.Bool2byte(a3)<<6 + util.Bool2byte(a4)<<7
		res[i] = x
		cnt += int64(bits.OnesCount8(x))
		idx += 8
	}

	// tail
	if len(src)%8 > 0 {
		for i, v := range src[idx:] {
			if U(v-a) <= diff {
				res[n] |= 0x1 << i
				cnt++
			}
		}
	}
	return cnt
}
