// Copyright (c) 2023-2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package generic

import (
	"math/bits"

	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/util"
)

func MatchEqual[T types.Integer](src []T, val T, res []byte) int64 {
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

func MatchNotEqual[T types.Integer](src []T, val T, res []byte) int64 {
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

func MatchLess[T types.Integer](src []T, val T, res []byte) int64 {
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

func MatchLessEqual[T types.Integer](src []T, val T, res []byte) int64 {
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

func MatchGreater[T types.Integer](src []T, val T, res []byte) int64 {
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

func MatchGreaterEqual[T types.Integer](src []T, val T, res []byte) int64 {
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

func MatchBetweenUnsigned[T types.Unsigned](src []T, a, b T, res []byte) int64 {
	diff := uint64(b) - uint64(a) + 1
	var cnt int64
	n := len(src) / 8
	var idx int
	for i := range n {
		a1 := uint64(src[idx])-uint64(a) < diff
		a2 := uint64(src[idx+1])-uint64(a) < diff
		a3 := uint64(src[idx+2])-uint64(a) < diff
		a4 := uint64(src[idx+3])-uint64(a) < diff
		// note: bitset bytes store bits inverted for efficient index algo
		x := util.Bool2byte(a1) + util.Bool2byte(a2)<<1 + util.Bool2byte(a3)<<2 + util.Bool2byte(a4)<<3
		a1 = uint64(src[idx+4])-uint64(a) < diff
		a2 = uint64(src[idx+5])-uint64(a) < diff
		a3 = uint64(src[idx+6])-uint64(a) < diff
		a4 = uint64(src[idx+7])-uint64(a) < diff
		x += util.Bool2byte(a1)<<4 + util.Bool2byte(a2)<<5 + util.Bool2byte(a3)<<6 + util.Bool2byte(a4)<<7
		res[i] = x
		cnt += int64(bits.OnesCount8(x))
		idx += 8
	}

	// tail
	if len(src)%8 > 0 {
		for i, v := range src[idx:] {
			if uint64(v)-uint64(a) < diff {
				res[n] |= 0x1 << i
				cnt++
			}
		}
	}
	return cnt
}

func MatchBetweenSigned[T types.Signed](src []T, a, b T, res []byte) int64 {
	diff := uint64(int64(b) - int64(a) + 1)
	var cnt int64
	n := len(src) / 8
	var idx int
	for i := range n {
		a1 := uint64(int64(src[idx])-int64(a)) < diff
		a2 := uint64(int64(src[idx+1])-int64(a)) < diff
		a3 := uint64(int64(src[idx+2])-int64(a)) < diff
		a4 := uint64(int64(src[idx+3])-int64(a)) < diff
		// note: bitset bytes store bits inverted for efficient index algo
		x := util.Bool2byte(a1) + util.Bool2byte(a2)<<1 + util.Bool2byte(a3)<<2 + util.Bool2byte(a4)<<3
		a1 = uint64(int64(src[idx+4])-int64(a)) < diff
		a2 = uint64(int64(src[idx+5])-int64(a)) < diff
		a3 = uint64(int64(src[idx+6])-int64(a)) < diff
		a4 = uint64(int64(src[idx+7])-int64(a)) < diff
		x += util.Bool2byte(a1)<<4 + util.Bool2byte(a2)<<5 + util.Bool2byte(a3)<<6 + util.Bool2byte(a4)<<7
		res[i] = x
		cnt += int64(bits.OnesCount8(x))
		idx += 8
	}

	// tail
	if len(src)%8 > 0 {
		for i, v := range src[idx:] {
			if uint64(int64(v)-int64(a)) < diff {
				res[n] |= 0x1 << i
				cnt++
			}
		}
	}
	return cnt
}
