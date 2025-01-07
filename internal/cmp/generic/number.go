// Copyright (c) 2023-2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package generic

import (
	"math/bits"

	"golang.org/x/exp/constraints"
)

type Number interface {
	constraints.Integer
}

func MatchEqual[T Number](src []T, val T, res []byte) int64 {
	var cnt int64
	n := len(src) / 8
	for i := 0; i < n; i++ {
		idx := i * 8
		a1 := src[idx] == val
		a2 := src[idx+1] == val
		a3 := src[idx+2] == val
		a4 := src[idx+3] == val
		// note: bitset bytes store bits inverted for efficient index algo
		b := b2u(a1) + b2u(a2)<<1 + b2u(a3)<<2 + b2u(a4)<<3
		a1 = src[idx+4] == val
		a2 = src[idx+5] == val
		a3 = src[idx+6] == val
		a4 = src[idx+7] == val
		b += b2u(a1)<<4 + b2u(a2)<<5 + b2u(a3)<<6 + b2u(a4)<<7
		res[i] = b
		cnt += int64(bits.OnesCount8(b))
	}

	// tail
	if len(src)%8 > 0 {
		for i, v := range src[n*8:] {
			if v == val {
				res[n] |= 0x1 << i
				cnt++
			}
		}
	}
	return cnt
}

func MatchNotEqual[T Number](src []T, val T, res []byte) int64 {
	var cnt int64
	n := len(src) / 8
	for i := 0; i < n; i++ {
		idx := i * 8
		a1 := src[idx] != val
		a2 := src[idx+1] != val
		a3 := src[idx+2] != val
		a4 := src[idx+3] != val
		// note: bitset bytes store bits inverted for efficient index algo
		b := b2u(a1) + b2u(a2)<<1 + b2u(a3)<<2 + b2u(a4)<<3
		a1 = src[idx+4] != val
		a2 = src[idx+5] != val
		a3 = src[idx+6] != val
		a4 = src[idx+7] != val
		b += b2u(a1)<<4 + b2u(a2)<<5 + b2u(a3)<<6 + b2u(a4)<<7
		res[i] = b
		cnt += int64(bits.OnesCount8(b))
	}

	// tail
	if len(src)%8 > 0 {
		for i, v := range src[n*8:] {
			if v != val {
				res[n] |= 0x1 << i
				cnt++
			}
		}
	}
	return cnt
}

func MatchLess[T Number](src []T, val T, res []byte) int64 {
	var cnt int64
	n := len(src) / 8
	for i := 0; i < n; i++ {
		idx := i * 8
		a1 := src[idx] < val
		a2 := src[idx+1] < val
		a3 := src[idx+2] < val
		a4 := src[idx+3] < val
		// note: bitset bytes store bits inverted for efficient index algo
		b := b2u(a1) + b2u(a2)<<1 + b2u(a3)<<2 + b2u(a4)<<3
		a1 = src[idx+4] < val
		a2 = src[idx+5] < val
		a3 = src[idx+6] < val
		a4 = src[idx+7] < val
		b += b2u(a1)<<4 + b2u(a2)<<5 + b2u(a3)<<6 + b2u(a4)<<7
		res[i] = b
		cnt += int64(bits.OnesCount8(b))
	}

	// tail
	if len(src)%8 > 0 {
		for i, v := range src[n*8:] {
			if v < val {
				res[n] |= 0x1 << i
				cnt++
			}
		}
	}
	return cnt
}

func MatchLessEqual[T Number](src []T, val T, res []byte) int64 {
	var cnt int64
	n := len(src) / 8
	for i := 0; i < n; i++ {
		idx := i * 8
		a1 := src[idx] <= val
		a2 := src[idx+1] <= val
		a3 := src[idx+2] <= val
		a4 := src[idx+3] <= val
		// note: bitset bytes store bits inverted for efficient index algo
		b := b2u(a1) + b2u(a2)<<1 + b2u(a3)<<2 + b2u(a4)<<3
		a1 = src[idx+4] <= val
		a2 = src[idx+5] <= val
		a3 = src[idx+6] <= val
		a4 = src[idx+7] <= val
		b += b2u(a1)<<4 + b2u(a2)<<5 + b2u(a3)<<6 + b2u(a4)<<7
		res[i] = b
		cnt += int64(bits.OnesCount8(b))
	}

	// tail
	if len(src)%8 > 0 {
		for i, v := range src[n*8:] {
			if v <= val {
				res[n] |= 0x1 << i
				cnt++
			}
		}
	}
	return cnt
}

func MatchGreater[T Number](src []T, val T, res []byte) int64 {
	var cnt int64
	n := len(src) / 8
	for i := 0; i < n; i++ {
		idx := i * 8
		a1 := src[idx] > val
		a2 := src[idx+1] > val
		a3 := src[idx+2] > val
		a4 := src[idx+3] > val
		// note: bitset bytes store bits inverted for efficient index algo
		b := b2u(a1) + b2u(a2)<<1 + b2u(a3)<<2 + b2u(a4)<<3
		a1 = src[idx+4] > val
		a2 = src[idx+5] > val
		a3 = src[idx+6] > val
		a4 = src[idx+7] > val
		b += b2u(a1)<<4 + b2u(a2)<<5 + b2u(a3)<<6 + b2u(a4)<<7
		res[i] = b
		cnt += int64(bits.OnesCount8(b))
	}

	// tail
	if len(src)%8 > 0 {
		for i, v := range src[n*8:] {
			if v > val {
				res[n] |= 0x1 << i
				cnt++
			}
		}
	}
	return cnt
}

func MatchGreaterEqual[T Number](src []T, val T, res []byte) int64 {
	var cnt int64
	n := len(src) / 8
	for i := 0; i < n; i++ {
		idx := i * 8
		a1 := src[idx] >= val
		a2 := src[idx+1] >= val
		a3 := src[idx+2] >= val
		a4 := src[idx+3] >= val
		// note: bitset bytes store bits inverted for efficient index algo
		b := b2u(a1) + b2u(a2)<<1 + b2u(a3)<<2 + b2u(a4)<<3
		a1 = src[idx+4] >= val
		a2 = src[idx+5] >= val
		a3 = src[idx+6] >= val
		a4 = src[idx+7] >= val
		b += b2u(a1)<<4 + b2u(a2)<<5 + b2u(a3)<<6 + b2u(a4)<<7
		res[i] = b
		cnt += int64(bits.OnesCount8(b))
	}

	// tail
	if len(src)%8 > 0 {
		for i, v := range src[n*8:] {
			if v >= val {
				res[n] |= 0x1 << i
				cnt++
			}
		}
	}
	return cnt
}

func MatchBetween[T Number](src []T, a, b T, res []byte) int64 {
	diff := uint64(b - a + 1)
	var cnt int64
	n := len(src) / 8
	for i := 0; i < n; i++ {
		idx := i * 8
		a1 := uint64(src[idx]-a) < diff
		a2 := uint64(src[idx+1]-a) < diff
		a3 := uint64(src[idx+2]-a) < diff
		a4 := uint64(src[idx+3]-a) < diff
		// note: bitset bytes store bits inverted for efficient index algo
		x := b2u(a1) + b2u(a2)<<1 + b2u(a3)<<2 + b2u(a4)<<3
		a1 = uint64(src[idx+4]-a) < diff
		a2 = uint64(src[idx+5]-a) < diff
		a3 = uint64(src[idx+6]-a) < diff
		a4 = uint64(src[idx+7]-a) < diff
		x += b2u(a1)<<4 + b2u(a2)<<5 + b2u(a3)<<6 + b2u(a4)<<7
		res[i] = x
		cnt += int64(bits.OnesCount8(x))
	}

	// tail
	if len(src)%8 > 0 {
		for i, v := range src[n*8:] {
			if uint64(v-a) < diff {
				res[n] |= 0x1 << i
				cnt++
			}
		}
	}
	return cnt
}
