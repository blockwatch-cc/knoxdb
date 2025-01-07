// Copyright (c) 2023-2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package generic

import (
	"math/bits"

	"golang.org/x/exp/constraints"
)

type Float interface {
	constraints.Float
}

func MatchFloatEqual[T Float](src []T, val T, res []byte) int64 {
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

func MatchFloatNotEqual[T Float](src []T, val T, res []byte) int64 {
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

func MatchFloatLess[T Float](src []T, val T, res []byte) int64 {
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

func MatchFloatLessEqual[T Float](src []T, val T, res []byte) int64 {
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

func MatchFloatGreater[T Float](src []T, val T, res []byte) int64 {
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

func MatchFloatGreaterEqual[T Float](src []T, val T, res []byte) int64 {
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

func MatchFloatBetween[T Float](src []T, a, b T, res []byte) int64 {
	var cnt int64
	n := len(src) / 8
	for i := 0; i < n; i++ {
		idx := i * 8
		a1 := a <= src[idx] && src[idx] <= b
		a2 := a <= src[idx+1] && src[idx+1] <= b
		a3 := a <= src[idx+2] && src[idx+2] <= b
		a4 := a <= src[idx+3] && src[idx+3] <= b
		// note: bitset bytes store bits inverted for efficient index algo
		x := b2u(a1) + b2u(a2)<<1 + b2u(a3)<<2 + b2u(a4)<<3
		a1 = a <= src[idx+4] && src[idx+4] <= b
		a2 = a <= src[idx+5] && src[idx+5] <= b
		a3 = a <= src[idx+6] && src[idx+6] <= b
		a4 = a <= src[idx+7] && src[idx+7] <= b
		x += b2u(a1)<<4 + b2u(a2)<<5 + b2u(a3)<<6 + b2u(a4)<<7
		res[i] = x
		cnt += int64(bits.OnesCount8(x))
	}

	// tail
	if len(src)%8 > 0 {
		for i, v := range src[n*8:] {
			if a <= v && v <= b {
				res[n] |= 0x1 << i
				cnt++
			}
		}
	}
	return cnt
}
