// Copyright (c) 2023 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package generic

import (
	"golang.org/x/exp/constraints"
)

type Number interface {
	constraints.Integer
}

func MatchEqual[T Number](src []T, val T, bits []byte) int64 {
	var cnt int64
	for i, v := range src {
		if v == val {
			bits[i>>3] |= 0x1 << uint(i&0x7)
			cnt++
		}
	}
	return cnt
}

func MatchNotEqual[T Number](src []T, val T, bits []byte) int64 {
	var cnt int64
	for i, v := range src {
		if v != val {
			bits[i>>3] |= 0x1 << uint(i&0x7)
			cnt++
		}
	}
	return cnt
}

func MatchLess[T Number](src []T, val T, bits []byte) int64 {
	var cnt int64
	for i, v := range src {
		if v < val {
			bits[i>>3] |= 0x1 << uint(i&0x7)
			cnt++
		}
	}
	return cnt
}

func MatchLessEqual[T Number](src []T, val T, bits []byte) int64 {
	var cnt int64
	for i, v := range src {
		if v <= val {
			bits[i>>3] |= 0x1 << uint(i&0x7)
			cnt++
		}
	}
	return cnt
}

func MatchGreater[T Number](src []T, val T, bits []byte) int64 {
	var cnt int64
	for i, v := range src {
		if v > val {
			bits[i>>3] |= 0x1 << uint(i&0x7)
			cnt++
		}
	}
	return cnt
}

func MatchGreaterEqual[T Number](src []T, val T, bits []byte) int64 {
	var cnt int64
	for i, v := range src {
		if v >= val {
			bits[i>>3] |= 0x1 << uint(i&0x7)
			cnt++
		}
	}
	return cnt
}

func MatchBetween[T Number](src []T, a, b T, bits []byte) int64 {
	diff := uint64(b - a + 1)
	var cnt int64
	for i, v := range src {
		if uint64(v-a) < diff {
			bits[i>>3] |= 0x1 << uint(i&0x7)
			cnt++
		}
	}
	return cnt
}
