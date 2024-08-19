// Copyright (c) 2023 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package generic

import (
	"golang.org/x/exp/constraints"
)

type Float interface {
	constraints.Float
}

func MatchEqualFloat[T Float](src []T, val T, bits []byte) int64 {
	var cnt int64
	for i, v := range src {
		if v == val {
			bits[i>>3] |= 0x1 << uint(i&0x7)
			cnt++
		}
	}
	return cnt
}

func MatchNotEqualFloat[T Float](src []T, val T, bits []byte) int64 {
	var cnt int64
	for i, v := range src {
		if v != val {
			bits[i>>3] |= 0x1 << uint(i&0x7)
			cnt++
		}
	}
	return cnt
}

func MatchLessFloat[T Float](src []T, val T, bits []byte) int64 {
	var cnt int64
	for i, v := range src {
		if v < val {
			bits[i>>3] |= 0x1 << uint(i&0x7)
			cnt++
		}
	}
	return cnt
}

func MatchLessEqualFloat[T Float](src []T, val T, bits []byte) int64 {
	var cnt int64
	for i, v := range src {
		if v <= val {
			bits[i>>3] |= 0x1 << uint(i&0x7)
			cnt++
		}
	}
	return cnt
}

func MatchGreaterFloat[T Float](src []T, val T, bits []byte) int64 {
	var cnt int64
	for i, v := range src {
		if v > val {
			bits[i>>3] |= 0x1 << uint(i&0x7)
			cnt++
		}
	}
	return cnt
}

func MatchGreaterEqualFloat[T Float](src []T, val T, bits []byte) int64 {
	var cnt int64
	for i, v := range src {
		if v >= val {
			bits[i>>3] |= 0x1 << uint(i&0x7)
			cnt++
		}
	}
	return cnt
}

func MatchBetweenFloat[T Float](src []T, a, b T, bits []byte) int64 {
	var cnt int64
	for i, v := range src {
		if a <= v && v <= b {
			bits[i>>3] |= 0x1 << uint(i&0x7)
			cnt++
		}
	}
	return cnt
}
