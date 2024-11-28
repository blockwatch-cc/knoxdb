// Copyright (c) 2023 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package generic

import (
	"golang.org/x/exp/constraints"
)

type Float interface {
	constraints.Float
}

func MatchFloatEqual[T Float](src []T, val T, bits []byte) int64 {
	var cnt int64
	for i, v := range src {
		if v == val {
			bits[i>>3] |= 0x1 << uint(i&0x7)
			cnt++
		}
	}
	return cnt
}

func MatchFloatNotEqual[T Float](src []T, val T, bits []byte) int64 {
	var cnt int64
	for i, v := range src {
		if v != val {
			bits[i>>3] |= 0x1 << uint(i&0x7)
			cnt++
		}
	}
	return cnt
}

func MatchFloatLess[T Float](src []T, val T, bits []byte) int64 {
	var cnt int64
	for i, v := range src {
		if v < val {
			bits[i>>3] |= 0x1 << uint(i&0x7)
			cnt++
		}
	}
	return cnt
}

func MatchFloatLessEqual[T Float](src []T, val T, bits []byte) int64 {
	var cnt int64
	for i, v := range src {
		if v <= val {
			bits[i>>3] |= 0x1 << uint(i&0x7)
			cnt++
		}
	}
	return cnt
}

func MatchFloatGreater[T Float](src []T, val T, bits []byte) int64 {
	var cnt int64
	for i, v := range src {
		if v > val {
			bits[i>>3] |= 0x1 << uint(i&0x7)
			cnt++
		}
	}
	return cnt
}

func MatchFloatGreaterEqual[T Float](src []T, val T, bits []byte) int64 {
	var cnt int64
	for i, v := range src {
		if v >= val {
			bits[i>>3] |= 0x1 << uint(i&0x7)
			cnt++
		}
	}
	return cnt
}

func MatchFloatBetween[T Float](src []T, a, b T, bits []byte) int64 {
	var cnt int64
	for i, v := range src {
		if a <= v && v <= b {
			bits[i>>3] |= 0x1 << uint(i&0x7)
			cnt++
		}
	}
	return cnt
}
