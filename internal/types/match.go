// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package types

import (
	"blockwatch.cc/knoxdb/internal/bitset"
)

type Bitset = bitset.Bitset

type Integer interface {
	int64 | int32 | int16 | int8 | uint64 | uint32 | uint16 | uint8
}

type Number interface {
	Integer | float64 | float32
}

type NumberMatcher[T Number] interface {
	MatchEqual(val T, bits, mask *Bitset) *Bitset
	MatchNotEqual(val T, bits, mask *Bitset) *Bitset
	MatchLess(val T, bits, mask *Bitset) *Bitset
	MatchLessEqual(val T, bits, mask *Bitset) *Bitset
	MatchGreater(val T, bits, mask *Bitset) *Bitset
	MatchGreaterEqual(val T, bits, mask *Bitset) *Bitset
	MatchBetween(a, b T, bits, mask *Bitset) *Bitset

	// Int: *xorar.Bitmap
	// Float: []float64, []float32
	MatchSet(s any, bits, mask *Bitset) *Bitset
	MatchNotSet(s any, bits, mask *Bitset) *Bitset
}

type NumberAccessor[T Number] interface {
	Get(int) T
	AppendTo([]uint32, []T) []T
}
