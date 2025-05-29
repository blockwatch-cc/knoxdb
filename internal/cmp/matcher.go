// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package cmp

import (
	"unsafe"

	"blockwatch.cc/knoxdb/internal/bitset"
	"blockwatch.cc/knoxdb/internal/types"
)

type (
	// func types for casting from ptr lookup table [mode][type]unsafe.Pointer
	numMatchFunc[T types.Number]      func(slice []T, val T, bits []byte) int64
	numRangeMatchFunc[T types.Number] func(slice []T, from, to T, bits []byte) int64

	Bitset = bitset.Bitset
)

const (
	// func ptr lookup table indexes for compare modes
	eq_t byte = iota
	ne_t
	gt_t
	ge_t
	lt_t
	le_t
	rg_t
)

const (
	// func ptr lookup table indices for data types
	i64_t byte = iota
	i32_t
	i16_t
	i8_t
	u64_t
	u32_t
	u16_t
	u8_t
	f64_t
	f32_t
	i128_t
	i256_t
)

type Matcher[T types.Number] struct {
	idx  byte
	vals []T
}

func NewMatcher[T types.Number](vals []T) Matcher[T] {
	switch any(T(0)).(type) {
	case int64:
		return Matcher[T]{i64_t, vals}
	case int32:
		return Matcher[T]{i32_t, vals}
	case int16:
		return Matcher[T]{i16_t, vals}
	case int8:
		return Matcher[T]{i8_t, vals}
	case uint64:
		return Matcher[T]{u64_t, vals}
	case uint32:
		return Matcher[T]{u32_t, vals}
	case uint16:
		return Matcher[T]{u16_t, vals}
	case uint8:
		return Matcher[T]{u8_t, vals}
	case float64:
		return Matcher[T]{f64_t, vals}
	case float32:
		return Matcher[T]{f32_t, vals}
	default:
		return Matcher[T]{} // unused
	}
}

func (m Matcher[T]) MatchEqual(val T, bits, mask *Bitset) {
	n := (*(*numMatchFunc[T])(matchFn[eq_t][m.idx]))(m.vals, val, bits.Bytes())
	bits.ResetCount(int(n))
}

func (m Matcher[T]) MatchNotEqual(val T, bits, mask *Bitset) {
	n := (*(*numMatchFunc[T])(matchFn[ne_t][m.idx]))(m.vals, val, bits.Bytes())
	bits.ResetCount(int(n))
}

func (m Matcher[T]) MatchLess(val T, bits, mask *Bitset) {
	n := (*(*numMatchFunc[T])(matchFn[lt_t][m.idx]))(m.vals, val, bits.Bytes())
	bits.ResetCount(int(n))
}

func (m Matcher[T]) MatchLessEqual(val T, bits, mask *Bitset) {
	n := (*(*numMatchFunc[T])(matchFn[le_t][m.idx]))(m.vals, val, bits.Bytes())
	bits.ResetCount(int(n))
}

func (m Matcher[T]) MatchGreater(val T, bits, mask *Bitset) {
	n := (*(*numMatchFunc[T])(matchFn[gt_t][m.idx]))(m.vals, val, bits.Bytes())
	bits.ResetCount(int(n))
}

func (m Matcher[T]) MatchGreaterEqual(val T, bits, mask *Bitset) {
	n := (*(*numMatchFunc[T])(matchFn[ge_t][m.idx]))(m.vals, val, bits.Bytes())
	bits.ResetCount(int(n))
}

func (m Matcher[T]) MatchBetween(a, b T, bits, mask *Bitset) {
	n := (*(*numRangeMatchFunc[T])(matchFn[rg_t][m.idx]))(m.vals, a, b, bits.Bytes())
	bits.ResetCount(int(n))
}

func (m Matcher[T]) MatchInSet(s any, bits, mask *Bitset) {
	// noop, implemeted in query.numXXXMatcher
}

func (m Matcher[T]) MatchNotInSet(s any, bits, mask *Bitset) {
	// noop, implemeted in query.numXXXMatcher
}

var matchFn = [7][12]unsafe.Pointer{
	// eq 0
	{
		unsafe.Pointer(&Int64Equal),   // 0
		unsafe.Pointer(&Int32Equal),   // 1
		unsafe.Pointer(&Int16Equal),   // 2
		unsafe.Pointer(&Int8Equal),    // 3
		unsafe.Pointer(&Uint64Equal),  // 4
		unsafe.Pointer(&Uint32Equal),  // 5
		unsafe.Pointer(&Uint16Equal),  // 6
		unsafe.Pointer(&Uint8Equal),   // 7
		unsafe.Pointer(&Float64Equal), // 8
		unsafe.Pointer(&Float32Equal), // 9
		unsafe.Pointer(&Int128Equal),  // 10
		unsafe.Pointer(&Int256Equal),  // 11
	},
	// ne 1
	{
		unsafe.Pointer(&Int64NotEqual),   // 0
		unsafe.Pointer(&Int32NotEqual),   // 1
		unsafe.Pointer(&Int16NotEqual),   // 2
		unsafe.Pointer(&Int8NotEqual),    // 3
		unsafe.Pointer(&Uint64NotEqual),  // 4
		unsafe.Pointer(&Uint32NotEqual),  // 5
		unsafe.Pointer(&Uint16NotEqual),  // 6
		unsafe.Pointer(&Uint8NotEqual),   // 7
		unsafe.Pointer(&Float64NotEqual), // 8
		unsafe.Pointer(&Float32NotEqual), // 9
		unsafe.Pointer(&Int128NotEqual),  // 10
		unsafe.Pointer(&Int256NotEqual),  // 11
	},
	// gt 2
	{
		unsafe.Pointer(&Int64Greater),   // 0
		unsafe.Pointer(&Int32Greater),   // 1
		unsafe.Pointer(&Int16Greater),   // 2
		unsafe.Pointer(&Int8Greater),    // 3
		unsafe.Pointer(&Uint64Greater),  // 4
		unsafe.Pointer(&Uint32Greater),  // 5
		unsafe.Pointer(&Uint16Greater),  // 6
		unsafe.Pointer(&Uint8Greater),   // 7
		unsafe.Pointer(&Float64Greater), // 8
		unsafe.Pointer(&Float32Greater), // 9
		unsafe.Pointer(&Int128Greater),  // 10
		unsafe.Pointer(&Int256Greater),  // 11
	},
	// ge 3
	{
		unsafe.Pointer(&Int64GreaterEqual),   // 0
		unsafe.Pointer(&Int32GreaterEqual),   // 1
		unsafe.Pointer(&Int16GreaterEqual),   // 2
		unsafe.Pointer(&Int8GreaterEqual),    // 3
		unsafe.Pointer(&Uint64GreaterEqual),  // 4
		unsafe.Pointer(&Uint32GreaterEqual),  // 5
		unsafe.Pointer(&Uint16GreaterEqual),  // 6
		unsafe.Pointer(&Uint8GreaterEqual),   // 7
		unsafe.Pointer(&Float64GreaterEqual), // 8
		unsafe.Pointer(&Float32GreaterEqual), // 9
		unsafe.Pointer(&Int128GreaterEqual),  // 10
		unsafe.Pointer(&Int256GreaterEqual),  // 11
	},
	// lt 4
	{
		unsafe.Pointer(&Int64Less),   // 0
		unsafe.Pointer(&Int32Less),   // 1
		unsafe.Pointer(&Int16Less),   // 2
		unsafe.Pointer(&Int8Less),    // 3
		unsafe.Pointer(&Uint64Less),  // 4
		unsafe.Pointer(&Uint32Less),  // 5
		unsafe.Pointer(&Uint16Less),  // 6
		unsafe.Pointer(&Uint8Less),   // 7
		unsafe.Pointer(&Float64Less), // 8
		unsafe.Pointer(&Float32Less), // 9
		unsafe.Pointer(&Int128Less),  // 10
		unsafe.Pointer(&Int256Less),  // 11
	},
	// le 5
	{
		unsafe.Pointer(&Int64LessEqual),   // 0
		unsafe.Pointer(&Int32LessEqual),   // 1
		unsafe.Pointer(&Int16LessEqual),   // 2
		unsafe.Pointer(&Int8LessEqual),    // 3
		unsafe.Pointer(&Uint64LessEqual),  // 4
		unsafe.Pointer(&Uint32LessEqual),  // 5
		unsafe.Pointer(&Uint16LessEqual),  // 6
		unsafe.Pointer(&Uint8LessEqual),   // 7
		unsafe.Pointer(&Float64LessEqual), // 8
		unsafe.Pointer(&Float32LessEqual), // 9
		unsafe.Pointer(&Int128LessEqual),  // 10
		unsafe.Pointer(&Int256LessEqual),  // 11
	},
	// rg 6
	{
		unsafe.Pointer(&Int64Between),   // 0
		unsafe.Pointer(&Int32Between),   // 1
		unsafe.Pointer(&Int16Between),   // 2
		unsafe.Pointer(&Int8Between),    // 3
		unsafe.Pointer(&Uint64Between),  // 4
		unsafe.Pointer(&Uint32Between),  // 5
		unsafe.Pointer(&Uint16Between),  // 6
		unsafe.Pointer(&Uint8Between),   // 7
		unsafe.Pointer(&Float64Between), // 8
		unsafe.Pointer(&Float32Between), // 9
		unsafe.Pointer(&Int128Between),  // 10
		unsafe.Pointer(&Int256Between),  // 11
	},
}
