// Copyright (c) 2023 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package query

import (
	"blockwatch.cc/knoxdb/internal/bitset"
	"blockwatch.cc/knoxdb/internal/block"
	"blockwatch.cc/knoxdb/internal/filter/bloom"
	"blockwatch.cc/knoxdb/internal/xroar"
	"blockwatch.cc/knoxdb/pkg/slicex"
)

type bitsetMatchFunc func(src *bitset.Bitset, val bool, bits, mask *bitset.Bitset) *bitset.Bitset

type BitMatcherFactory struct{}

func (f BitMatcherFactory) New(m FilterMode) Matcher {
	switch m {
	case FilterModeEqual:
		return &bitEqualMatcher{}
	case FilterModeNotEqual:
		return &bitNotEqualMatcher{}
	case FilterModeGt:
		return &bitGtMatcher{}
	case FilterModeGe:
		return &bitGeMatcher{}
	case FilterModeLt:
		return &bitLtMatcher{}
	case FilterModeLe:
		return &bitLeMatcher{}
	case FilterModeRange:
		return &bitRangeMatcher{}
	case FilterModeIn:
		return &bitInSetMatcher{}
	case FilterModeNotIn:
		return &bitNotInSetMatcher{}
	default:
		// any other mode is unsupported
		return &noopMatcher{}
	}
}

type bitMatcher struct {
	noopMatcher
	val  bool
	hash [2]uint32
}

func (m *bitMatcher) WithValue(v any) {
	m.val = v.(bool)
	m.hash = bloom.HashAny(m.val)
}

func (m *bitMatcher) Value() any {
	return m.val
}

func (m bitMatcher) MatchBloom(_ *bloom.Filter) bool {
	return true
}

func (m bitMatcher) MatchBitmap(_ *xroar.Bitmap) bool {
	return true
}

// EQUAL

type bitEqualMatcher struct {
	bitMatcher
}

func (m bitEqualMatcher) MatchValue(v any) bool {
	return m.val == v.(bool)
}

func (m bitEqualMatcher) MatchRange(from, to any) bool {
	return m.val == from.(bool) || m.val == to.(bool)
}

func (m bitEqualMatcher) MatchBlock(b *block.Block, bits, _ *bitset.Bitset) *bitset.Bitset {
	if m.val {
		return bits.Copy(b.Bool())
	} else {
		return bits.Copy(b.Bool()).Neg()
	}
}

func (m bitEqualMatcher) MatchBloom(flt *bloom.Filter) bool {
	return flt.ContainsHash(m.hash)
}

// NOT EQUAL

type bitNotEqualMatcher struct {
	bitMatcher
}

func (m *bitNotEqualMatcher) WithValue(v any) {
	m.val = v.(bool)
}

func (m *bitNotEqualMatcher) Value() any {
	return m.val
}

func (m bitNotEqualMatcher) MatchValue(v any) bool {
	return m.val != v.(bool)
}

func (m bitNotEqualMatcher) MatchRange(from, to any) bool {
	if from.(bool) == to.(bool) {
		return m.val != from.(bool)
	}
	return true
}

func (m bitNotEqualMatcher) MatchBlock(b *block.Block, bits, _ *bitset.Bitset) *bitset.Bitset {
	if !m.val {
		return bits.Copy(b.Bool())
	} else {
		return bits.Copy(b.Bool()).Neg()
	}
}

// GT ---

type bitGtMatcher struct {
	bitMatcher
}

func (m bitGtMatcher) MatchValue(v any) bool {
	return v.(bool) && !m.val
}

func (m bitGtMatcher) MatchRange(_, to any) bool {
	return to.(bool) && !m.val
}

func (m bitGtMatcher) MatchBlock(b *block.Block, bits, _ *bitset.Bitset) *bitset.Bitset {
	if m.val {
		return bits.Zero()
	} else {
		return bits.Copy(b.Bool())
	}
}

// GE ---

type bitGeMatcher struct {
	bitMatcher
}

func (m bitGeMatcher) MatchValue(val any) bool {
	// m.val   val
	// ---------------------
	// false   false -> true
	// false   true  -> true
	// true    false -> false
	// true    true  -> true
	return !(m.val && !val.(bool))
}

func (m bitGeMatcher) MatchRange(from, to any) bool {
	if m.val {
		return to.(bool)
	}
	return true
}

func (m bitGeMatcher) MatchBlock(b *block.Block, bits, _ *bitset.Bitset) *bitset.Bitset {
	return bits.One()
}

// LT ---

type bitLtMatcher struct {
	bitMatcher
}

func (m bitLtMatcher) MatchValue(v any) bool {
	return m.val && !v.(bool)
}

func (m bitLtMatcher) MatchRange(from, _ any) bool {
	return m.val && !from.(bool)
}

func (m bitLtMatcher) MatchBlock(b *block.Block, bits, _ *bitset.Bitset) *bitset.Bitset {
	if m.val {
		return bits.Copy(b.Bool()).Neg()
	} else {
		return bits.Zero()
	}
}

// LE ---

type bitLeMatcher struct {
	bitMatcher
}

func (m bitLeMatcher) MatchValue(val any) bool {
	// m.val   val
	// ---------------------
	// false   false -> true
	// false   true  -> false
	// true    false -> true
	// true    true  -> true
	return m.val || !val.(bool)
}

func (m bitLeMatcher) MatchRange(_, _ any) bool {
	return true
}

func (m bitLeMatcher) MatchBlock(b *block.Block, bits, _ *bitset.Bitset) *bitset.Bitset {
	return bits.One()
}

// RANGE ---

// InBetween, ContainsRange
type bitRangeMatcher struct {
	bitMatcher
	from bool
	to   bool
}

func (m *bitRangeMatcher) Value() any { return RangeValue{m.from, m.to} }

func (m *bitRangeMatcher) Weight() int { return 2 }

func (m *bitRangeMatcher) Len() int { return 2 }

func (m *bitRangeMatcher) WithValue(v any) {
	val := v.(RangeValue)
	m.from = val[0].(bool)
	m.to = val[1].(bool)
}

func (m bitRangeMatcher) MatchValue(v any) bool {
	return !m.from && m.to || m.from == v.(bool)
}

func (m bitRangeMatcher) MatchRange(from, to any) bool {
	return !m.from && m.to || from.(bool) != to.(bool) || m.from == from.(bool)
}

func (m bitRangeMatcher) MatchBitmap(flt *xroar.Bitmap) bool {
	if m.to {
		return flt.Contains(1)
	}
	return flt.Contains(0)
}

func (m bitRangeMatcher) MatchBlock(b *block.Block, bits, _ *bitset.Bitset) *bitset.Bitset {
	if m.from {
		return bits.Copy(b.Bool())
	}
	if m.to {
		return bits.One()
	}
	return bits.Copy(b.Bool()).Neg()
}

// IN ---

// In, Contains
type bitInSetMatcher struct {
	bitRangeMatcher
	hashes [][2]uint32
}

func (m *bitInSetMatcher) Weight() int { return 1 }

func (m *bitInSetMatcher) Len() int {
	if m.to != m.from {
		return 2
	}
	return 1
}

func (m *bitInSetMatcher) Value() any {
	if m.to != m.from {
		return []bool{m.from, m.to}
	} else {
		return []bool{m.from}
	}
}

func (m *bitInSetMatcher) WithValue(val any) {
	m.WithSlice(val)
}

func (m *bitInSetMatcher) WithSlice(slice any) {
	m.from = false
	m.to = false
	vals := slicex.UniqueBools(slice.([]bool))
	switch len(vals) {
	case 1:
		m.from, m.to = vals[0], vals[0]
	case 2:
		m.from, m.to = vals[0], vals[1]
	}
	m.hashes = bloom.HashAnySlice(vals)
}

func (m *bitInSetMatcher) WithSet(set *xroar.Bitmap) {
	var r byte
	if set.Contains(0) {
		r |= 0x1
	}
	if set.Contains(1) {
		r |= 0x2
	}
	switch r {
	case 2:
		// all true
		m.from, m.to = true, true
		m.hashes = [][2]uint32{bloom.HashAny(true)}
	case 3:
		// full range
		m.from, m.to = false, true
		m.hashes = [][2]uint32{bloom.HashAny(false), bloom.HashAny(true)}
	default:
		// empty or all false
		m.from, m.to = false, false
		m.hashes = [][2]uint32{bloom.HashAny(false)}
	}
}

func (m bitInSetMatcher) MatchBloom(flt *bloom.Filter) bool {
	return flt.ContainsAnyHash(m.hashes)
}

// NOT IN ---

type bitNotInSetMatcher struct {
	bitInSetMatcher
}

func (m bitNotInSetMatcher) MatchBloom(flt *bloom.Filter) bool {
	return true
}

func (m bitNotInSetMatcher) MatchBitmap(flt *xroar.Bitmap) bool {
	if m.to {
		return !flt.Contains(1)
	}
	return !flt.Contains(0)
}

func (m bitNotInSetMatcher) MatchValue(v any) bool {
	return !m.bitInSetMatcher.MatchValue(v)
}

func (m bitNotInSetMatcher) MatchRange(from, to any) bool {
	return !m.bitInSetMatcher.MatchRange(from, to)
}

func (m bitNotInSetMatcher) MatchBlock(b *block.Block, bits, mask *bitset.Bitset) *bitset.Bitset {
	return m.bitInSetMatcher.MatchBlock(b, bits, mask).Neg()
}
