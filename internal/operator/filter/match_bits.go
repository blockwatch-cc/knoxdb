// Copyright (c) 2023-2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package filter

import (
	"blockwatch.cc/knoxdb/internal/bitset"
	"blockwatch.cc/knoxdb/internal/block"
	"blockwatch.cc/knoxdb/internal/filter"
	"blockwatch.cc/knoxdb/internal/xroar"
	"blockwatch.cc/knoxdb/pkg/slicex"
	"blockwatch.cc/knoxdb/pkg/util"
)

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
	hash uint64
}

func (m *bitMatcher) WithValue(v any) {
	m.val = v.(bool)
	m.hash = filter.HashUint8(util.Bool2byte(m.val))
}

func (m *bitMatcher) Value() any {
	return m.val
}

func (m bitMatcher) MatchFilter(_ filter.Filter) bool {
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

func (m bitEqualMatcher) MatchVector(b *block.Block, bits, _ *bitset.Bitset) {
	if b.IsMaterialized() {
		bits.Copy(b.Bool().Writer())
	} else {
		b.Bool().AppendTo(bits.Resize(0), nil)
	}
	if !m.val {
		bits.Neg()
	}
}

func (m bitEqualMatcher) MatchRangeVectors(mins, maxs *block.Block, bits, _ *bitset.Bitset) {
	if m.val {
		if maxs.IsMaterialized() {
			bits.Copy(maxs.Bool().Writer())
		} else {
			maxs.Bool().AppendTo(bits.Resize(0), nil)
		}
	} else {
		if mins.IsMaterialized() {
			bits.Copy(mins.Bool().Writer()).Neg()
		} else {
			mins.Bool().AppendTo(bits.Resize(0), nil)
			bits.Neg()
		}
	}
}

func (m bitEqualMatcher) MatchFilter(flt filter.Filter) bool {
	return flt.Contains(m.hash)
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

func (m bitNotEqualMatcher) MatchVector(b *block.Block, bits, _ *bitset.Bitset) {
	if b.IsMaterialized() {
		bits.Copy(b.Bool().Writer())
	} else {
		b.Bool().AppendTo(bits.Resize(0), nil)
	}
	if m.val {
		bits.Neg()
	}
}

func (m bitNotEqualMatcher) MatchRangeVectors(mins, maxs *block.Block, bits, _ *bitset.Bitset) {
	if !m.val {
		if maxs.IsMaterialized() {
			bits.Copy(maxs.Bool().Writer())
		} else {
			maxs.Bool().AppendTo(bits.Resize(0), nil)
		}
	} else {
		if mins.IsMaterialized() {
			bits.Copy(mins.Bool().Writer()).Neg()
		} else {
			mins.Bool().AppendTo(bits.Resize(0), nil)
			bits.Neg()
		}
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

func (m bitGtMatcher) MatchVector(b *block.Block, bits, _ *bitset.Bitset) {
	if m.val {
		bits.Zero()
	} else {
		if b.IsMaterialized() {
			bits.Copy(b.Bool().Writer())
		} else {
			b.Bool().AppendTo(bits.Resize(0), nil)
		}
	}
}

func (m bitGtMatcher) MatchRangeVectors(_, maxs *block.Block, bits, _ *bitset.Bitset) {
	if m.val {
		bits.Zero()
	} else {
		if maxs.IsMaterialized() {
			bits.Copy(maxs.Bool().Writer())
		} else {
			maxs.Bool().AppendTo(bits.Resize(0), nil)
		}
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
	return !m.val || val.(bool)
}

func (m bitGeMatcher) MatchRange(from, to any) bool {
	if m.val {
		return to.(bool)
	}
	return true
}

func (m bitGeMatcher) MatchVector(b *block.Block, bits, mask *bitset.Bitset) {
	if m.val {
		if b.IsMaterialized() {
			bits.Copy(b.Bool().Writer())
		} else {
			b.Bool().AppendTo(bits.Resize(0), nil)
		}
	} else {
		// always true
		if mask != nil {
			bits.Copy(mask)
		} else {
			bits.One()
		}
	}
}

func (m bitGeMatcher) MatchRangeVectors(mins, maxs *block.Block, bits, mask *bitset.Bitset) {
	if m.val {
		if maxs.IsMaterialized() {
			bits.Copy(maxs.Bool().Writer())
		} else {
			maxs.Bool().AppendTo(bits.Resize(0), nil)
		}
	} else {
		// always true
		if mask != nil {
			bits.Copy(mask)
		} else {
			bits.One()
		}
	}
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

func (m bitLtMatcher) MatchVector(b *block.Block, bits, _ *bitset.Bitset) {
	if m.val {
		if b.IsMaterialized() {
			bits.Copy(b.Bool().Writer()).Neg()
		} else {
			b.Bool().AppendTo(bits.Resize(0), nil)
			bits.Neg()
		}
	} else {
		bits.Zero()
	}
}

func (m bitLtMatcher) MatchRangeVectors(mins, _ *block.Block, bits, _ *bitset.Bitset) {
	if m.val {
		if mins.IsMaterialized() {
			bits.Copy(mins.Bool().Writer()).Neg()
		} else {
			mins.Bool().AppendTo(bits.Resize(0), nil)
			bits.Neg()
		}
	} else {
		bits.Zero()
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

func (m bitLeMatcher) MatchVector(b *block.Block, bits, _ *bitset.Bitset) {
	bits.One()
}

func (m bitLeMatcher) MatchRangeVectors(mins, _ *block.Block, bits, mask *bitset.Bitset) {
	if m.val {
		// always true
		if mask != nil {
			bits.Copy(mask)
		} else {
			bits.One()
		}
	} else {
		if mins.IsMaterialized() {
			bits.Copy(mins.Bool().Writer()).Neg()
		} else {
			mins.Bool().AppendTo(bits.Resize(0), nil)
			bits.Neg()
		}
	}
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

func (m bitRangeMatcher) MatchVector(b *block.Block, bits, _ *bitset.Bitset) {
	switch {
	case m.from:
		if b.IsMaterialized() {
			bits.Copy(b.Bool().Writer())
		} else {
			b.Bool().AppendTo(bits.Resize(0), nil)
		}
	case m.to:
		bits.One()
	default:
		if b.IsMaterialized() {
			bits.Copy(b.Bool().Writer()).Neg()
		} else {
			b.Bool().AppendTo(bits.Resize(0), nil)
			bits.Neg()
		}
	}
}

func (m bitRangeMatcher) MatchRangeVectors(mins, maxs *block.Block, bits, mask *bitset.Bitset) {
	switch {
	case !m.from && !m.to: // 00, data vec must contain false -> min = false
		if mins.IsMaterialized() {
			bits.Copy(mins.Bool().Writer()).Neg()
		} else {
			mins.Bool().AppendTo(bits.Resize(0), nil)
			bits.Neg()
		}
	case !m.from && m.to: // 01, always true
		bits.One()
	// case m.from && !m.to: // 10, illegal range
	default: // 11, data vec must contain true -> max = true
		if maxs.IsMaterialized() {
			bits.Copy(maxs.Bool().Writer())
		} else {
			maxs.Bool().AppendTo(bits.Resize(0), nil)
		}
	}
}

// IN ---

// In, Contains
type bitInSetMatcher struct {
	bitRangeMatcher
	hashes []uint64
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
	m.hashes = filter.HashMulti(vals)
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
		m.hashes = []uint64{filter.HashUint8(1)}
	case 3:
		// full range
		m.from, m.to = false, true
		m.hashes = []uint64{filter.HashUint8(0), filter.HashUint8(1)}
	default:
		// empty or all false
		m.from, m.to = false, false
		m.hashes = []uint64{filter.HashUint8(0)}
	}
}

func (m bitInSetMatcher) MatchFilter(flt filter.Filter) bool {
	return flt.ContainsAny(m.hashes)
}

// NOT IN ---

type bitNotInSetMatcher struct {
	bitInSetMatcher
}

func (m bitNotInSetMatcher) MatchFilter(flt filter.Filter) bool {
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

func (m bitNotInSetMatcher) MatchVector(b *block.Block, bits, mask *bitset.Bitset) {
	m.bitInSetMatcher.MatchVector(b, bits, mask)
	bits.Neg()
}

func (m bitNotInSetMatcher) MatchRangeVectors(mins, maxs *block.Block, bits, _ *bitset.Bitset) {
	m.bitInSetMatcher.MatchRangeVectors(mins, maxs, bits, nil)
	bits.Neg()
}
