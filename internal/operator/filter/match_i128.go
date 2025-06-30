// Copyright (c) 2023-2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package filter

import (
	"blockwatch.cc/knoxdb/internal/bitset"
	"blockwatch.cc/knoxdb/internal/block"
	"blockwatch.cc/knoxdb/internal/cmp"
	"blockwatch.cc/knoxdb/internal/filter"
	"blockwatch.cc/knoxdb/pkg/num"
)

type i128MatchFunc func(src *num.Int128Stride, val num.Int128, bits, mask []byte) int64

type I128MatcherFactory struct{}

func (f I128MatcherFactory) New(m FilterMode) Matcher {
	switch m {
	case FilterModeEqual:
		return &i128EqualMatcher{i128Matcher{match: cmp.Int128Equal}}
	case FilterModeNotEqual:
		return &i128NotEqualMatcher{i128Matcher{match: cmp.Int128NotEqual}}
	case FilterModeGt:
		return &i128GtMatcher{i128Matcher{match: cmp.Int128Greater}}
	case FilterModeGe:
		return &i128GeMatcher{i128Matcher{match: cmp.Int128GreaterEqual}}
	case FilterModeLt:
		return &i128LtMatcher{i128Matcher{match: cmp.Int128Less}}
	case FilterModeLe:
		return &i128LeMatcher{i128Matcher{match: cmp.Int128LessEqual}}
	case FilterModeRange:
		return &i128RangeMatcher{}
	case FilterModeIn:
		return &i128InSetMatcher{}
	case FilterModeNotIn:
		return &i128NotInSetMatcher{}
	default:
		// unsupported
		// FilterModeRegexp:
		return &noopMatcher{}
	}
}

type i128Matcher struct {
	noopMatcher
	match i128MatchFunc
	val   num.Int128
	hash  uint64
}

func (m *i128Matcher) Weight() int { return 2 }

func (m *i128Matcher) WithValue(v any) {
	m.val = v.(num.Int128)
	m.hash = filter.Hash(m.val.Bytes()).Uint64()
}

func (m *i128Matcher) Value() any {
	return m.val
}

// EQUAL ---

type i128EqualMatcher struct {
	i128Matcher
}

func (m i128EqualMatcher) MatchValue(v any) bool {
	return m.val.Eq(v.(num.Int128))
}

func (m i128EqualMatcher) MatchRange(from, to any) bool {
	return !(m.val.Lt(from.(num.Int128)) || m.val.Gt(to.(num.Int128)))
}

func (m i128EqualMatcher) MatchRangeVectors(mins, maxs *block.Block, bits, mask *bitset.Bitset) {
	// min <= v && max >= v, mask is optional
	f := newFactory(mins.Type())
	le, ge := f.New(FilterModeLe), f.New(FilterModeGe)
	le.WithValue(m.val)
	ge.WithValue(m.val)
	minBits := bitset.New(mins.Len())
	le.MatchVector(mins, minBits, mask)
	if mask != nil {
		minBits.And(mask)
	}
	ge.MatchVector(maxs, bits, minBits)
	bits.And(minBits)
	minBits.Close()
}

func (m i128EqualMatcher) MatchVector(b *block.Block, bits, mask *bitset.Bitset) {
	if b.IsMaterialized() {
		n := m.match(b.Int128().Slice(), m.val, bits.Bytes(), mask.Bytes())
		bits.ResetCount(int(n))
	} else {
		b.Int128().Matcher().MatchEqual(m.val, bits, mask)
	}
}

// NOT EQUAL ---

type i128NotEqualMatcher struct {
	i128Matcher
}

func (m i128NotEqualMatcher) MatchValue(v any) bool {
	return !m.val.Eq(v.(num.Int128))
}

func (m i128NotEqualMatcher) MatchRange(from, to any) bool {
	return m.val.Lt(from.(num.Int128)) || m.val.Gt(to.(num.Int128))
}

func (m i128NotEqualMatcher) MatchRangeVectors(_, _ *block.Block, bits, mask *bitset.Bitset) {
	// undecided, always true
	if mask != nil {
		bits.Copy(mask)
	} else {
		bits.One()
	}
}

func (m i128NotEqualMatcher) MatchFilter(flt filter.Filter) bool {
	return flt.Contains(m.hash)
}

func (m i128NotEqualMatcher) MatchVector(b *block.Block, bits, mask *bitset.Bitset) {
	if b.IsMaterialized() {
		n := m.match(b.Int128().Slice(), m.val, bits.Bytes(), mask.Bytes())
		bits.ResetCount(int(n))
	} else {
		b.Int128().Matcher().MatchNotEqual(m.val, bits, mask)
	}
}

// GT ---

type i128GtMatcher struct {
	i128Matcher
}

func (m i128GtMatcher) MatchValue(v any) bool {
	return m.val.Lt(v.(num.Int128))
}

func (m i128GtMatcher) MatchRange(_, to any) bool {
	return m.val.Lt(to.(num.Int128))
}

func (m i128GtMatcher) MatchRangeVectors(_, maxs *block.Block, bits, mask *bitset.Bitset) {
	// max > v
	gt := newFactory(maxs.Type()).New(FilterModeGt)
	gt.WithValue(m.val)
	gt.MatchVector(maxs, bits, mask)
}

func (m i128GtMatcher) MatchVector(b *block.Block, bits, mask *bitset.Bitset) {
	if b.IsMaterialized() {
		n := m.match(b.Int128().Slice(), m.val, bits.Bytes(), mask.Bytes())
		bits.ResetCount(int(n))
	} else {
		b.Int128().Matcher().MatchGreater(m.val, bits, mask)
	}
}

// GE ---

type i128GeMatcher struct {
	i128Matcher
}

func (m i128GeMatcher) MatchValue(v any) bool {
	return m.val.Le(v.(num.Int128))
}

func (m i128GeMatcher) MatchRange(from, to any) bool {
	return m.val.Le(to.(num.Int128))
}

func (m i128GeMatcher) MatchRangeVectors(_, maxs *block.Block, bits, mask *bitset.Bitset) {
	// max >= v
	ge := newFactory(maxs.Type()).New(FilterModeGe)
	ge.WithValue(m.val)
	ge.MatchVector(maxs, bits, mask)
}

func (m i128GeMatcher) MatchVector(b *block.Block, bits, mask *bitset.Bitset) {
	if b.IsMaterialized() {
		n := m.match(b.Int128().Slice(), m.val, bits.Bytes(), mask.Bytes())
		bits.ResetCount(int(n))
	} else {
		b.Int128().Matcher().MatchGreaterEqual(m.val, bits, mask)
	}
}

// LT ---

type i128LtMatcher struct {
	i128Matcher
}

func (m i128LtMatcher) MatchValue(v any) bool {
	return m.val.Gt(v.(num.Int128))
}

func (m i128LtMatcher) MatchRange(from, to any) bool {
	return m.val.Gt(from.(num.Int128))
}

func (m i128LtMatcher) MatchRangeVectors(mins, _ *block.Block, bits, mask *bitset.Bitset) {
	// min < v
	lt := newFactory(mins.Type()).New(FilterModeLt)
	lt.WithValue(m.val)
	lt.MatchVector(mins, bits, mask)
}

func (m i128LtMatcher) MatchVector(b *block.Block, bits, mask *bitset.Bitset) {
	if b.IsMaterialized() {
		n := m.match(b.Int128().Slice(), m.val, bits.Bytes(), mask.Bytes())
		bits.ResetCount(int(n))
	} else {
		b.Int128().Matcher().MatchLess(m.val, bits, mask)
	}
}

// LE ---

type i128LeMatcher struct {
	i128Matcher
}

func (m i128LeMatcher) MatchValue(v any) bool {
	return m.val.Ge(v.(num.Int128))
}

func (m i128LeMatcher) MatchRange(from, to any) bool {
	return m.val.Ge(from.(num.Int128))
}

func (m i128LeMatcher) MatchRangeVectors(mins, _ *block.Block, bits, mask *bitset.Bitset) {
	// min <= v
	le := newFactory(mins.Type()).New(FilterModeLe)
	le.WithValue(m.val)
	le.MatchVector(mins, bits, mask)
}

func (m i128LeMatcher) MatchVector(b *block.Block, bits, mask *bitset.Bitset) {
	if b.IsMaterialized() {
		n := m.match(b.Int128().Slice(), m.val, bits.Bytes(), mask.Bytes())
		bits.ResetCount(int(n))
	} else {
		b.Int128().Matcher().MatchLessEqual(m.val, bits, mask)
	}
}

// RANGE ---

type i128RangeMatcher struct {
	noopMatcher
	from num.Int128
	to   num.Int128
}

func (m *i128RangeMatcher) Weight() int { return 4 }

func (m *i128RangeMatcher) Len() int { return 2 }

func (m *i128RangeMatcher) WithValue(v any) {
	val := v.(RangeValue)
	m.from = val[0].(num.Int128)
	m.to = val[1].(num.Int128)
}

func (m *i128RangeMatcher) Value() any {
	val := RangeValue{m.from, m.to}
	return val
}

func (m i128RangeMatcher) MatchValue(v any) bool {
	return m.from.Le(v.(num.Int128)) && m.to.Ge(v.(num.Int128))
}

func (m i128RangeMatcher) MatchRange(from, to any) bool {
	return !(from.(num.Int128).Gt(m.to) || to.(num.Int128).Lt(m.from))
}

func (m i128RangeMatcher) MatchVector(b *block.Block, bits, mask *bitset.Bitset) {
	if b.IsMaterialized() {
		n := cmp.Int128Between(b.Int128().Slice(), m.from, m.to, bits.Bytes(), mask.Bytes())
		bits.ResetCount(int(n))
	} else {
		b.Int128().Matcher().MatchBetween(m.from, m.to, bits, mask)
	}
}

func (m i128RangeMatcher) MatchRangeVectors(mins, maxs *block.Block, bits, mask *bitset.Bitset) {
	// min <= to && max >= from
	f := newFactory(mins.Type())
	le, ge := f.New(FilterModeLe), f.New(FilterModeGe)
	le.WithValue(m.to)
	ge.WithValue(m.from)
	minBits := bitset.New(mins.Len())
	le.MatchVector(mins, minBits, mask)
	if mask != nil {
		minBits.And(mask)
	}
	ge.MatchVector(maxs, bits, minBits)
	bits.And(minBits)
	minBits.Close()
}

// IN ---

// In, Contains
type i128InSetMatcher struct {
	noopMatcher
	slice  []num.Int128
	hashes []filter.HashValue
}

func (m *i128InSetMatcher) Weight() int { return len(m.slice) }

func (m *i128InSetMatcher) Len() int { return len(m.slice) }

func (m *i128InSetMatcher) Value() any {
	return m.slice
}

func (m *i128InSetMatcher) WithValue(val any) {
	m.WithSlice(val)
}

func (m *i128InSetMatcher) WithSlice(slice any) {
	m.slice = num.Int128Sort(slice.([]num.Int128))
	m.hashes = filter.HashMulti(m.slice)
}

func (m i128InSetMatcher) MatchValue(v any) bool {
	return num.Int128Contains(m.slice, v.(num.Int128))
}

func (m i128InSetMatcher) MatchRange(from, to any) bool {
	return num.Int128ContainsRange(m.slice, from.(num.Int128), to.(num.Int128))
}

func (m i128InSetMatcher) MatchFilter(flt filter.Filter) bool {
	return flt.ContainsAny(m.hashes)
}

func (m i128InSetMatcher) MatchVector(b *block.Block, bits, mask *bitset.Bitset) {
	stride := b.Int128()
	if mask != nil {
		// skip masked values
		for i := range mask.Iterator() {
			if num.Int128Contains(m.slice, stride.Get(i)) {
				bits.Set(i)
			}
		}
	} else {
		for i, v := range stride.Iterator() {
			if num.Int128Contains(m.slice, v) {
				bits.Set(i)
			}
		}
	}
}

func (m i128InSetMatcher) MatchRangeVectors(mins, maxs *block.Block, bits, mask *bitset.Bitset) {
	setMin, setMax := m.slice[0], m.slice[len(m.slice)-1]
	rg := newFactory(mins.Type()).New(FilterModeRange)
	rg.WithValue(RangeValue{setMin, setMax})
	rg.MatchRangeVectors(mins, maxs, bits, mask)
}

// NOT IN ---

type i128NotInSetMatcher struct {
	noopMatcher
	slice []num.Int128
}

func (m *i128NotInSetMatcher) Weight() int { return len(m.slice) }

func (m *i128NotInSetMatcher) Len() int { return len(m.slice) }

func (m *i128NotInSetMatcher) Value() any {
	return m.slice
}

func (m *i128NotInSetMatcher) WithValue(val any) {
	m.WithSlice(val)
}

func (m *i128NotInSetMatcher) WithSlice(slice any) {
	m.slice = num.Int128Sort(slice.([]num.Int128))
}

func (m i128NotInSetMatcher) MatchValue(v any) bool {
	return !num.Int128Contains(m.slice, v.(num.Int128))
}

func (m i128NotInSetMatcher) MatchRange(from, to any) bool {
	return !num.Int128ContainsRange(m.slice, from.(num.Int128), to.(num.Int128))
}

func (m i128NotInSetMatcher) MatchFilter(_ filter.Filter) bool {
	// we don't know generally, so full scan is always required
	return true
}

func (m i128NotInSetMatcher) MatchVector(b *block.Block, bits, mask *bitset.Bitset) {
	stride := b.Int128()
	if mask != nil {
		// skip masked values
		for i := range mask.Iterator() {
			if !num.Int128Contains(m.slice, stride.Get(i)) {
				bits.Set(i)
			}
		}
	} else {
		for i, v := range stride.Iterator() {
			if !num.Int128Contains(m.slice, v) {
				bits.Set(i)
			}
		}
	}
}

func (m i128NotInSetMatcher) MatchRangeVectors(_, _ *block.Block, bits, mask *bitset.Bitset) {
	// undecided, always true
	if mask != nil {
		bits.Copy(mask)
	} else {
		bits.One()
	}
}
