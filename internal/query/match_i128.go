// Copyright (c) 2023 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package query

import (
	"blockwatch.cc/knoxdb/internal/bitset"
	"blockwatch.cc/knoxdb/internal/block"
	"blockwatch.cc/knoxdb/internal/cmp"
	"blockwatch.cc/knoxdb/internal/filter"
	"blockwatch.cc/knoxdb/pkg/num"
)

type i128MatchFunc func(src num.Int128Stride, val num.Int128, bits, mask *bitset.Bitset) *bitset.Bitset

type I128MatcherFactory struct{}

func (f I128MatcherFactory) New(m FilterMode) Matcher {
	switch m {
	case FilterModeEqual:
		return &i128EqualMatcher{i128Matcher{match: cmp.MatchInt128Equal}}
	case FilterModeNotEqual:
		return &i128NotEqualMatcher{i128Matcher{match: cmp.MatchInt128NotEqual}}
	case FilterModeGt:
		return &i128GtMatcher{i128Matcher{match: cmp.MatchInt128Greater}}
	case FilterModeGe:
		return &i128GeMatcher{i128Matcher{match: cmp.MatchInt128GreaterEqual}}
	case FilterModeLt:
		return &i128LtMatcher{i128Matcher{match: cmp.MatchInt128Less}}
	case FilterModeLe:
		return &i128LeMatcher{i128Matcher{match: cmp.MatchInt128LessEqual}}
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
}

func (m *i128Matcher) Weight() int { return 2 }

func (m *i128Matcher) WithValue(v any) {
	m.val = v.(num.Int128)
}

func (m *i128Matcher) Value() any {
	return m.val
}

func (m i128Matcher) MatchVector(b *block.Block, bits, mask *bitset.Bitset) *bitset.Bitset {
	return m.match(*b.Int128(), m.val, bits, mask)
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

func (m i128EqualMatcher) MatchRangeVectors(mins, maxs *block.Block, bits, mask *bitset.Bitset) *bitset.Bitset {
	// min <= v && max >= v, mask is optional
	f := newFactory(mins.Type())
	le, ge := f.New(FilterModeLe), f.New(FilterModeGe)
	le.WithValue(m.val)
	ge.WithValue(m.val)
	minBits := le.MatchVector(mins, nil, mask)
	if mask != nil {
		minBits.And(mask)
	}
	bits = ge.MatchVector(maxs, bits, minBits)
	bits.And(minBits)
	minBits.Close()
	return bits
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

func (m i128NotEqualMatcher) MatchRangeVectors(_, _ *block.Block, bits, mask *bitset.Bitset) *bitset.Bitset {
	// undecided, always true
	if mask != nil {
		bits.Copy(mask)
	} else {
		bits.One()
	}
	return bits
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

func (m i128GtMatcher) MatchRangeVectors(_, maxs *block.Block, bits, mask *bitset.Bitset) *bitset.Bitset {
	// max > v
	gt := newFactory(maxs.Type()).New(FilterModeGt)
	gt.WithValue(m.val)
	return gt.MatchVector(maxs, bits, mask)
}

// GE ---

type i128GeMatcher struct {
	i128Matcher
}

func (m i128GeMatcher) MatchValue(v any) bool {
	return m.val.Lte(v.(num.Int128))
}

func (m i128GeMatcher) MatchRange(from, to any) bool {
	return m.val.Lte(to.(num.Int128))
}

func (m i128GeMatcher) MatchRangeVectors(_, maxs *block.Block, bits, mask *bitset.Bitset) *bitset.Bitset {
	// max >= v
	ge := newFactory(maxs.Type()).New(FilterModeGe)
	ge.WithValue(m.val)
	return ge.MatchVector(maxs, bits, mask)
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

func (m i128LtMatcher) MatchRangeVectors(mins, _ *block.Block, bits, mask *bitset.Bitset) *bitset.Bitset {
	// min < v
	lt := newFactory(mins.Type()).New(FilterModeLt)
	lt.WithValue(m.val)
	return lt.MatchVector(mins, bits, mask)
}

// LE ---

type i128LeMatcher struct {
	i128Matcher
}

func (m i128LeMatcher) MatchValue(v any) bool {
	return m.val.Gte(v.(num.Int128))
}

func (m i128LeMatcher) MatchRange(from, to any) bool {
	return m.val.Gte(from.(num.Int128))
}

func (m i128LeMatcher) MatchRangeVectors(mins, _ *block.Block, bits, mask *bitset.Bitset) *bitset.Bitset {
	// min <= v
	le := newFactory(mins.Type()).New(FilterModeLe)
	le.WithValue(m.val)
	return le.MatchVector(mins, bits, mask)
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
	return m.from.Lte(v.(num.Int128)) && m.to.Gte(v.(num.Int128))
}

func (m i128RangeMatcher) MatchRange(from, to any) bool {
	return !(from.(num.Int128).Gt(m.to) || to.(num.Int128).Lt(m.from))
}

func (m i128RangeMatcher) MatchVector(b *block.Block, bits, mask *bitset.Bitset) *bitset.Bitset {
	return cmp.MatchInt128Between(*b.Int128(), m.from, m.to, bits, mask)
}

func (m i128RangeMatcher) MatchRangeVectors(mins, maxs *block.Block, bits, mask *bitset.Bitset) *bitset.Bitset {
	// min <= to && max >= from
	f := newFactory(mins.Type())
	le, ge := f.New(FilterModeLe), f.New(FilterModeGe)
	le.WithValue(m.to)
	ge.WithValue(m.from)
	minBits := le.MatchVector(mins, nil, mask)
	if mask != nil {
		minBits.And(mask)
	}
	bits = ge.MatchVector(maxs, bits, minBits)
	bits.And(minBits)
	minBits.Close()
	return bits
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

func (m i128InSetMatcher) MatchVector(b *block.Block, bits, mask *bitset.Bitset) *bitset.Bitset {
	stride := b.Int128()
	if mask != nil {
		// skip masked values
		for i, l := 0, stride.Len(); i < l; i++ {
			if !mask.IsSet(i) {
				continue
			}
			if num.Int128Contains(m.slice, stride.Elem(i)) {
				bits.Set(i)
			}
		}
	} else {
		for i, l := 0, stride.Len(); i < l; i++ {
			if num.Int128Contains(m.slice, stride.Elem(i)) {
				bits.Set(i)
			}
		}
	}
	return bits
}

func (m i128InSetMatcher) MatchRangeVectors(mins, maxs *block.Block, bits, mask *bitset.Bitset) *bitset.Bitset {
	setMin, setMax := m.slice[0], m.slice[len(m.slice)-1]
	rg := newFactory(mins.Type()).New(FilterModeRange)
	rg.WithValue(RangeValue{setMin, setMax})
	return rg.MatchRangeVectors(mins, maxs, bits, mask)
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

func (m i128NotInSetMatcher) MatchVector(b *block.Block, bits, mask *bitset.Bitset) *bitset.Bitset {
	stride := b.Int128()
	if mask != nil {
		// skip masked values
		for i, l := 0, stride.Len(); i < l; i++ {
			if !mask.IsSet(i) {
				continue
			}
			if !num.Int128Contains(m.slice, stride.Elem(i)) {
				bits.Set(i)
			}
		}
	} else {
		for i, l := 0, stride.Len(); i < l; i++ {
			if !num.Int128Contains(m.slice, stride.Elem(i)) {
				bits.Set(i)
			}
		}
	}
	return bits
}

func (m i128NotInSetMatcher) MatchRangeVectors(_, _ *block.Block, bits, mask *bitset.Bitset) *bitset.Bitset {
	// undecided, always true
	if mask != nil {
		bits.Copy(mask)
	} else {
		bits.One()
	}
	return bits

}
