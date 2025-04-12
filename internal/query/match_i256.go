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

type i256MatchFunc func(src num.Int256Stride, val num.Int256, bits, mask *bitset.Bitset) *bitset.Bitset

type I256MatcherFactory struct{}

func (f I256MatcherFactory) New(m FilterMode) Matcher {
	switch m {
	case FilterModeEqual:
		return &i256EqualMatcher{i256Matcher{match: cmp.MatchInt256Equal}}
	case FilterModeNotEqual:
		return &i256NotEqualMatcher{i256Matcher{match: cmp.MatchInt256NotEqual}}
	case FilterModeGt:
		return &i256GtMatcher{i256Matcher{match: cmp.MatchInt256Greater}}
	case FilterModeGe:
		return &i256GeMatcher{i256Matcher{match: cmp.MatchInt256GreaterEqual}}
	case FilterModeLt:
		return &i256LtMatcher{i256Matcher{match: cmp.MatchInt256Less}}
	case FilterModeLe:
		return &i256LeMatcher{i256Matcher{match: cmp.MatchInt256LessEqual}}
	case FilterModeRange:
		return &i256RangeMatcher{}
	case FilterModeIn:
		return &i256InSetMatcher{}
	case FilterModeNotIn:
		return &i256NotInSetMatcher{}
	default:
		// unsupported
		// FilterModeRegexp:
		return &noopMatcher{}
	}
}

type i256Matcher struct {
	noopMatcher
	match i256MatchFunc
	val   num.Int256
}

func (m *i256Matcher) Weight() int { return 4 }

func (m *i256Matcher) WithValue(v any) {
	m.val = v.(num.Int256)
}

func (m *i256Matcher) Value() any {
	return m.val
}

func (m i256Matcher) MatchVector(b *block.Block, bits, mask *bitset.Bitset) *bitset.Bitset {
	return m.match(*b.Int256(), m.val, bits, mask)
}

func (m i256Matcher) MatchRangeVectors(mins, maxs *block.Block, bits, mask *bitset.Bitset) *bitset.Bitset {
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

// EQUAL ---

type i256EqualMatcher struct {
	i256Matcher
}

func (m i256EqualMatcher) MatchValue(v any) bool {
	return m.val.Eq(v.(num.Int256))
}

func (m i256EqualMatcher) MatchRange(from, to any) bool {
	return !(m.val.Lt(from.(num.Int256)) || m.val.Gt(to.(num.Int256)))
}

func (m i256EqualMatcher) MatchRangeVectors(mins, maxs *block.Block, bits, mask *bitset.Bitset) *bitset.Bitset {
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

type i256NotEqualMatcher struct {
	i256Matcher
}

func (m i256NotEqualMatcher) MatchValue(v any) bool {
	return !m.val.Eq(v.(num.Int256))
}

func (m i256NotEqualMatcher) MatchRange(from, to any) bool {
	return m.val.Lt(from.(num.Int256)) || m.val.Gt(to.(num.Int256))
}

func (m i256NotEqualMatcher) MatchRangeVectors(_, _ *block.Block, bits, mask *bitset.Bitset) *bitset.Bitset {
	// undecided, always true
	if mask != nil {
		bits.Copy(mask)
	} else {
		bits.One()
	}
	return bits
}

// GT ---

type i256GtMatcher struct {
	i256Matcher
}

func (m i256GtMatcher) MatchValue(v any) bool {
	return m.val.Lt(v.(num.Int256))
}

func (m i256GtMatcher) MatchRange(_, to any) bool {
	return m.val.Lt(to.(num.Int256))
}

func (m i256GtMatcher) MatchRangeVectors(_, maxs *block.Block, bits, mask *bitset.Bitset) *bitset.Bitset {
	// max > v
	gt := newFactory(maxs.Type()).New(FilterModeGt)
	gt.WithValue(m.val)
	return gt.MatchVector(maxs, bits, mask)
}

// GE ---

type i256GeMatcher struct {
	i256Matcher
}

func (m i256GeMatcher) MatchValue(v any) bool {
	return m.val.Lte(v.(num.Int256))
}

func (m i256GeMatcher) MatchRange(from, to any) bool {
	return m.val.Lte(to.(num.Int256))
}

func (m i256GeMatcher) MatchRangeVectors(_, maxs *block.Block, bits, mask *bitset.Bitset) *bitset.Bitset {
	// max >= v
	ge := newFactory(maxs.Type()).New(FilterModeGe)
	ge.WithValue(m.val)
	return ge.MatchVector(maxs, bits, mask)
}

// LT ---

type i256LtMatcher struct {
	i256Matcher
}

func (m i256LtMatcher) MatchValue(v any) bool {
	return m.val.Gt(v.(num.Int256))
}

func (m i256LtMatcher) MatchRange(from, to any) bool {
	return m.val.Gt(from.(num.Int256))
}

func (m i256LtMatcher) MatchRangeVectors(mins, _ *block.Block, bits, mask *bitset.Bitset) *bitset.Bitset {
	// min < v
	lt := newFactory(mins.Type()).New(FilterModeLt)
	lt.WithValue(m.val)
	return lt.MatchVector(mins, bits, mask)
}

// LE ---

type i256LeMatcher struct {
	i256Matcher
}

func (m i256LeMatcher) MatchValue(v any) bool {
	return m.val.Gte(v.(num.Int256))
}

func (m i256LeMatcher) MatchRange(from, to any) bool {
	return m.val.Gte(from.(num.Int256))
}

func (m i256LeMatcher) MatchRangeVectors(mins, _ *block.Block, bits, mask *bitset.Bitset) *bitset.Bitset {
	// min <= v
	le := newFactory(mins.Type()).New(FilterModeLe)
	le.WithValue(m.val)
	return le.MatchVector(mins, bits, mask)
}

// RANGE ---

type i256RangeMatcher struct {
	noopMatcher
	from num.Int256
	to   num.Int256
}

func (m *i256RangeMatcher) Weight() int { return 8 }

func (m *i256RangeMatcher) Len() int { return 2 }

func (m *i256RangeMatcher) WithValue(v any) {
	val := v.(RangeValue)
	m.from = val[0].(num.Int256)
	m.to = val[1].(num.Int256)
}

func (m *i256RangeMatcher) Value() any {
	val := RangeValue{m.from, m.to}
	return val
}

func (m i256RangeMatcher) MatchValue(v any) bool {
	return m.from.Lte(v.(num.Int256)) && m.to.Gte(v.(num.Int256))
}

func (m i256RangeMatcher) MatchRange(from, to any) bool {
	return !(from.(num.Int256).Gt(m.to) || to.(num.Int256).Lt(m.from))
}

func (m i256RangeMatcher) MatchVector(b *block.Block, bits, mask *bitset.Bitset) *bitset.Bitset {
	return cmp.MatchInt256Between(*b.Int256(), m.from, m.to, bits, mask)
}

func (m i256RangeMatcher) MatchRangeVectors(mins, maxs *block.Block, bits, mask *bitset.Bitset) *bitset.Bitset {
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
type i256InSetMatcher struct {
	noopMatcher
	slice  []num.Int256
	hashes []filter.HashValue
}

func (m *i256InSetMatcher) Weight() int { return len(m.slice) }

func (m *i256InSetMatcher) Len() int { return len(m.slice) }

func (m *i256InSetMatcher) Value() any {
	return m.slice
}

func (m *i256InSetMatcher) WithValue(val any) {
	m.WithSlice(val)
}

func (m *i256InSetMatcher) WithSlice(slice any) {
	m.slice = num.Int256Sort(slice.([]num.Int256))
	m.hashes = filter.HashMulti(m.slice)
}

func (m i256InSetMatcher) MatchValue(v any) bool {
	return num.Int256Contains(m.slice, v.(num.Int256))
}

func (m i256InSetMatcher) MatchRange(from, to any) bool {
	return num.Int256ContainsRange(m.slice, from.(num.Int256), to.(num.Int256))
}

func (m i256InSetMatcher) MatchFilter(flt filter.Filter) bool {
	return flt.ContainsAny(m.hashes)
}

func (m i256InSetMatcher) MatchVector(b *block.Block, bits, mask *bitset.Bitset) *bitset.Bitset {
	stride := b.Int256()
	if mask != nil {
		// skip masked values
		for i, l := 0, stride.Len(); i < l; i++ {
			if !mask.IsSet(i) {
				continue
			}
			if num.Int256Contains(m.slice, stride.Elem(i)) {
				bits.Set(i)
			}
		}
	} else {
		for i, l := 0, stride.Len(); i < l; i++ {
			if num.Int256Contains(m.slice, stride.Elem(i)) {
				bits.Set(i)
			}
		}
	}
	return bits
}

func (m i256InSetMatcher) MatchRangeVectors(mins, maxs *block.Block, bits, mask *bitset.Bitset) *bitset.Bitset {
	setMin, setMax := m.slice[0], m.slice[len(m.slice)-1]
	rg := newFactory(mins.Type()).New(FilterModeRange)
	rg.WithValue(RangeValue{setMin, setMax})
	return rg.MatchRangeVectors(mins, maxs, bits, mask)
}

// NOT IN ---

type i256NotInSetMatcher struct {
	noopMatcher
	slice []num.Int256
}

func (m *i256NotInSetMatcher) Weight() int { return len(m.slice) }

func (m *i256NotInSetMatcher) Len() int { return len(m.slice) }

func (m *i256NotInSetMatcher) Value() any {
	return m.slice
}

func (m *i256NotInSetMatcher) WithValue(val any) {
	m.WithSlice(val)
}

func (m *i256NotInSetMatcher) WithSlice(slice any) {
	m.slice = num.Int256Sort(slice.([]num.Int256))
}

func (m i256NotInSetMatcher) MatchValue(v any) bool {
	return !num.Int256Contains(m.slice, v.(num.Int256))
}

func (m i256NotInSetMatcher) MatchRange(from, to any) bool {
	return !num.Int256ContainsRange(m.slice, from.(num.Int256), to.(num.Int256))
}

func (m i256NotInSetMatcher) MatchFilter(_ filter.Filter) bool {
	// we don't know generally, so full scan is always required
	return true
}

func (m i256NotInSetMatcher) MatchVector(b *block.Block, bits, mask *bitset.Bitset) *bitset.Bitset {
	stride := b.Int256()
	if mask != nil {
		// skip masked values
		for i, l := 0, stride.Len(); i < l; i++ {
			if !mask.IsSet(i) {
				continue
			}
			if !num.Int256Contains(m.slice, stride.Elem(i)) {
				bits.Set(i)
			}
		}
	} else {
		for i, l := 0, stride.Len(); i < l; i++ {
			if !num.Int256Contains(m.slice, stride.Elem(i)) {
				bits.Set(i)
			}
		}
	}
	return bits
}

func (m i256NotInSetMatcher) MatchRangeVectors(_, _ *block.Block, bits, mask *bitset.Bitset) *bitset.Bitset {
	// undecided, always true
	if mask != nil {
		bits.Copy(mask)
	} else {
		bits.One()
	}
	return bits

}
