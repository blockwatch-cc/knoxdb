// Copyright (c) 2023 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package query

import (
	"blockwatch.cc/knoxdb/internal/bitset"
	"blockwatch.cc/knoxdb/internal/block"
	"blockwatch.cc/knoxdb/internal/cmp"
	"blockwatch.cc/knoxdb/internal/filter/bloom"
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

func (m i256Matcher) MatchBlock(b *block.Block, bits, mask *bitset.Bitset) *bitset.Bitset {
	return m.match(*b.Int256(), m.val, bits, mask)
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

func (m i256RangeMatcher) MatchBlock(b *block.Block, bits, mask *bitset.Bitset) *bitset.Bitset {
	return cmp.MatchInt256Between(*b.Int256(), m.from, m.to, bits, mask)
}

// IN ---

// In, Contains
type i256InSetMatcher struct {
	noopMatcher
	slice  []num.Int256
	hashes [][2]uint32
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
	m.hashes = bloom.HashAnySlice(m.slice)
}

func (m i256InSetMatcher) MatchValue(v any) bool {
	return num.Int256Contains(m.slice, v.(num.Int256))
}

func (m i256InSetMatcher) MatchRange(from, to any) bool {
	return num.Int256ContainsRange(m.slice, from.(num.Int256), to.(num.Int256))
}

func (m i256InSetMatcher) MatchBloom(flt *bloom.Filter) bool {
	return flt.ContainsAnyHash(m.hashes)
}

func (m i256InSetMatcher) MatchBlock(b *block.Block, bits, mask *bitset.Bitset) *bitset.Bitset {
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

func (m i256NotInSetMatcher) MatchBloom(flt *bloom.Filter) bool {
	// we don't know generally, so full scan is always required
	return true
}

func (m i256NotInSetMatcher) MatchBlock(b *block.Block, bits, mask *bitset.Bitset) *bitset.Bitset {
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
