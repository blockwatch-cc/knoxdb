// Copyright (c) 2023 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package query

import (
	"blockwatch.cc/knoxdb/internal/bitset"
	"blockwatch.cc/knoxdb/internal/block"
	"blockwatch.cc/knoxdb/internal/cmp"
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
	default:
		// unsupported
		// FilterModeIn, FilterModeNotIn, FilterModeRegexp:
		return &noopMatcher{}
	}
}

type i128Matcher struct {
	noopMatcher
	match i128MatchFunc
	val   num.Int128
}

func (m *i128Matcher) WithValue(v any) {
	m.val = v.(num.Int128)
}

func (m i128Matcher) MatchBlock(b *block.Block, bits, mask *bitset.Bitset) *bitset.Bitset {
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

// RANGE ---

type i128RangeMatcher struct {
	noopMatcher
	from num.Int128
	to   num.Int128
}

func (m *i128RangeMatcher) Weight() int { return 2 }

func (m *i128RangeMatcher) WithRange(from, to any) {
	m.from = from.(num.Int128)
	m.to = to.(num.Int128)
}

func (m i128RangeMatcher) MatchValue(v any) bool {
	return m.from.Lte(v.(num.Int128)) && m.to.Gte(v.(num.Int128))
}

func (m i128RangeMatcher) MatchRange(from, to any) bool {
	return !(from.(num.Int128).Gt(m.to) || to.(num.Int128).Lt(m.from))
}

func (m i128RangeMatcher) MatchBlock(b *block.Block, bits, mask *bitset.Bitset) *bitset.Bitset {
	return cmp.MatchInt128Between(*b.Int128(), m.from, m.to, bits, mask)
}
