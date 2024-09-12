// Copyright (c) 2023 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package query

import (
	"blockwatch.cc/knoxdb/internal/bitset"
	"blockwatch.cc/knoxdb/internal/block"
	"blockwatch.cc/knoxdb/internal/cmp"
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
	default:
		// unsupported
		// FilterModeIn, FilterModeNotIn, FilterModeRegexp:
		return &noopMatcher{}
	}
}

type i256Matcher struct {
	noopMatcher
	match i256MatchFunc
	val   num.Int256
}

func (m *i256Matcher) WithValue(v any) Matcher {
	m.val = v.(num.Int256)
	return m
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

func (m *i256RangeMatcher) Weight() int { return 2 }

func (m *i256RangeMatcher) WithValue(v any) Matcher {
	val := v.(RangeValue)
	m.from = val[0].(num.Int256)
	m.to = val[1].(num.Int256)
	return m
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
