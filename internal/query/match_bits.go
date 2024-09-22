// Copyright (c) 2023 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package query

import (
	"blockwatch.cc/knoxdb/internal/bitset"
	"blockwatch.cc/knoxdb/internal/block"
)

type bitsetMatchFunc func(src *bitset.Bitset, val bool, bits, mask *bitset.Bitset) *bitset.Bitset

type BitMatcherFactory struct{}

func (f BitMatcherFactory) New(m FilterMode) Matcher {
	switch m {
	case FilterModeEqual:
		return &bitEqualMatcher{}
	case FilterModeNotEqual:
		return &bitNotEqualMatcher{}
	default:
		// any other mode is unsupported
		return &noopMatcher{}
	}
}

// EQUAL

type bitEqualMatcher struct {
	noopMatcher
	val bool
}

func (m *bitEqualMatcher) WithValue(v any) {
	m.val = v.(bool)
}

func (m *bitEqualMatcher) Value() any {
	return m.val
}

func (m bitEqualMatcher) MatchValue(v any) bool {
	return m.val == v.(bool)
}

func (m bitEqualMatcher) MatchRange(from, to any) bool {
	if from.(bool) == to.(bool) {
		return m.val == from.(bool)
	}
	return true
}

func (m bitEqualMatcher) MatchBlock(b *block.Block, bits, mask *bitset.Bitset) *bitset.Bitset {
	if m.val {
		return bits.Copy(b.Bool())
	} else {
		return bits.Copy(b.Bool()).Neg()
	}
}

// NOT EQUAL

type bitNotEqualMatcher struct {
	bitEqualMatcher
	val bool
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

func (m bitNotEqualMatcher) MatchBlock(b *block.Block, bits, mask *bitset.Bitset) *bitset.Bitset {
	if !m.val {
		return bits.Copy(b.Bool())
	} else {
		return bits.Copy(b.Bool()).Neg()
	}
}
