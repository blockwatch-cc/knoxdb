// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package query

import (
	"slices"

	"blockwatch.cc/knoxdb/internal/bitset"
	"blockwatch.cc/knoxdb/internal/block"
	"blockwatch.cc/knoxdb/internal/filter"
	"blockwatch.cc/knoxdb/internal/filter/bloom"
	"blockwatch.cc/knoxdb/internal/hash"
	"blockwatch.cc/knoxdb/pkg/slicex"
)

// IN ---

// In, Contains
type floatInSetMatcher[T Number] struct {
	noopMatcher
	slice  slicex.OrderedNumbers[T]
	hashes []hash.HashValue
}

func (m *floatInSetMatcher[T]) Weight() int { return m.slice.Len() }

func (m *floatInSetMatcher[T]) Len() int { return m.slice.Len() }

func (m *floatInSetMatcher[T]) Value() any {
	return m.slice.Values
}

func (m *floatInSetMatcher[T]) WithValue(val any) {
	m.WithSlice(val)
}

func (m *floatInSetMatcher[T]) WithSlice(slice any) {
	data := slice.([]T)
	slices.Sort(data)
	m.slice.Values = data
	m.hashes = hash.HashAnySlice(data)
}

func (m floatInSetMatcher[T]) MatchValue(v any) bool {
	return m.slice.Contains(v.(T))
}

func (m floatInSetMatcher[T]) MatchRange(from, to any) bool {
	return m.slice.ContainsRange(from.(T), to.(T))
}

func (m floatInSetMatcher[T]) MatchFilter(flt filter.Filter) bool {
	if x, ok := flt.(*bloom.Filter); ok {
		return x.ContainsAnyHash(m.hashes)
	}
	for _, h := range m.hashes {
		if flt.Contains(h.Uint64()) {
			return true
		}
	}
	return false
}

func (m floatInSetMatcher[T]) MatchVector(b *block.Block, bits, mask *bitset.Bitset) *bitset.Bitset {
	acc := block.NewBlockAccessor[T](b)
	if mask != nil {
		// skip masked values
		for i, v := range acc.Slice() {
			if !mask.IsSet(i) {
				continue
			}
			if m.slice.Contains(v) {
				bits.Set(i)
			}
		}
	} else {
		for i, v := range acc.Slice() {
			if m.slice.Contains(v) {
				bits.Set(i)
			}
		}
	}
	return bits
}

func (m floatInSetMatcher[T]) MatchRangeVectors(mins, maxs *block.Block, bits, mask *bitset.Bitset) *bitset.Bitset {
	setMin, setMax := m.slice.MinMax()
	rg := newFactory(mins.Type()).New(FilterModeRange)
	rg.WithValue(RangeValue{setMin, setMax})
	return rg.MatchRangeVectors(mins, maxs, bits, mask)
}

// NOT IN ---

type floatNotInSetMatcher[T Number] struct {
	noopMatcher
	slice slicex.OrderedNumbers[T]
}

func (m *floatNotInSetMatcher[T]) Weight() int { return m.slice.Len() }

func (m *floatNotInSetMatcher[T]) Len() int { return m.slice.Len() }

func (m *floatNotInSetMatcher[T]) Value() any {
	return m.slice.Values
}

func (m *floatNotInSetMatcher[T]) WithValue(val any) {
	m.WithSlice(val)
}

func (m *floatNotInSetMatcher[T]) WithSlice(slice any) {
	bits := slice.([]T)
	slices.Sort(bits)
	m.slice.Values = bits
}

func (m floatNotInSetMatcher[T]) MatchValue(v any) bool {
	return !m.slice.Contains(v.(T))
}

func (m floatNotInSetMatcher[T]) MatchRange(from, to any) bool {
	return !m.slice.ContainsRange(from.(T), to.(T))
}

func (m floatNotInSetMatcher[T]) MatchFilter(_ filter.Filter) bool {
	// we don't know generally, so full scan is always required
	return true
}

func (m floatNotInSetMatcher[T]) MatchVector(b *block.Block, bits, mask *bitset.Bitset) *bitset.Bitset {
	acc := block.NewBlockAccessor[T](b)
	if mask != nil {
		// skip masked values
		for i, v := range acc.Slice() {
			if !mask.IsSet(i) {
				continue
			}
			if !m.slice.Contains(v) {
				bits.Set(i)
			}
		}
	} else {
		for i, v := range acc.Slice() {
			if !m.slice.Contains(v) {
				bits.Set(i)
			}
		}
	}
	return bits
}

func (m floatNotInSetMatcher[T]) MatchRangeVectors(_, _ *block.Block, bits, mask *bitset.Bitset) *bitset.Bitset {
	// undecided, always true
	if mask != nil {
		bits.Copy(mask)
	} else {
		bits.One()
	}
	return bits
}
