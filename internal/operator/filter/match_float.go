// Copyright (c) 2024-2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package filter

import (
	"slices"

	"blockwatch.cc/knoxdb/internal/bitset"
	"blockwatch.cc/knoxdb/internal/block"
	"blockwatch.cc/knoxdb/internal/filter"
	"blockwatch.cc/knoxdb/internal/hash"
	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/slicex"
)

// IN ---

// In, Contains
type floatInSetMatcher[T types.Float] struct {
	noopMatcher
	slice  []T
	hashes []uint64
}

func (m *floatInSetMatcher[T]) Weight() int { return len(m.slice) }

func (m *floatInSetMatcher[T]) Len() int { return len(m.slice) }

func (m *floatInSetMatcher[T]) Value() any {
	return m.slice
}

func (m *floatInSetMatcher[T]) WithValue(val any) {
	m.WithSlice(val)
}

func (m *floatInSetMatcher[T]) WithSlice(slice any) {
	data := slice.([]T)
	slices.Sort(data)
	m.slice = data
	m.hashes = hash.Vec(m.hashes, data)
}

func (m floatInSetMatcher[T]) MatchValue(v any) bool {
	return slicex.ContainsSorted(m.slice, v.(T))
}

func (m floatInSetMatcher[T]) MatchRange(from, to any) bool {
	return slicex.ContainsRangeSorted(m.slice, from.(T), to.(T))
}

func (m floatInSetMatcher[T]) MatchFilter(flt filter.Filter) bool {
	return flt.ContainsAny(m.hashes)
}

func (m floatInSetMatcher[T]) MatchVector(b *block.Block, bits, mask *bitset.Bitset) {
	// Note: float compression containers do not implement set matching
	acc := block.NewAccessor[T](b)
	if mask != nil {
		for i := range mask.Ones() {
			if slicex.ContainsSorted(m.slice, acc.Get(i)) {
				bits.Set(i)
			}
		}
	} else {
		for i, v := range acc.Slice() {
			if slicex.ContainsSorted(m.slice, v) {
				bits.Set(i)
			}
		}
	}
}

func (m floatInSetMatcher[T]) MatchRangeVectors(mins, maxs *block.Block, bits, mask *bitset.Bitset) {
	setMin, setMax, _ := slicex.RangeSorted(m.slice)
	rg := newFactory(mins.Type()).New(FilterModeRange)
	rg.WithValue(RangeValue{setMin, setMax})
	rg.MatchRangeVectors(mins, maxs, bits, mask)
}

// NOT IN ---

type floatNotInSetMatcher[T types.Float] struct {
	noopMatcher
	slice []T
}

func (m *floatNotInSetMatcher[T]) Weight() int { return len(m.slice) }

func (m *floatNotInSetMatcher[T]) Len() int { return len(m.slice) }

func (m *floatNotInSetMatcher[T]) Value() any {
	return m.slice
}

func (m *floatNotInSetMatcher[T]) WithValue(val any) {
	m.WithSlice(val)
}

func (m *floatNotInSetMatcher[T]) WithSlice(slice any) {
	bits := slice.([]T)
	slices.Sort(bits)
	m.slice = bits
}

func (m floatNotInSetMatcher[T]) MatchValue(v any) bool {
	return !slicex.ContainsSorted(m.slice, v.(T))
}

func (m floatNotInSetMatcher[T]) MatchRange(from, to any) bool {
	return !slicex.ContainsRangeSorted(m.slice, from.(T), to.(T))
}

func (m floatNotInSetMatcher[T]) MatchFilter(_ filter.Filter) bool {
	// we don't know generally, so full scan is always required
	return true
}

func (m floatNotInSetMatcher[T]) MatchVector(b *block.Block, bits, mask *bitset.Bitset) {
	// Note: float compression containers do not implement set matching
	acc := block.NewAccessor[T](b)
	if mask != nil {
		for i := range mask.Ones() {
			if !slicex.ContainsSorted(m.slice, acc.Get(i)) {
				bits.Set(i)
			}
		}
	} else {
		for i, v := range acc.Slice() {
			if !slicex.ContainsSorted(m.slice, v) {
				bits.Set(i)
			}
		}
	}
}

func (m floatNotInSetMatcher[T]) MatchRangeVectors(_, _ *block.Block, bits, mask *bitset.Bitset) {
	// undecided, always true
	if mask != nil {
		bits.Copy(mask)
	} else {
		bits.One()
	}
}
