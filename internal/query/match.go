// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package query

import (
	"blockwatch.cc/knoxdb/internal/bitset"
	"blockwatch.cc/knoxdb/internal/block"
	"blockwatch.cc/knoxdb/internal/filter/bloom"
	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/internal/xroar"
	"blockwatch.cc/knoxdb/pkg/schema"
)

type Matcher interface {
	WithValue(any) Matcher                                                  // use a scalar value to match against
	WithSlice(any) Matcher                                                  // use slice of values to match against
	WithSet(*xroar.Bitmap) Matcher                                          // use bitmap of integer values to match against
	Weight() int                                                            // matcher complexity (number of values)
	Len() int                                                               // number of values in matcher, typically 1, more for sets
	Value() any                                                             // access matcher value (depends on type)
	MatchValue(any) bool                                                    // match a single value
	MatchRange(any, any) bool                                               // min/max checks in MaybeMatchPack()
	MatchBloom(*bloom.Filter) bool                                          // bloom filter match in MaybeMatchPack()
	MatchBitmap(*xroar.Bitmap) bool                                         // bitmap filter match in MaybeMatchPack()
	MatchBlock(*block.Block, *bitset.Bitset, *bitset.Bitset) *bitset.Bitset // full block compare with mask
}

// MatcherFactory is a factory object that can generate type based matchers
// for a given query filter mode. Not all type/mode combinations exists (e.g.
// only string/byte blocks support regexp matching).
type MatcherFactory interface {
	New(FilterMode) Matcher
}

// Need custom matchers for
// Time (maybe, currently int64 internally; if we were to introduce time-zones, then yes)
func NewFactory(ftyp types.FieldType) MatcherFactory {
	return newFactory(BlockTypes[ftyp])
}

func newFactory(typ types.BlockType) MatcherFactory {
	switch typ {
	case BlockTime, BlockInt64:
		return NumMatcherFactory[int64]{typ}
	case BlockBool:
		return BitMatcherFactory{}
	case BlockString, BlockBytes:
		return BytesMatcherFactory{}
	case BlockInt8:
		return NumMatcherFactory[int8]{typ}
	case BlockInt16:
		return NumMatcherFactory[int16]{typ}
	case BlockInt32:
		return NumMatcherFactory[int32]{typ}
	case BlockUint8:
		return NumMatcherFactory[uint8]{typ}
	case BlockUint16:
		return NumMatcherFactory[uint16]{typ}
	case BlockUint32:
		return NumMatcherFactory[uint32]{typ}
	case BlockUint64:
		return NumMatcherFactory[uint64]{typ}
	case BlockFloat32:
		return NumMatcherFactory[float32]{typ}
	case BlockFloat64:
		return NumMatcherFactory[float64]{typ}
	case BlockInt128:
		return I128MatcherFactory{}
	case BlockInt256:
		return I256MatcherFactory{}
	default:
		return nil
	}
}

// noopMatcher can be used for undefined type/mode combinations,
// e.g. regexp match on numeric fields
type noopMatcher struct{}

func (m *noopMatcher) WithValue(_ any) Matcher { return m }

func (m *noopMatcher) WithSlice(_ any) Matcher { return m }

func (m *noopMatcher) WithSet(_ *xroar.Bitmap) Matcher { return m }

func (m *noopMatcher) Weight() int { return 1 }

func (m *noopMatcher) Len() int { return 1 }

func (m *noopMatcher) Value() any { return nil }

func (m noopMatcher) MatchValue(_ any) bool { return false }

func (m noopMatcher) MatchRange(_, _ any) bool { return false }

func (m noopMatcher) MatchBloom(_ *bloom.Filter) bool { return false }

func (m noopMatcher) MatchBitmap(_ *xroar.Bitmap) bool { return false }

func (m noopMatcher) MatchBlock(_ *block.Block, bits, mask *bitset.Bitset) *bitset.Bitset {
	if mask != nil {
		return bits.Copy(mask)
	}
	return bits
}

func MatchTree(n *FilterTreeNode, v *schema.View) bool {
	// if root is empty and no leaf is defined, return a full match
	if n.IsEmpty() {
		return true
	}

	// if root contains a single leaf only, match it
	if n.IsLeaf() {
		return MatchFilter(n.Filter, v)
	}

	// process all children
	if n.OrKind {
		for _, c := range n.Children {
			if MatchTree(c, v) {
				return true
			}
		}
		return false
	} else {
		for _, c := range n.Children {
			if !MatchTree(c, v) {
				return false
			}
		}
		return true
	}
}

func MatchFilter(f *Filter, view *schema.View) bool {
	// get data value as interface
	v, ok := view.Get(int(f.Index))
	if !ok {
		return false
	}
	// compare against condition value
	return f.Matcher.MatchValue(v)
}
