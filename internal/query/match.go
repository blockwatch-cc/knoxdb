// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package query

import (
	"blockwatch.cc/knoxdb/internal/bitset"
	"blockwatch.cc/knoxdb/internal/block"
	"blockwatch.cc/knoxdb/internal/filter"
	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/internal/xroar"
)

var NoopMatcher = &noopMatcher{}

// Matcher defines a common interface for comparison operations regardless
// of data type and mode.
type Matcher interface {
	// Initializes the matcher with a fixed scalar value to match against.
	// The interface must match precisely the Go type this matcher is
	// implementing. Use schema.Caster and schema.Parser for generating
	// correct types. For range mode matchers use RangeValue.
	WithValue(any)

	// Initializes the matcher with a slice of values to match against.
	// the interface must be of slice type and a compatible element type.
	WithSlice(any)

	// Initializes the matcher with a bitmap of integer values to match against.
	// Only applicable to IN, NIN mode matcher types.
	WithSet(*xroar.Bitmap)

	// Returns the matchers algorithmic complexity to make cost-based match tree
	// reorganization decisions. Weight is based on the number and run-time
	// complexity of comparison operations. Bitset membership checks and simple
	// integer comparisons have a low weight, regexp and byte array comparisons
	// have a heigher weight.
	Weight() int

	// Returns the number of values each candidate will be matched against.
	// Typically one for most modes, two for range and N for set based matching.
	Len() int

	// Returns the matcher's value, either single element type, RangeValue
	// or slice of elements.
	Value() any

	// Matches against a single candidate value which must be of same type
	// as the matcher.
	MatchValue(any) bool

	// Matches min/max candidate ranges against the matcher's value. Single
	// value matchers return true when the matcher's configured value is
	// within range. RangeValue matchers return true when both ranges overlap.
	// Set matchers return true when any set members are within the candidate range.
	MatchRange(any, any) bool

	// Returns true when any of the configured matcher values is in the
	// given filter.
	MatchFilter(filter.Filter) bool

	// Returns a bitset of matching positions for a column vector. For efficieny
	// expectes a pre-allocated bitset res which will be filled and returned as result.
	// Optional mask allows to skip values from being matched. Masks are useful
	// to skip earlier non-matches for AND conditions or cover only non-matches for
	// OR condtions.
	MatchVector(block *block.Block, res *bitset.Bitset, mask *bitset.Bitset)

	// Vectorized match for min/max candidate ranges against the matcher's value.
	// Returns a bitset of matching positions for the pair of min/max column vectors.
	// Single value matchers return true when the matcher's configured value is
	// within a range. RangeValue matchers return true when both ranges overlap.
	// Set matchers return true when any set members are within candidate ranges.
	MatchRangeVectors(mins, maxs *block.Block, res *bitset.Bitset, mask *bitset.Bitset)
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
	return newFactory(ftyp.BlockType())
}

func newFactory(typ types.BlockType) MatcherFactory {
	switch typ {
	case BlockInt64:
		return NumMatcherFactory[int64]{typ}
	case BlockBool:
		return BitMatcherFactory{}
	case BlockBytes:
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

func (m *noopMatcher) WithValue(_ any) {}

func (m *noopMatcher) WithSlice(_ any) {}

func (m *noopMatcher) WithSet(_ *xroar.Bitmap) {}

func (m *noopMatcher) Weight() int { return 1 }

func (m *noopMatcher) Len() int { return 1 }

func (m *noopMatcher) Value() any { return nil }

func (m noopMatcher) MatchValue(_ any) bool { return false }

func (m noopMatcher) MatchRange(_, _ any) bool { return false }

func (m noopMatcher) MatchFilter(_ filter.Filter) bool { return false }

func (m noopMatcher) MatchVector(_ *block.Block, bits, mask *bitset.Bitset) {
	if mask != nil {
		bits.Copy(mask)
	}
}

func (m noopMatcher) MatchRangeVectors(_, _ *block.Block, bits, mask *bitset.Bitset) {
	if mask != nil {
		bits.Copy(mask)
	}
}

// func MatchTree(n *FilterTreeNode, v *schema.View) bool {
// 	// handle always true conditions
// 	if n.IsAnyMatch() {
// 		return true
// 	}

// 	// handle always false conditions
// 	if n.IsNoMatch() {
// 		return false
// 	}

// 	// match leaf filter
// 	if n.IsLeaf() {
// 		return MatchFilter(n.Filter, v)
// 	}

// 	// match and aggregate all children
// 	if n.OrKind {
// 		for _, c := range n.Children {
// 			if MatchTree(c, v) {
// 				return true
// 			}
// 		}
// 		return false
// 	} else {
// 		for _, c := range n.Children {
// 			if !MatchTree(c, v) {
// 				return false
// 			}
// 		}
// 		return true
// 	}
// }

// func MatchFilter(f *Filter, view *schema.View) bool {
// 	// get data value as interface
// 	v, ok := view.Get(int(f.Index))
// 	if !ok {
// 		return false
// 	}
// 	// compare against condition value
// 	return f.Matcher.MatchValue(v)
// }
