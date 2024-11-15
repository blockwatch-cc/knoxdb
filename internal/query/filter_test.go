// Copyright (c) 2024 Blockwatch Data Inc.
// Author: oliver@blockwatch.cc

package query

import (
	"blockwatch.cc/knoxdb/internal/bitset"
	"blockwatch.cc/knoxdb/internal/block"
	"blockwatch.cc/knoxdb/internal/filter/bloom"
	"blockwatch.cc/knoxdb/internal/xroar"
	"github.com/stretchr/testify/assert"
	"testing"
)

// MockMatcher is a mock implementation of the Matcher interface for testing purposes.
type MockMatcher struct {
	WeightVal int
}

// MatchValue simulates a value match by always returning true, regardless of the input value.
func (m *MockMatcher) MatchValue(value any) bool {
	return true
}

// MatchRange simulates a range match by always returning true, regardless of the range values.
func (m *MockMatcher) MatchRange(min, max any) bool {
	return true
}

// MatchBitmap simulates a bitmap match by always returning true, regardless of the input bitmap.
func (m *MockMatcher) MatchBitmap(bits *xroar.Bitmap) bool {
	return true
}

// MatchBlock simulates a block match by returning the `bits` parameter unchanged, regardless of the block or mask inputs.
func (m *MockMatcher) MatchBlock(block *block.Block, bits, mask *bitset.Bitset) *bitset.Bitset {
	return bits
}

// MatchBloom simulates a match against a bloom filter by always returning true, regardless of the filter's state.
func (m *MockMatcher) MatchBloom(filter *bloom.Filter) bool {
	return true
}

// WithValue simulates setting a value for the matcher; implementation is a no-op for testing purposes.
func (m *MockMatcher) WithValue(value any) {}

// WithSlice simulates setting a slice for the matcher; implementation is a no-op for testing purposes.
func (m *MockMatcher) WithSlice(value any) {}

// WithSet simulates setting a bitmap for the matcher; implementation is a no-op for testing purposes.
func (m *MockMatcher) WithSet(set *xroar.Bitmap) {}

// Value always returns nil to simulate a matcher with no stored value.
func (m *MockMatcher) Value() any {
	return nil
}

// Len returns 1 to represent a constant length for testing purposes.
func (m *MockMatcher) Len() int {
	return 1
}

// Weight returns the matcher's weight as a fixed value for testing purposes.
func (m *MockMatcher) Weight() int {
	return m.WeightVal
}

// newMockMatcher instantiates a MockMatcher with a fixed weight value for use in tests.
func newMockMatcher(weight int) Matcher {
	return &MockMatcher{WeightVal: weight}
}

// makeRandomFilter creates a Filter with a specified name, mode, and value, using a mock matcher for testing purposes.
func makeRandomFilter(name string, mode FilterMode, value any) *Filter {
	return &Filter{
		Name:    name,
		Mode:    mode,
		Matcher: newMockMatcher(1), // Using mock matcher for simplicity
		Value:   value,
	}
}

// makeRandomTree constructs a FilterTreeNode with the specified depth and number of leaves, recursively adding child nodes and filters for testing purposes.
func makeRandomTree(depth int, numLeaves int) *FilterTreeNode {
	root := &FilterTreeNode{OrKind: COND_AND}
	for i := 0; i < numLeaves; i++ {
		root.AddAndFilter(makeRandomFilter("field", FilterModeEqual, i))
	}
	if depth > 1 {
		child := makeRandomTree(depth-1, numLeaves/2)
		root.AddNode(child)
	}
	return root
}

// TestFilterTreeNodeValidation validates the correctness of FilterTreeNode configurations by checking proper behavior for valid and invalid trees.
func TestFilterTreeNodeValidation(t *testing.T) {
	validTree := makeRandomTree(2, 4)
	assert.NoError(t, validTree.Validate("root"))

	invalidLeaf := &FilterTreeNode{
		Children: nil,
		Filter:   nil,
	}
	assert.Error(t, invalidLeaf.Validate("root"))

	invalidParent := &FilterTreeNode{
		Children: []*FilterTreeNode{{Filter: nil}},
		Filter:   nil,
	}
	assert.Error(t, invalidParent.Validate("root"))
}

// TestFilterMatchValue tests whether a filter correctly simulates a value match using its matcher.
func TestFilterMatchValue(t *testing.T) {
	filter := makeRandomFilter("test_field", FilterModeEqual, 42)
	assert.True(t, filter.Matcher.MatchValue(42))
}

// TestFilterWeightCalculation verifies that the weight calculation for filters matches the expected fixed value.
func TestFilterWeightCalculation(t *testing.T) {
	filter := makeRandomFilter("test_field", FilterModeEqual, 42)
	assert.Equal(t, 1, filter.Matcher.Weight())
}
