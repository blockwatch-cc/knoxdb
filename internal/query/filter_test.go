// Copyright (c) 2024 Blockwatch Data Inc.
// Author: oliver@blockwatch.cc

package query

import (
	"fmt"
	"testing"

	"blockwatch.cc/knoxdb/internal/bitset"
	"blockwatch.cc/knoxdb/internal/block"
	"blockwatch.cc/knoxdb/internal/filter/bloom"
	"blockwatch.cc/knoxdb/internal/xroar"
	"blockwatch.cc/knoxdb/pkg/bitmap"
	"blockwatch.cc/knoxdb/pkg/util"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// MockMatcher is a basic implementation of the Matcher interface for testing purposes.
type MockMatcher struct {
	WeightVal int
}

// MatchValue always returns true, simulating a match for any value.
func (m *MockMatcher) MatchValue(value any) bool {
	return true
}

// MatchRange always returns true, simulating a range match for any min and max values.
func (m *MockMatcher) MatchRange(min, max any) bool {
	return true
}

// MatchBitmap always returns true, simulating a bitmap match.
func (m *MockMatcher) MatchBitmap(bits *xroar.Bitmap) bool {
	return true
}

// MatchBlock returns the input bits unchanged, simulating a valid block match.
func (m *MockMatcher) MatchBlock(block *block.Block, bits, mask *bitset.Bitset) *bitset.Bitset {
	return bits
}

// MatchBloom always returns true, simulating a match against a bloom filter.
func (m *MockMatcher) MatchBloom(filter *bloom.Filter) bool {
	return true
}

// WithValue sets a value for the matcher; implementation is simplified for testing.
func (m *MockMatcher) WithValue(value any) {}

// WithSlice sets a slice for the matcher; implementation is simplified for testing.
func (m *MockMatcher) WithSlice(value any) {}

// WithSet sets a bitmap for the matcher; implementation is simplified for testing.
func (m *MockMatcher) WithSet(set *xroar.Bitmap) {}

// Value returns nil, simulating the absence of a stored value.
func (m *MockMatcher) Value() any {
	return nil
}

// Len returns 1, representing a fixed weight for the matcher.
func (m *MockMatcher) Len() int {
	return 1
}

// Weight returns the matcher’s weight, used for testing purposes.
func (m *MockMatcher) Weight() int {
	return m.WeightVal
}

// newMockMatcher creates a new MockMatcher with the specified weight for testing purposes.
func newMockMatcher(weight int) Matcher {
	return &MockMatcher{WeightVal: weight}
}

// makeRandomFilter creates a Filter with a random name, mode, and value for testing.
func makeRandomFilter(name string, mode FilterMode, value any) *Filter {
	return &Filter{
		Name:    name,
		Mode:    mode,
		Matcher: newMockMatcher(1), // Using mock matcher for simplicity
		Value:   value,
	}
}

// makeRandomTree generates a random FilterTreeNode with the specified depth and number of leaves.
func makeRandomTree(depth int, numLeaves int) *FilterTreeNode {
	root := &FilterTreeNode{OrKind: COND_AND}
	for i := 0; i < numLeaves; i++ {
		root.AddAndFilter(makeRandomFilter("field"+util.ToString(i), FilterModeEqual, i))
	}
	if depth > 1 {
		child := makeRandomTree(depth-1, numLeaves/2)
		root.AddNode(child)
	}
	fmt.Printf("Generated tree: Size=%d, Depth=%d\n", root.Size(), root.Depth())
	return root
}

// TestFilterTreeNodeRandomTree verifies that random trees are generated with expected size and depth.
func TestFilterTreeNodeRandomTree(t *testing.T) {
	tree := makeRandomTree(3, 8)
	fmt.Printf("TestFilterTreeNodeRandomTree: Generated tree with Size=%d, Depth=%d\n", tree.Size(), tree.Depth())
	assert.Equal(t, 14, tree.Size()) // Expecting a size of 14
	assert.Equal(t, 2, tree.Depth()) // Depth is 2 based on the random tree generation logic
}

// TestFilterTreeNodeValidation checks that tree validation correctly identifies valid and invalid nodes.
func TestFilterTreeNodeValidation(t *testing.T) {
	// Test a valid tree
	validTree := makeRandomTree(2, 4)
	err := validTree.Validate("root")
	fmt.Printf("TestFilterTreeNodeValidation: Valid tree validation returned: %v\n", err)
	require.NoError(t, err)

	// Create an invalid tree with no filter and no children (invalid leaf)
	invalidLeaf := &FilterTreeNode{
		Children: nil, // No children, treated as a leaf
		Filter:   nil, // Invalid filter
	}
	fmt.Println("Testing invalid leaf structure:")
	fmt.Printf("Invalid leaf node: IsLeaf=%v, Filter=%v\n", invalidLeaf.IsLeaf(), invalidLeaf.Filter)
	err = invalidLeaf.Validate("root")
	fmt.Printf("Validation result for invalid leaf: %v\n", err)
	assert.Error(t, err, "Expected validation to fail for invalid leaf")

	// Create an invalid tree with children but invalid structure
	invalidParent := &FilterTreeNode{
		Children: []*FilterTreeNode{
			{Filter: nil}, // Invalid child
		},
		Filter: nil, // Parent filter missing
	}
	fmt.Println("Testing invalid parent structure:")
	for i, child := range invalidParent.Children {
		fmt.Printf("Child %d: IsLeaf=%v, Filter=%v\n", i, child.IsLeaf(), child.Filter)
	}
	err = invalidParent.Validate("root")
	fmt.Printf("Validation result for invalid parent: %v\n", err)
	assert.Error(t, err, "Expected validation to fail for invalid parent")
}

// TestFilterTreeNodeComplexOperations verifies adding AND/OR filters and tree processing behavior.
func TestFilterTreeNodeComplexOperations(t *testing.T) {
	root := makeRandomTree(2, 4)
	fmt.Printf("Initial tree: Size=%d, Depth=%d\n", root.Size(), root.Depth())

	// Test AddAndFilter
	root.AddAndFilter(makeRandomFilter("extra", FilterModeGt, 10))
	fmt.Printf("After AddAndFilter: Size=%d, Depth=%d\n", root.Size(), root.Depth())
	assert.Equal(t, 7, root.Size()) // Expecting size 7

	// Test AddOrFilter
	root.AddOrFilter(makeRandomFilter("optional", FilterModeLt, 5))
	fmt.Printf("After AddOrFilter: Size=%d, Depth=%d\n", root.Size(), root.Depth())
	assert.Equal(t, 8, root.Size()) // Expecting size 8

	// Test IsProcessed on a valid tree
	processed := root.IsProcessed()
	fmt.Printf("Tree processed state: %v\n", processed)
	assert.False(t, processed) // Expecting the tree to not be processed yet
}

// TestFilterIntegrationWithBitmap verifies tree integration with a bitmap object.
func TestFilterIntegrationWithBitmap(t *testing.T) {
	tree := makeRandomTree(2, 4)
	tree.Bits = bitmap.NewFromArray([]uint64{1, 2, 3, 4})
	fmt.Printf("TestFilterIntegrationWithBitmap: Tree bitmap state: Valid=%v\n", tree.Bits.IsValid())
	assert.True(t, tree.Bits.IsValid())
	assert.False(t, tree.IsEmpty())
}
