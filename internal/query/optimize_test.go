// Copyright (c) 2024 Blockwatch Data Inc.
// Author: oliver@blockwatch.cc

package query

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestOptimizeConditions tests the query optimizer's ability to handle complex
// and edge cases. It verifies:
// 1. Condition merging and simplification
// 2. Reordering by selectivity
// 3. Handling of invalid/contradictory conditions
// 4. Type safety and mixed type handling
func TestOptimize(t *testing.T) {
	tests := []struct {
		name     string
		input    *FilterTreeNode
		expected *FilterTreeNode
		comment  string
	}{
		{
			name: "SimpleReorder",
			input: newTestTree(COND_AND,
				newTestRangeNode(1, int64(0), int64(100)),
				newTestNode(FilterModeEqual, 1, int64(50)),
			),
			expected: newTestTree(COND_AND,
				newTestNode(FilterModeEqual, 1, int64(50)),
			),
			comment: "Optimized away the unnecessary range condition",
		},
		{
			name: "MergeInGaps",
			input: newTestTree(COND_OR,
				newTestNode(FilterModeIn, 1, []int64{1, 2, 3}),
				newTestNode(FilterModeEqual, 1, int64(4)),
				newTestNode(FilterModeIn, 1, []int64{5, 6, 7}),
			),
			expected: newTestTree(COND_OR,
				newTestNode(FilterModeIn, 1, []int64{1, 2, 3, 4, 5, 6, 7}),
			),
			comment: "Adjacent IN conditions should be merged with gap-filling equals",
		},
		{
			name: "MergeRanges",
			input: newTestTree(COND_AND,
				newTestNode(FilterModeGt, 1, int64(10)),
				newTestNode(FilterModeLt, 1, int64(90)),
				newTestNode(FilterModeGe, 1, int64(20)),
				newTestNode(FilterModeLe, 1, int64(80)),
				newTestRangeNode(1, int64(30), int64(70)),
			),
			expected: newTestTree(COND_AND,
				newTestRangeNode(1, int64(30), int64(70)),
			),
			comment: "Multiple overlapping ranges should be merged into most restrictive form",
		},
		{
			name: "RangeOrOverlap",
			input: newTestTree(COND_OR,
				newTestRangeNode(1, int64(0), int64(15)),
				newTestRangeNode(1, int64(10), int64(30)),
			),
			expected: newTestTree(COND_OR,
				newTestRangeNode(1, int64(0), int64(30)),
			),
			comment: "Non-overlapping ranges in OR should not be merged",
		},
		{
			name: "RangeOrNoOverlap",
			input: newTestTree(COND_OR,
				newTestRangeNode(1, int64(0), int64(10)),
				newTestRangeNode(1, int64(20), int64(30)),
			),
			expected: newTestTree(COND_OR,
				newTestRangeNode(1, int64(0), int64(10)),
				newTestRangeNode(1, int64(20), int64(30)),
			),
			comment: "Non-overlapping ranges in OR should not be merged",
		},
		{
			name: "TypeBoundsGtLt",
			input: newTestTree(COND_AND,
				newTestNode(FilterModeGt, 1, int64(0)),
				newTestNode(FilterModeLt, 1, int64(100)),
			),
			expected: newTestTree(COND_AND,
				newTestRangeNode(1, int64(1), int64(99)),
			),
			comment: "Boundary conditions should be handled correctly",
		},
		{
			name: "TypeBoundsGeLe",
			input: newTestTree(COND_AND,
				newTestNode(FilterModeGe, 1, int64(0)),
				newTestNode(FilterModeLe, 1, int64(100)),
			),
			expected: newTestTree(COND_AND,
				newTestRangeNode(1, int64(0), int64(100)),
			),
			comment: "Boundary conditions should be handled correctly",
		},
		{
			name: "RangeNotEqual",
			input: newTestTree(COND_AND,
				newTestRangeNode(1, int64(0), int64(100)),
				newTestNode(FilterModeNotEqual, 1, int64(50)),
			),
			expected: newTestTree(COND_AND,
				newTestRangeNode(1, int64(0), int64(100)),
				newTestNode(FilterModeNotEqual, 1, int64(50)),
			),
			comment: "NOT conditions should not affect range merging",
		},
		{
			name: "EqualAndGt",
			input: newTestTree(COND_AND,
				newTestNode(FilterModeEqual, 1, int64(42)),
				newTestNode(FilterModeGt, 1, int64(41)),
			),
			expected: newTestTree(COND_AND,
				newTestNode(FilterModeEqual, 1, int64(42)),
			),
			comment: "EQ and GT should be simplified",
		},
		{
			name: "RegexpRange",
			input: newTestTree(COND_AND,
				newTestRangeNode(1, "a", "z"),
				newTestNode(FilterModeRegexp, 1, "^[a-m]+$"),
			),
			expected: newTestTree(COND_AND,
				newTestRangeNode(1, "a", "z"),
				newTestNode(FilterModeRegexp, 1, "^[a-m]+$"),
			),
			comment: "Regexp conditions should not be merged with ranges",
		},
		// TODO: always true condition, needs to be supported
		{
			name: "TautologyOne",
			input: newTestTree(COND_OR,
				newTestRangeNode(1, int64(0), int64(100)),
				newTestNode(FilterModeNotEqual, 1, int64(50)),
				newTestRangeNode(1, int64(40), int64(60)),
			),
			expected: newTestTree(COND_OR),
			// newTestRangeNode(1, int64(0), int64(49)),
			// newTestRangeNode(1, int64(51), int64(100)),
			// newTestRangeNode(1, int64(40), int64(49)),
			// newTestRangeNode(1, int64(51), int64(60)),

			comment: "Range splits should handle multiple overlapping conditions",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.NotPanics(t, tt.input.Optimize)
			assert.Equal(t, tt.expected, tt.input, tt.comment)
		})
	}
}

// typeFromValue maps Go types to BlockType constants for testing.
// This is used to create Filter nodes with the correct type information.
// Note: defaults to BlockTime for unknown types to test type handling edge cases.
func typeFromValue(v interface{}) BlockType {
	if v == nil {
		return BlockTime
	}
	switch v.(type) {
	case int64:
		return BlockInt64
	case int32:
		return BlockInt32
	case int16:
		return BlockInt16
	case int8:
		return BlockInt8
	case uint64:
		return BlockUint64
	case uint32:
		return BlockUint32
	case uint16:
		return BlockUint16
	case uint8:
		return BlockUint8
	case float64:
		return BlockFloat64
	case float32:
		return BlockFloat32
	case bool:
		return BlockBool
	case string:
		return BlockString
	case []byte:
		return BlockBytes
	default:
		return BlockTime
	}
}

// newTestNode creates a FilterTreeNode with a single condition
func newTestNode(mode FilterMode, fieldIndex uint16, value interface{}) *FilterTreeNode {
	f := &Filter{
		Mode:  mode,
		Index: fieldIndex,
		Type:  typeFromValue(value),
		Value: value,
	}

	// Special handling for nil values
	if value == nil {
		f.Type = BlockTime // default type for nil values
		f.Matcher = newFactory(f.Type).New(mode)
		return &FilterTreeNode{Filter: f}
	}

	f.Matcher = newFactory(f.Type).New(mode)

	// Handle different modes appropriately
	switch mode {
	case FilterModeRegexp:
		if s, ok := value.(string); ok {
			f.Value = s
		}
		f.Matcher.WithValue(f.Value)
	case FilterModeIn, FilterModeNotIn:
		f.Matcher.WithSlice(value)
	default:
		f.Matcher.WithValue(value)
	}

	return &FilterTreeNode{Filter: f}
}

// newTestRangeNode creates a FilterTreeNode with a range condition
func newTestRangeNode(fieldIndex uint16, from, to interface{}) *FilterTreeNode {
	// Handle nil range bounds
	if from == nil && to == nil {
		return newTestNode(FilterModeRange, fieldIndex, nil)
	}

	// Handle time values
	switch v := from.(type) {
	case string:
		from = []byte(v)
	case time.Time:
		from = v.UnixNano()
	}

	switch v := to.(type) {
	case string:
		to = []byte(v)
	case time.Time:
		to = v.UnixNano()
	}

	// Determine type from non-nil value
	var blockType BlockType
	if from != nil {
		blockType = typeFromValue(from)
	} else {
		blockType = typeFromValue(to)
	}

	val := RangeValue{from, to}

	f := &Filter{
		Mode:  FilterModeRange,
		Index: fieldIndex,
		Type:  blockType,
		Value: val,
	}
	f.Matcher = newFactory(f.Type).New(FilterModeRange)
	f.Matcher.WithValue(val)
	return &FilterTreeNode{Filter: f}
}

// newTestTree creates a FilterTreeNode with the specified children
func newTestTree(orKind bool, children ...*FilterTreeNode) *FilterTreeNode {
	if len(children) == 0 {
		return &FilterTreeNode{}
	}
	return &FilterTreeNode{
		OrKind:   orKind,
		Children: children,
	}
}
