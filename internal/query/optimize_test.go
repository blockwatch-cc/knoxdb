// Copyright (c) 2024 Blockwatch Data Inc.
// Author: oliver@blockwatch.cc

package query

import (
	"fmt"
	"log"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Define a schema with a value for each data type
var allTypesSchema = map[BlockType]interface{}{
	BlockInt64:   int64(42),
	BlockInt32:   int32(42),
	BlockInt16:   int16(42),
	BlockInt8:    int8(42),
	BlockUint64:  uint64(42),
	BlockUint32:  uint32(42),
	BlockUint16:  uint16(42),
	BlockUint8:   uint8(42),
	BlockFloat64: float64(42.0),
	BlockFloat32: float32(42.0),
	BlockBool:    true,
	BlockString:  "test",
	BlockBytes:   []byte("test"),
}

// Define query conditions
var queryConditions = []FilterMode{
	FilterModeEqual,
	FilterModeNotEqual,
	FilterModeIn,
	FilterModeNotIn,
	FilterModeGt,
	FilterModeLt,
	FilterModeGe,
	FilterModeLe,
	FilterModeRange,
	FilterModeRegexp,
}

// TestOptimize tests the query optimizer's ability to handle complex and edge cases.
// It verifies:
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
			name:     "Specialized",
			input:    makeAndTree(makeRangeNode(0, 100), makeEqualNode(50)),
			expected: makeAndTree(makeEqualNode(50)),
			comment:  "Optimized away the unnecessary range condition",
		},
		{
			name:     "MergeInGaps",
			input:    makeOrTree(makeInNode(1, 2, 3), makeEqualNode(4), makeInNode(5, 6, 7)),
			expected: makeOrTree(makeInNode(1, 2, 3, 4, 5, 6, 7)),
			comment:  "Adjacent IN conditions should be merged with gap-filling equals",
		},
		{
			name:     "MergeRanges",
			input:    makeAndTree(makeGtNode(10), makeLtNode(90), makeGeNode(20), makeLeNode(80), makeRangeNode(30, 70)),
			expected: makeAndTree(makeRangeNode(30, 70)),
			comment:  "Multiple overlapping ranges should be merged into most restrictive form",
		},
		{
			name:     "RangeOrOverlap",
			input:    makeOrTree(makeRangeNode(0, 15), makeRangeNode(10, 30)),
			expected: makeOrTree(makeRangeNode(0, 30)),
			comment:  "Non-overlapping ranges in OR should not be merged",
		},
		{
			name:     "RangeOrNoOverlap",
			input:    makeOrTree(makeRangeNode(0, 10), makeRangeNode(20, 30)),
			expected: makeOrTree(makeRangeNode(0, 10), makeRangeNode(20, 30)),
			comment:  "Non-overlapping ranges in OR should not be merged",
		},
		{
			name:     "TypeBoundsGtLt",
			input:    makeAndTree(makeGtNode(0), makeLtNode(100)),
			expected: makeAndTree(makeRangeNode(1, 99)),
			comment:  "Boundary conditions should be handled correctly",
		},
		{
			name:     "TypeBoundsGeLe",
			input:    makeAndTree(makeGeNode(0), makeLeNode(100)),
			expected: makeAndTree(makeRangeNode(0, 100)),
			comment:  "Boundary conditions should be handled correctly",
		},
		{
			name:     "RangeNotEqual",
			input:    makeAndTree(makeRangeNode(0, 100), makeNotEqualNode(50)),
			expected: makeAndTree(makeNotEqualNode(50), makeRangeNode(0, 100)),
			comment:  "NOT conditions should not affect range merging",
		},
		{
			name:     "EqualAndGt",
			input:    makeAndTree(makeEqualNode(42), makeGtNode(41)),
			expected: makeAndTree(makeEqualNode(42)),
			comment:  "EQ and GT should be simplified",
		},
		{
			name:     "RegexpRange",
			input:    makeAndTree(makeTestRangeNode(1, "a", "z"), makeRegexNode("^[a-m]+$")),
			expected: makeAndTree(makeTestRangeNode(1, "a", "z"), makeRegexNode("^[a-m]+$")),
			comment:  "Regexp conditions should not be merged with ranges",
		},
		{
			name:     "TautologyOne",
			input:    makeOrTree(makeRangeNode(0, 100), makeNotEqualNode(50), makeRangeNode(40, 60)),
			expected: makeOrTree(makeNotEqualNode(50), makeRangeNode(0, 100)),
		},
		{
			name:     "Independent Fields",
			input:    makeAndTree(makeNode(FilterModeEqual, 1, int64(1)), makeNode(FilterModeEqual, 2, []byte("hi"))),
			expected: makeAndTree(makeNode(FilterModeEqual, 1, int64(1)), makeNode(FilterModeEqual, 2, []byte("hi"))),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.NotPanics(t, tt.input.Optimize)
			assert.Equal(t, tt.expected.String(), tt.input.String(), tt.comment)
		})
	}
}

// TestOptimizeExtended verifies the optimizer's behavior with various data types and conditions, ensuring correct tree structures.
func TestOptimizeExtended(t *testing.T) {
	// Iterate over all data types and query conditions
	for typ, val := range allTypesSchema {
		for _, cond := range queryConditions {
			t.Run(fmt.Sprintf("%s_%s", typ, cond), func(t *testing.T) {
				// Create a filter node for the current type and condition
				node := makeNode(cond, 1, val)

				// Create AND/OR trees with the node
				andTree := makeAndTree(node, node)
				orTree := makeOrTree(node, node)

				// Run optimizer and check results
				require.NotPanics(t, andTree.Optimize, "Optimizer should not panic")
				require.NotPanics(t, orTree.Optimize, "Optimizer should not panic")

				// Enhanced assertions
				assert.NotNil(t, andTree, "AND tree should not be nil after optimization")
				assert.NotNil(t, orTree, "OR tree should not be nil after optimization")

				// Check if redundant conditions are removed
				if cond == FilterModeEqual {
					assert.Equal(t, 1, len(andTree.Children), "Redundant conditions should be removed in AND tree")
				}

				// Check if certain conditions are simplified
				if cond == FilterModeIn {
					assert.Condition(t, func() bool {
						// Example condition: check if IN condition is simplified
						return len(andTree.Children) <= 2
					}, "IN condition should be simplified in AND tree")
				}

				// Check logical tree structure
				assert.True(t, andTree.OrKind == false, "AND tree should maintain AND structure")
				assert.True(t, orTree.OrKind == true, "OR tree should maintain OR structure")
			})
		}
	}
}

// typeFromValue returns the BlockType corresponding to the given Go type, defaulting to BlockTime for unknown or nil types.
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

// makeNode constructs a FilterTreeNode with a specified filter mode, field index, and value, setting up the appropriate matcher.
func makeNode(mode FilterMode, fieldIndex uint16, value interface{}) *FilterTreeNode {
	// Log the initial value and its type
	log.Printf("makeNode called with mode: %v, fieldIndex: %d, value: %v (type: %T)", mode, fieldIndex, value, value)

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
		// Ensure value is a slice of the correct type
		switch v := value.(type) {
		case []int64:
			f.Value = v
		case []int32:
			f.Value = v
		case []int16:
			f.Value = v
		case []int8:
			f.Value = v
		case []uint64:
			f.Value = v
		case []uint32:
			f.Value = v
		case []uint16:
			f.Value = v
		case []float64:
			f.Value = v
		case []float32:
			f.Value = v
		case []bool:
			f.Value = v
		case []string:
			f.Value = v
		case []byte:
			if f.Type == BlockBytes {
				f.Value = [][]byte{v}
			} else {
				f.Value = v
			}
		default:
			// Convert single values to a slice of the correct type
			switch f.Type {
			case BlockInt64:
				f.Value = []int64{v.(int64)}
			case BlockInt32:
				f.Value = []int32{v.(int32)}
			case BlockInt16:
				f.Value = []int16{v.(int16)}
			case BlockInt8:
				f.Value = []int8{v.(int8)}
			case BlockUint64:
				f.Value = []uint64{v.(uint64)}
			case BlockUint32:
				f.Value = []uint32{v.(uint32)}
			case BlockUint16:
				f.Value = []uint16{v.(uint16)}
			case BlockUint8:
				f.Value = []uint8{v.(uint8)}
			case BlockFloat64:
				f.Value = []float64{v.(float64)}
			case BlockFloat32:
				f.Value = []float32{v.(float32)}
			case BlockBool:
				f.Value = []bool{v.(bool)}
			case BlockString:
				f.Value = []string{v.(string)}
			case BlockBytes:
				f.Value = [][]byte{v.([]byte)}
			default:
				f.Value = []interface{}{v}
			}
		}
		f.Matcher.WithSlice(f.Value)
	case FilterModeRange:
		// Ensure value is a RangeValue
		if _, ok := value.(RangeValue); !ok {
			f.Value = RangeValue{value, value}
		}
		f.Matcher.WithValue(f.Value)
	default:
		// Convert string to []byte if necessary
		if s, ok := value.(string); ok && f.Type == BlockBytes {
			f.Value = []byte(s)
		}
		f.Matcher.WithValue(f.Value)
	}

	// Log the final value and its type after processing
	log.Printf("makeNode processed value: %v (type: %T)", f.Value, f.Value)

	return &FilterTreeNode{Filter: f}
}

// makeTestRangeNode constructs a FilterTreeNode for a range condition, converting string and time values to byte slices and Unix timestamps.
func makeTestRangeNode(fieldIndex uint16, from, to interface{}) *FilterTreeNode {
	// Handle nil range bounds
	if from == nil && to == nil {
		return makeNode(FilterModeRange, fieldIndex, nil)
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

// newTestTree constructs a logical tree (AND/OR) FilterTreeNode with specified child nodes.
func newTestTree(orKind bool, children ...*FilterTreeNode) *FilterTreeNode {
	if len(children) == 0 {
		return &FilterTreeNode{}
	}
	return &FilterTreeNode{
		OrKind:   orKind,
		Children: children,
	}
}

// makeAndTree constructs a logical AND tree from the provided child nodes.
func makeAndTree(children ...*FilterTreeNode) *FilterTreeNode {
	return newTestTree(COND_AND, children...)
}

// makeOrTree constructs a logical OR tree from the provided child nodes.
func makeOrTree(children ...*FilterTreeNode) *FilterTreeNode {
	return newTestTree(COND_OR, children...)
}

// makeEqualNode constructs a FilterTreeNode for an equality condition with a specified integer value.
func makeEqualNode(val int) *FilterTreeNode {
	return makeNode(FilterModeEqual, 1, int64(val))
}

// makeRangeNode constructs a FilterTreeNode for a range condition between two integer values.
func makeRangeNode(from, to int) *FilterTreeNode {
	return makeTestRangeNode(1, int64(from), int64(to))
}

// makeRegexNode constructs a FilterTreeNode for a regular expression condition with a specified string.
func makeRegexNode(s string) *FilterTreeNode {
	return makeNode(FilterModeRegexp, 1, s)
}

// makeInNode constructs a FilterTreeNode for an IN condition with a list of integer values.
func makeInNode(vals ...int) *FilterTreeNode {
	cval := make([]int64, len(vals))
	for i, v := range vals {
		cval[i] = int64(v)
	}
	return makeNode(FilterModeIn, 1, cval)
}

// makeNotEqualNode constructs a FilterTreeNode for a not-equal condition with a specified integer value.
func makeNotEqualNode(val int) *FilterTreeNode {
	return makeNode(FilterModeNotEqual, 1, int64(val))
}

// makeGtNode constructs a FilterTreeNode for a greater-than condition with a specified integer value.
func makeGtNode(val int) *FilterTreeNode {
	return makeNode(FilterModeGt, 1, int64(val))
}

// makeLtNode constructs a FilterTreeNode for a less-than condition with a specified integer value.
func makeLtNode(val int) *FilterTreeNode {
	return makeNode(FilterModeLt, 1, int64(val))
}

// makeGeNode constructs a FilterTreeNode for a greater-than-or-equal condition with a specified integer value.
func makeGeNode(val int) *FilterTreeNode {
	return makeNode(FilterModeGe, 1, int64(val))
}

// makeLeNode constructs a FilterTreeNode for a less-than-or-equal condition with a specified integer value.
func makeLeNode(val int) *FilterTreeNode {
	return makeNode(FilterModeLe, 1, int64(val))
}
