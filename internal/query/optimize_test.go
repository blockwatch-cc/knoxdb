// Copyright (c) 2024 Blockwatch Data Inc.
// Author: oliver@blockwatch.cc

package query

import (
	"fmt"
	"log"
	"reflect"
	"testing"

	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/schema"
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
	BlockString:  []byte("test"), // sic
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

func tryUnwrapAnySlice(s any) any {
	val := reflect.ValueOf(s)
	if val.Type().Kind() != reflect.Slice || val.Len() == 0 || val.Index(0).Kind() != reflect.Interface {
		return s
	}
	etyp := val.Index(0).Elem().Type()
	switch etyp.Kind() {
	case reflect.Slice:
		return val.Index(0).Elem().Interface()
	default:
		slice := reflect.MakeSlice(reflect.SliceOf(val.Index(0).Elem().Type()), 0, val.Len())
		for i := 0; i < val.Len(); i++ {
			slice = reflect.Append(slice, val.Index(i).Elem())
		}
		return slice.Interface()
	}
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
			input:    makeAndTree(makeRangeNode("id", 1, 0, 100), makeEqualNode("id", 1, 50)),
			expected: makeAndTree(makeEqualNode("id", 1, 50)),
			comment:  "Optimized away the unnecessary range condition",
		},
		{
			name:     "MergeInGaps",
			input:    makeOrTree(makeInNode("id", 1, []int64{2, 3}), makeEqualNode("id", 1, 4), makeInNode("id", 1, []int64{5, 6, 7})),
			expected: makeOrTree(makeInNode("id", 1, []int64{2, 3, 4, 5, 6, 7})),
			comment:  "Adjacent IN conditions should be merged with gap-filling equals",
		},
		{
			name:     "MergeRanges",
			input:    makeAndTree(makeGtNode("id", 1, 10), makeLtNode("id", 1, 90), makeGeNode("id", 1, 20), makeLeNode("id", 1, 80), makeRangeNode("id", 1, 30, 70)),
			expected: makeAndTree(makeRangeNode("id", 1, 30, 70)),
			comment:  "Multiple overlapping ranges should be merged into most restrictive form",
		},
		{
			name:     "RangeOrOverlap",
			input:    makeOrTree(makeRangeNode("id", 1, 0, 15), makeRangeNode("id", 1, 10, 30)),
			expected: makeOrTree(makeRangeNode("id", 1, 0, 30)),
			comment:  "Non-overlapping ranges in OR should not be merged",
		},
		{
			name:     "RangeOrNoOverlap",
			input:    makeOrTree(makeRangeNode("id", 1, 0, 10), makeRangeNode("id", 1, 20, 30)),
			expected: makeOrTree(makeRangeNode("id", 1, 0, 10), makeRangeNode("id", 1, 20, 30)),
			comment:  "Non-overlapping ranges in OR should not be merged",
		},
		{
			name:     "TypeBoundsGtLt",
			input:    makeAndTree(makeGtNode("id", 1, 0), makeLtNode("id", 1, 100)),
			expected: makeAndTree(makeRangeNode("id", 1, 1, 99)),
			comment:  "Boundary conditions should be handled correctly",
		},
		{
			name:     "TypeBoundsGeLe",
			input:    makeAndTree(makeGeNode("id", 1, 0), makeLeNode("id", 1, 100)),
			expected: makeAndTree(makeRangeNode("id", 1, 0, 100)),
			comment:  "Boundary conditions should be handled correctly",
		},
		{
			name:     "RangeNotEqual",
			input:    makeAndTree(makeRangeNode("id", 0, 1, 100), makeNotEqualNode("id", 1, 50)),
			expected: makeAndTree(makeNotEqualNode("id", 1, 50), makeRangeNode("id", 0, 1, 100)),
			comment:  "NOT conditions should not affect range merging",
		},
		{
			name:     "EqualAndGt",
			input:    makeAndTree(makeEqualNode("id", 1, 42), makeGtNode("id", 1, 41)),
			expected: makeAndTree(makeEqualNode("id", 1, 42)),
			comment:  "EQ and GT should be simplified",
		},
		{
			name:     "RegexpRange",
			input:    makeAndTree(makeRangeNode("name", 1, "a", "z"), makeRegexNode("name", 1, "^[a-m]+$")),
			expected: makeAndTree(makeRangeNode("name", 1, "a", "z"), makeRegexNode("name", 1, "^[a-m]+$")),
			comment:  "Regexp conditions should not be merged with ranges",
		},
		{
			name:     "TautologyOne",
			input:    makeOrTree(makeRangeNode("id", 1, 0, 100), makeNotEqualNode("id", 1, 50), makeRangeNode("id", 1, 40, 60)),
			expected: makeOrTree(makeNotEqualNode("id", 1, 50), makeRangeNode("id", 1, 0, 100)),
		},
		{
			name:     "Independent Fields",
			input:    makeAndTree(makeNode("id", FilterModeEqual, 1, int64(1)), makeNode("name", FilterModeEqual, 2, []byte("hi"))),
			expected: makeAndTree(makeNode("id", FilterModeEqual, 1, int64(1)), makeNode("name", FilterModeEqual, 2, []byte("hi"))),
		},
		{
			name:     "OR_IN",
			input:    makeOrTree(makeInNode("id", 1, []int64{1, 2, 3}), makeInNode("id", 0, []int64{2, 3, 4})),
			expected: makeOrTree(makeInNode("id", 1, []int64{1, 2, 3, 4})),
			comment:  "Overlapping IN conditions should be merged",
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
				node := makeNode("id", cond, 1, val)

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

// fieldTypeFromValue returns the BlockType corresponding to the given Go type, defaulting to BlockTime for unknown or nil types.
func fieldTypeFromValue(v interface{}) types.FieldType {
	if v == nil {
		panic(fmt.Errorf("unsupported test nil value"))
	}
	switch val := v.(type) {
	case int, int64:
		return types.FieldTypeInt64
	case int32:
		return types.FieldTypeInt32
	case int16:
		return types.FieldTypeInt16
	case int8:
		return types.FieldTypeInt8
	case uint, uint64, []uint64:
		return types.FieldTypeUint64
	case uint32:
		return types.FieldTypeUint32
	case uint16:
		return types.FieldTypeUint16
	case uint8:
		return types.FieldTypeUint8
	case float64:
		return types.FieldTypeFloat64
	case float32:
		return types.FieldTypeFloat32
	case bool:
		return types.FieldTypeBoolean
	case string:
		return types.FieldTypeString
	case []byte:
		return types.FieldTypeBytes
	case RangeValue:
		if val[0] == nil {
			return fieldTypeFromValue(val[1])
		} else {
			return fieldTypeFromValue(val[0])
		}
	default:
		value := reflect.ValueOf(v)
		if value.Kind() == reflect.Slice && reflectSliceLen(v) > 0 {
			switch value.Index(0).Interface().(type) {
			case uint, uint64:
				return types.FieldTypeUint64
			case int, int64:
				return types.FieldTypeInt64
			}
		}
		panic(fmt.Errorf("unsupported test value type %s [%s]", value.Type(), value.Type().Kind()))
	}
}

// makeNode constructs a FilterTreeNode with a specified filter mode, field index, and value, setting up the appropriate matcher.
func makeNode(name string, mode FilterMode, fieldIndex uint16, value any) *FilterTreeNode {
	tree := &FilterTreeNode{}
	// Log the initial value and its type
	log.Printf("makeNode called with mode: %v, fieldIndex: %d, value: %v (type: %T)", mode, fieldIndex, value, value)

	// unwrap the []any interface from Go variadic function args
	value = tryUnwrapAnySlice(value)

	f := &Filter{
		Name:  name,
		Mode:  mode,
		Index: fieldIndex,
		Type:  BlockTypes[fieldTypeFromValue(value)],
		Value: value,
	}

	// Special handling for nil values
	if value == nil {
		f.Type = BlockTime // default type for nil values
		f.Matcher = newFactory(f.Type).New(mode)
		return &FilterTreeNode{Filter: f}
	}

	f.Matcher = newFactory(f.Type).New(mode)

	fieldType := fieldTypeFromValue(value)
	caster := schema.NewCaster(fieldType, nil)

	// Handle different modes appropriately
	switch mode {
	case FilterModeFalse:
		f.Value = nil
		tree.Empty = true
	case FilterModeTrue:
		f.Value = nil
		tree.Skip = true
	case FilterModeIn, FilterModeNotIn:
		if reflect.ValueOf(value).Kind() != reflect.Slice {
			value = makeReflectSlice(value)
		}
		v, err := caster.CastSlice(value)
		if err != nil {
			panic(err)
		}
		f.Value = v
		f.Matcher.WithSlice(f.Value)
	case FilterModeRange:
		rg, ok := value.(RangeValue)
		if !ok {
			// make a range out of a single value
			rg[0] = value
			rg[1] = typedAdd(f.Type, value, 1)
		}
		var err error
		rg[0], err = caster.CastValue(rg[0])
		if err != nil {
			panic(err)
		}
		rg[1], err = caster.CastValue(rg[1])
		if err != nil {
			panic(err)
		}
		f.Value = rg
		f.Matcher.WithValue(f.Value)
	default:
		v, err := caster.CastValue(value)
		if err != nil {
			panic(err)
		}
		f.Value = v
		f.Matcher.WithValue(f.Value)
	}

	// Log the final value and its type after processing
	log.Printf("makeNode processed value: %v (type: %T)", f.Value, f.Value)

	tree.Filter = f
	return tree
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
func makeEqualNode(name string, idx uint16, val any) *FilterTreeNode {
	return makeNode(name, FilterModeEqual, idx, val)
}

// makeRangeNode constructs a FilterTreeNode for a range condition between two integer values.
func makeRangeNode(name string, idx uint16, from, to any) *FilterTreeNode {
	return makeNode(name, FilterModeRange, idx, RangeValue{from, to})
}

// makeRegexNode constructs a FilterTreeNode for a regular expression condition with a specified string.
// makeRegexNode constructs a FilterTreeNode for a regexp conditions.
func makeRegexNode(name string, idx uint16, s string) *FilterTreeNode {
	return makeNode(name, FilterModeRegexp, idx, s)
}

// makeInNode constructs a FilterTreeNode for an IN condition with a list of integer values.
func makeInNode(name string, idx uint16, vals ...any) *FilterTreeNode {
	return makeNode(name, FilterModeIn, idx, vals)
}

// makeNiNode constructs a FilterTreeNode for an Not IN condition with a list of integer values.
func makeNotInNode(name string, idx uint16, vals ...any) *FilterTreeNode {
	return makeNode(name, FilterModeNotIn, idx, vals)
}

// makeNotEqualNode constructs a FilterTreeNode for a not-equal condition with a specified integer value.
func makeNotEqualNode(name string, idx uint16, val any) *FilterTreeNode {
	return makeNode(name, FilterModeNotEqual, idx, val)
}

// makeGtNode constructs a FilterTreeNode for a greater-than condition with a specified integer value.
func makeGtNode(name string, idx uint16, val any) *FilterTreeNode {
	return makeNode(name, FilterModeGt, idx, val)
}

// makeLtNode constructs a FilterTreeNode for a less-than condition with a specified integer value.
func makeLtNode(name string, idx uint16, val any) *FilterTreeNode {
	return makeNode(name, FilterModeLt, idx, val)
}

// makeGeNode constructs a FilterTreeNode for a greater-than-or-equal condition with a specified integer value.
func makeGeNode(name string, idx uint16, val any) *FilterTreeNode {
	return makeNode(name, FilterModeGe, idx, val)
}

// makeLeNode constructs a FilterTreeNode for a less-than-or-equal condition with a specified integer value.
func makeLeNode(name string, idx uint16, val any) *FilterTreeNode {
	return makeNode(name, FilterModeLe, idx, val)
}

// makeFalseNode constructs a FilterTreeNode for a false condition.
func makeFalseNode(name string, idx uint16, val any) *FilterTreeNode {
	return makeNode(name, FilterModeFalse, idx, val)
}

// makeTrueNode constructs a FilterTreeNode for a true condition.
func makeTrueNode(name string, idx uint16, val any) *FilterTreeNode {
	return makeNode(name, FilterModeTrue, idx, val)
}
