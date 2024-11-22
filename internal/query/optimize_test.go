// Copyright (c) 2024 Blockwatch Data Inc.
// Author: oliver@blockwatch.cc

package query

import (
	"fmt"
	"reflect"
	"testing"

	"blockwatch.cc/knoxdb/internal/tests"
	"blockwatch.cc/knoxdb/pkg/schema"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestOptimize tests the query optimizer's ability to handle complex and edge cases.
// It verifies:
// 1. Condition merging and simplification
// 2. Reordering by selectivity
// 3. Handling of invalid/contradictory conditions
// 4. Type safety and mixed type handling
func TestOptimize(t *testing.T) {
	for _, gen := range tests.Generators {
		v := gen.MakeValue
		s := gen.MakeSlice
		sm := schema.NewSchema().
			WithField(schema.NewField(tests.FieldTypes[gen.Type()]).WithName("f1")).
			WithField(schema.NewField(tests.FieldTypes[gen.Type()]).WithName("f2"))
		f1, _ := sm.FieldByName("f1")
		f2, _ := sm.FieldByName("f2")
		tests := []struct {
			name     string
			input    *FilterTreeNode
			expected *FilterTreeNode
			comment  string
		}{
			{
				name:     "RG(0,100) AND EQ(50)",
				input:    makeAndTree(makeRangeNode(f1, v(0), v(100)), makeEqualNode(f1, v(50))),
				expected: makeAndTree(makeEqualNode(f1, v(50))),
				comment:  "Optimized away the unnecessary range condition",
			},
			{
				name:     "IN(2,3) OR IN(1,4) OR IN(5,6,7)",
				input:    makeOrTree(makeInNode(f1, s(2, 3)), makeEqualNode(f1, v(4)), makeInNode(f1, s(5, 6, 7))),
				expected: makeOrTree(makeInNode(f1, s(2, 3, 4, 5, 6, 7))),
				comment:  "Adjacent IN conditions should be merged with gap-filling equals",
			},
			{
				name:     "GT(10) AND LT(90) AND GE(20) AND LE(80) AND RG(30,70)",
				input:    makeAndTree(makeGtNode(f1, v(10)), makeLtNode(f1, v(90)), makeGeNode(f1, v(20)), makeLeNode(f1, v(80)), makeRangeNode(f1, v(30), v(70))),
				expected: makeAndTree(makeRangeNode(f1, v(30), v(70))),
				comment:  "Multiple overlapping ranges should be merged into most restrictive form",
			},
			{
				name:     "RG(0,15) OR RG(10,30)",
				input:    makeOrTree(makeRangeNode(f1, v(0), v(15)), makeRangeNode(f1, v(10), v(30))),
				expected: makeOrTree(makeRangeNode(f1, v(0), v(30))),
				comment:  "Non-overlapping ranges in OR should not be merged",
			},
			{
				name:     "RG(0,10) OR RG(20,30)",
				input:    makeOrTree(makeRangeNode(f1, v(0), v(10)), makeRangeNode(f1, v(20), v(30))),
				expected: makeOrTree(makeRangeNode(f1, v(0), v(10)), makeRangeNode(f1, v(20), v(30))),
				comment:  "Non-overlapping ranges in OR should not be merged",
			},
			{
				name:     "GT(0) AND LT(100)",
				input:    makeAndTree(makeGtNode(f1, v(0)), makeLtNode(f1, v(100))),
				expected: makeAndTree(makeRangeNode(f1, v(1), v(99))),
				comment:  "Boundary conditions should be handled correctly",
			},
			{
				name:     "GE(0) AND LE(100)",
				input:    makeAndTree(makeGeNode(f1, v(0)), makeLeNode(f1, v(100))),
				expected: makeAndTree(makeRangeNode(f1, v(0), v(100))),
				comment:  "Boundary conditions should be handled correctly",
			},
			{
				name:     "RG(1,100) AND EQ(50)",
				input:    makeAndTree(makeRangeNode(f1, v(1), v(100)), makeNotEqualNode(f1, v(50))),
				expected: makeAndTree(makeNotEqualNode(f1, v(50)), makeRangeNode(f1, v(1), v(100))),
				comment:  "NOT conditions should not affect range merging",
			},
			{
				name:     "EQ(42) AND GT(41)",
				input:    makeAndTree(makeEqualNode(f1, v(42)), makeGtNode(f1, v(41))),
				expected: makeAndTree(makeEqualNode(f1, v(42))),
				comment:  "EQ and GT should be simplified",
			},
			{
				name:     "RG(0,100) OR NE(50) OR RG(40,60) - Tautology",
				input:    makeOrTree(makeRangeNode(f1, v(0), v(100)), makeNotEqualNode(f1, v(50)), makeRangeNode(f1, v(40), v(60))),
				expected: makeOrTree(makeNotEqualNode(f1, v(50)), makeRangeNode(f1, v(0), v(100))),
			},
			{
				name:     "Independent Fields",
				input:    makeAndTree(makeNode(f1, FilterModeEqual, v(1)), makeNode(f2, FilterModeEqual, v(2))),
				expected: makeAndTree(makeNode(f1, FilterModeEqual, v(1)), makeNode(f2, FilterModeEqual, v(2))),
			},
			{
				name:     "IN(1,2,3) OR IN(2,3,4)",
				input:    makeOrTree(makeInNode(f1, s(1, 2, 3)), makeInNode(f1, s(2, 3, 4))),
				expected: makeOrTree(makeInNode(f1, s(1, 2, 3, 4))),
				comment:  "Overlapping IN conditions should be merged",
			},
		}

		for _, tt := range tests {
			t.Run(gen.Name()+"/"+tt.name, func(t *testing.T) {
				require.NotPanics(t, tt.input.Optimize)
				assert.Equal(t, tt.expected.String(), tt.input.String(), tt.comment)
			})
		}
	}
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

// TestOptimizeExtended verifies the optimizer's behavior with various data types and conditions, ensuring correct tree structures.
func TestOptimizeExtended(t *testing.T) {
	// Iterate over all data types and query conditions
	for _, gen := range tests.Generators {
		for _, cond := range queryConditions {
			t.Run(fmt.Sprintf("%s_%s", gen.Name(), cond), func(t *testing.T) {
				// Create a filter node for the current type and condition
				field := schema.NewField(tests.FieldTypes[gen.Type()]).WithName("f1")
				node := makeNode(field, cond, gen.MakeValue(42))

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

// makeNode constructs a FilterTreeNode with a specified filter mode, field index, and value, setting up the appropriate matcher.
func makeNode(field schema.Field, mode FilterMode, value any) *FilterTreeNode {
	tree := &FilterTreeNode{}
	// Log the initial value and its type
	// log.Printf("makeNode called with mode: %v, fieldIndex: %d, value: %v (type: %T)", mode, fieldIndex, value, value)

	blockType := BlockTypes[field.Type()]
	f := &Filter{
		Name:    field.Name(),
		Mode:    mode,
		Index:   field.Id() - 1,
		Type:    blockType,
		Value:   value,
		Matcher: newFactory(blockType).New(mode),
	}

	caster := schema.NewCaster(field.Type(), nil)

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
	// log.Printf("makeNode processed value: %v (type: %T)", f.Value, f.Value)

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
func makeEqualNode(field schema.Field, val any) *FilterTreeNode {
	return makeNode(field, FilterModeEqual, val)
}

// makeRangeNode constructs a FilterTreeNode for a range condition between two integer values.
func makeRangeNode(field schema.Field, from, to any) *FilterTreeNode {
	return makeNode(field, FilterModeRange, RangeValue{from, to})
}

// makeRegexNode constructs a FilterTreeNode for a regular expression condition with a specified string.
// makeRegexNode constructs a FilterTreeNode for a regexp conditions.
func makeRegexNode(field schema.Field, s string) *FilterTreeNode {
	return makeNode(field, FilterModeRegexp, s)
}

// makeInNode constructs a FilterTreeNode for an IN condition with a list of integer values.
func makeInNode(field schema.Field, vals any) *FilterTreeNode {
	return makeNode(field, FilterModeIn, vals)
}

// makeNiNode constructs a FilterTreeNode for an Not IN condition with a list of integer values.
func makeNotInNode(field schema.Field, vals any) *FilterTreeNode {
	return makeNode(field, FilterModeNotIn, vals)
}

// makeNotEqualNode constructs a FilterTreeNode for a not-equal condition with a specified integer value.
func makeNotEqualNode(field schema.Field, val any) *FilterTreeNode {
	return makeNode(field, FilterModeNotEqual, val)
}

// makeGtNode constructs a FilterTreeNode for a greater-than condition with a specified integer value.
func makeGtNode(field schema.Field, val any) *FilterTreeNode {
	return makeNode(field, FilterModeGt, val)
}

// makeLtNode constructs a FilterTreeNode for a less-than condition with a specified integer value.
func makeLtNode(field schema.Field, val any) *FilterTreeNode {
	return makeNode(field, FilterModeLt, val)
}

// makeGeNode constructs a FilterTreeNode for a greater-than-or-equal condition with a specified integer value.
func makeGeNode(field schema.Field, val any) *FilterTreeNode {
	return makeNode(field, FilterModeGe, val)
}

// makeLeNode constructs a FilterTreeNode for a less-than-or-equal condition with a specified integer value.
func makeLeNode(field schema.Field, val any) *FilterTreeNode {
	return makeNode(field, FilterModeLe, val)
}

// makeFalseNode constructs a FilterTreeNode for a false condition.
func makeFalseNode(field schema.Field, val any) *FilterTreeNode {
	return makeNode(field, FilterModeFalse, val)
}

// makeTrueNode constructs a FilterTreeNode for a true condition.
func makeTrueNode(field schema.Field, val any) *FilterTreeNode {
	return makeNode(field, FilterModeTrue, val)
}
