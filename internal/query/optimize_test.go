// Copyright (c) 2024 Blockwatch Data Inc.
// Author: oliver@blockwatch.cc

package query

import (
    "fmt"
    "github.com/stretchr/testify/assert"
    "reflect"
    "testing"
    "time"
)

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

// TestOptimizeConditions tests the query optimizer's ability to handle complex
// and edge cases. It verifies:
// 1. Condition merging and simplification
// 2. Reordering by selectivity
// 3. Handling of invalid/contradictory conditions
// 4. Type safety and mixed type handling
func TestOptimizeConditions(t *testing.T) {
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
            // Clone input to ensure it's not modified
            input := cloneFilterTree(tt.input)

            // Catch panics
            defer func() {
                if r := recover(); r != nil {
                    t.Errorf("Test panicked: %v", r)
                }
            }()

            input.Optimize()
            assertTreeEqual(t, tt.expected, input, tt.comment)
        })
    }
}

// Update assertFilterTreeEqual to be more lenient with ordering
func assertFilterTreeEqual(t *testing.T, expected, actual *FilterTreeNode, msg string) {
    if expected == nil && actual == nil {
        return
    }
    if expected == nil || actual == nil {
        t.Errorf("%s: one tree is nil while other is not", msg)
        return
    }

    // Compare node properties
    assert.Equal(t, expected.OrKind, actual.OrKind, msg+": OrKind mismatch")

    // Compare filters if both exist
    if expected.Filter != nil && actual.Filter != nil {
        assert.Equal(t, expected.Filter.Mode, actual.Filter.Mode, msg+": Filter Mode mismatch")
        assert.Equal(t, expected.Filter.Index, actual.Filter.Index, msg+": Filter Index mismatch")
        assert.Equal(t, expected.Filter.Type, actual.Filter.Type, msg+": Filter Type mismatch")
        assert.Equal(t, expected.Filter.Value, actual.Filter.Value, msg+": Filter Value mismatch")
    } else if expected.Filter != nil || actual.Filter != nil {
        t.Errorf("%s: Filter existence mismatch", msg)
    }

    // Compare children length
    assert.Equal(t, len(expected.Children), len(actual.Children), msg+": number of children mismatch")

    // Compare children recursively (order matters only for same field conditions)
    for i := range expected.Children {
        if i < len(actual.Children) {
            assertFilterTreeEqual(t, expected.Children[i], actual.Children[i], fmt.Sprintf("%s (child %d)", msg, i))
        }
    }
}

// assertFilterEqual compares two filters in detail
func assertFilterEqual(t *testing.T, expected, actual *Filter, msg string) {
    t.Helper()

    assert.Equal(t, expected.Mode, actual.Mode, "%s: Filter Mode mismatch", msg)
    assert.Equal(t, expected.Index, actual.Index, "%s: Filter Index mismatch", msg)
    assert.Equal(t, expected.Type, actual.Type, "%s: Filter Type mismatch", msg)

    // Special handling for different value types
    switch expected.Value.(type) {
    case []byte:
        // Convert actual to []byte if it's a string
        if s, ok := actual.Value.(string); ok {
            actual.Value = []byte(s)
        }
    case string:
        // Convert actual to string if it's a []byte
        if b, ok := actual.Value.([]byte); ok {
            actual.Value = string(b)
        }
    }

    assert.Equal(t, expected.Value, actual.Value, "%s: Filter Value mismatch", msg)
}

// cloneFilterTree creates a deep copy with proper matcher initialization
func cloneFilterTree(n *FilterTreeNode) *FilterTreeNode {
    if n == nil {
        return nil
    }

    clone := &FilterTreeNode{
        OrKind: n.OrKind,
        Skip:   n.Skip,
        Empty:  n.Empty,
    }

    if n.Filter != nil {
        clone.Filter = &Filter{
            Mode:  n.Filter.Mode,
            Index: n.Filter.Index,
            Type:  n.Filter.Type,
            Value: cloneValue(n.Filter.Value),
        }
        // Properly initialize the matcher
        clone.Filter.Matcher = newFactory(clone.Filter.Type).New(clone.Filter.Mode)
        // Fix: Handle nil values properly
        if clone.Filter.Value != nil {
            if clone.Filter.Mode == FilterModeIn || clone.Filter.Mode == FilterModeNotIn {
                clone.Filter.Matcher.WithSlice(clone.Filter.Value)
            } else {
                clone.Filter.Matcher.WithValue(clone.Filter.Value)
            }
        }
    }

    if len(n.Children) > 0 {
        clone.Children = make([]*FilterTreeNode, len(n.Children))
        for i, child := range n.Children {
            clone.Children[i] = cloneFilterTree(child)
        }
    }

    return clone
}

// cloneValue creates a deep copy of filter values
func cloneValue(v interface{}) interface{} {
    if v == nil {
        return nil
    }

    switch val := v.(type) {
    case []byte:
        clone := make([]byte, len(val))
        copy(clone, val)
        return clone
    case []interface{}:
        clone := make([]interface{}, len(val))
        for i, item := range val {
            clone[i] = cloneValue(item)
        }
        return clone
    case RangeValue:
        // Fix: Use array indexing since RangeValue is [2]any
        return RangeValue{
            cloneValue(val[0]),
            cloneValue(val[1]),
        }
    default:
        return val
    }
}

// validateRangeValues ensures range values are properly ordered and of consistent types
func validateRangeValues(t *testing.T, name string, val RangeValue) {
    t.Helper()

    if val[0] == nil || val[1] == nil {
        t.Errorf("%s: range values cannot be nil", name)
        return
    }

    // Check types match
    if reflect.TypeOf(val[0]) != reflect.TypeOf(val[1]) {
        t.Errorf("%s: range value types must match: %T != %T",
            name, val[0], val[1])
    }

    // Check ordering for numeric types
    switch v0 := val[0].(type) {
    case int64:
        v1 := val[1].(int64)
        if v0 > v1 {
            t.Errorf("%s: range values must be ordered: %d > %d",
                name, v0, v1)
        }
        // Add other numeric types...
    }
}

// validateFilterTree ensures the tree structure is valid
func validateFilterTree(t *testing.T, name string, node *FilterTreeNode) {
    t.Helper()

    if node == nil {
        return
    }

    // Check for nil filters in non-leaf nodes
    if len(node.Children) > 0 && node.Filter != nil {
        t.Errorf("%s: non-leaf node cannot have filter", name)
    }

    // Check for missing filters in leaf nodes
    if len(node.Children) == 0 && node.Filter == nil {
        t.Errorf("%s: leaf node must have filter", name)
    }

    // Validate children
    for i, child := range node.Children {
        if child == nil {
            t.Errorf("%s: child node %d cannot be nil", name, i)
            continue
        }
        validateFilterTree(t, fmt.Sprintf("%s.child[%d]", name, i), child)
    }
}

// assertTreeEqual compares two FilterTreeNodes for equality
func assertTreeEqual(t *testing.T, expected, actual *FilterTreeNode, msg string) {
    t.Helper()

    if expected == nil && actual == nil {
        return
    }

    if expected == nil || actual == nil {
        t.Errorf("%s: one tree is nil: expected=%v, actual=%v",
            msg, expected != nil, actual != nil)
        return
    }

    // Compare number of children
    if len(expected.Children) != len(actual.Children) {
        t.Errorf("%s: number of children mismatch: expected=%d, actual=%d",
            msg, len(expected.Children), len(actual.Children))
        return
    }

    // Compare filters
    if expected.Filter != nil || actual.Filter != nil {
        if expected.Filter == nil || actual.Filter == nil {
            t.Errorf("%s: Filter existence mismatch", msg)
            return
        }

        // Compare filter properties
        if expected.Filter.Index != actual.Filter.Index {
            t.Errorf("%s: Filter Index mismatch: expected=0x%x, actual=0x%x",
                msg, expected.Filter.Index, actual.Filter.Index)
        }
        if expected.Filter.Type != actual.Filter.Type {
            t.Errorf("%s: Filter Type mismatch: expected=0x%x, actual=0x%x",
                msg, expected.Filter.Type, actual.Filter.Type)
        }
        if expected.Filter.Mode != actual.Filter.Mode {
            t.Errorf("%s: Filter Mode mismatch: expected=%d, actual=%d",
                msg, expected.Filter.Mode, actual.Filter.Mode)
        }
        if !reflect.DeepEqual(expected.Filter.Value, actual.Filter.Value) {
            t.Errorf("%s: Filter Value mismatch: expected=%#v, actual=%#v",
                msg, expected.Filter.Value, actual.Filter.Value)
        }
    }

    // Compare children recursively
    for i := range expected.Children {
        childMsg := fmt.Sprintf("%s (child %d)", msg, i)
        assertTreeEqual(t, expected.Children[i], actual.Children[i], childMsg)
    }
}
