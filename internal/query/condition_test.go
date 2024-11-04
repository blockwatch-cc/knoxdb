// Copyright (c) 2024 Blockwatch Data Inc.
// Author: oliver@blockwatch.cc

package query

import (
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/schema"
)

var testSchema *schema.Schema

func init() {
	testSchema = schema.NewSchema().
		WithName("test").
		WithField(schema.NewField(types.FieldTypeInt64).WithName("id").WithFlags(types.FieldFlagPrimary)).
		WithField(schema.NewField(types.FieldTypeFloat64).WithName("score")).
		WithField(schema.NewField(types.FieldTypeString).WithName("name")).
		WithField(schema.NewField(types.FieldTypeDatetime).WithName("created")).
		WithField(schema.NewField(types.FieldTypeUint16).WithName("status").WithFlags(types.FieldFlagEnum)).
		WithField(schema.NewField(types.FieldTypeBoolean).WithName("is_active")).
		Finalize()

	statusEnum := schema.NewEnumDictionary("status")
	statusEnum.Append("active", "pending", "inactive")
	testSchema.WithEnum(statusEnum)
}

// TestConditionParse verifies that ParseCondition correctly handles various input formats
// and data types, including edge cases and error conditions. It ensures proper type conversion
// and validation of field names and filter modes.
func TestConditionParse(t *testing.T) {
	// Test cases cover core functionality:
	// - Basic type parsing (int, float, string)
	// - Special formats (date ranges, enums)
	// - Error cases (invalid fields, modes)
	tests := []struct {
		name     string
		key      string
		val      string
		expected Condition
		wantErr  bool
	}{
		// Basic integer equality - verifies number parsing and type conversion
		{"Equal Integer", "id", "123", Condition{Name: "id", Type: types.FieldTypeInt64, Mode: FilterModeEqual, Value: int64(123)}, false},

		// Float comparison - tests decimal parsing and GT mode
		{"Greater Than Float", "score.gt", "4.5", Condition{Name: "score", Type: types.FieldTypeFloat64, Mode: FilterModeGt, Value: 4.5}, false},

		// String pattern matching - validates regexp mode handling
		{"String Contains", "name.re", "Blockwatch", Condition{Name: "name", Type: types.FieldTypeString, Mode: FilterModeRegexp, Value: "Blockwatch"}, false},

		// Date range - tests date parsing and range mode handling
		{"Date Range", "created.rg", "2023-01-01,2023-12-31", Condition{Name: "created", Type: types.FieldTypeDatetime, Mode: FilterModeRange, Value: RangeValue{int64(1672531200000000000), int64(1703980800000000000)}}, false},

		// Enum in - tests enum parsing and IN mode handling
		{"Enum In", "status.in", "1,2", Condition{Name: "status", Type: types.FieldTypeUint16, Mode: FilterModeIn, Value: []uint16{1, 2}}, false},

		// Invalid field - tests error handling for invalid fields
		{"Invalid Field", "nonexistent", "value", Condition{}, true},

		// Invalid mode - tests error handling for invalid modes
		{"Invalid Mode", "id.invalid", "123", Condition{}, true},

		// Empty string - tests empty string handling
		{"Empty String", "name", "", Condition{Name: "name", Type: types.FieldTypeString, Mode: FilterModeEqual, Value: ""}, false},

		// Boolean value - tests boolean parsing and mode handling
		{"Boolean Value", "is_active", "true", Condition{Name: "is_active", Type: types.FieldTypeBoolean, Mode: FilterModeEqual, Value: true}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseCondition(tt.key, tt.val, testSchema)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseCondition() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && !conditionEqual(got, tt.expected) {
				t.Errorf("ParseCondition() = %v, want %v", got, tt.expected)
			}
		})
	}
}

// TestConditionCompile tests the Compile method of Condition with different condition types.
func TestConditionCompile(t *testing.T) {
	tests := []struct {
		name    string
		c       Condition
		wantErr bool
	}{
		{
			name:    "Simple Equal",
			c:       Condition{Name: "id", Mode: FilterModeEqual, Value: int64(123)},
			wantErr: false,
		},
		{
			name:    "Range Condition",
			c:       Condition{Name: "score", Mode: FilterModeRange, Value: RangeValue{3.5, 4.5}},
			wantErr: false,
		},
		{
			name:    "Enum In",
			c:       Condition{Name: "status", Mode: FilterModeIn, Value: []uint16{1, 2}},
			wantErr: false,
		},
		{
			name:    "Invalid Field",
			c:       Condition{Name: "invalid", Mode: FilterModeEqual, Value: 123},
			wantErr: true,
		},
		{
			name: "Complex AND Condition",
			c: And(
				Equal("id", 1),
				Gt("score", 4.5),
				Regexp("name", "Block.*"),
			),
			wantErr: false,
		},
		{
			name: "Complex OR Condition",
			c: Or(
				Equal("id", 1),
				Equal("id", 2),
				Equal("id", 3),
			),
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := tt.c.Compile(testSchema)
			if (err != nil) != tt.wantErr {
				t.Errorf("Condition.Compile() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

// TestConditionFields tests the Fields method of Condition for various condition structures.
func TestConditionFields(t *testing.T) {
	tests := []struct {
		name string
		c    Condition
		want []string
	}{
		{
			name: "Single Field",
			c:    Condition{Name: "id", Mode: FilterModeEqual, Value: 1},
			want: []string{"id"},
		},
		{
			name: "Multiple Fields",
			c: And(
				Condition{Name: "id", Mode: FilterModeEqual, Value: 1},
				Condition{Name: "name", Mode: FilterModeEqual, Value: "Blockwatch"},
			),
			want: []string{"id", "name"},
		},
		{
			name: "Nested Fields",
			c: And(
				Condition{Name: "id", Mode: FilterModeEqual, Value: 1},
				Or(
					Condition{Name: "name", Mode: FilterModeEqual, Value: "Blockwatch"},
					Condition{Name: "age", Mode: FilterModeGt, Value: 6},
				),
			),
			want: []string{"age", "id", "name"},
		},
		{
			name: "Empty Condition",
			c:    Condition{},
			want: nil,
		},
		{
			name: "Duplicate Fields",
			c: And(
				Equal("id", 1),
				Equal("id", 2),
				Equal("name", "Blockwatch"),
			),
			want: []string{"id", "name"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.c.Fields()
			if !equalStringSlices(got, tt.want) {
				t.Errorf("Condition.Fields() = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestConditionAdd verifies the composition of conditions using the Add method.
// This is a critical test as it ensures proper building of complex query trees.
func TestConditionAdd(t *testing.T) {
	// Test strategy:
	// 1. Start with simple conditions
	// 2. Add complexity gradually
	// 3. Verify both structure and string representation
	tests := []struct {
		name     string
		initial  Condition
		add      Condition
		expected string
	}{
		// Basic AND composition
		{
			name:     "Add AND Condition",
			initial:  Equal("id", 1),
			add:      Equal("name", "Blockwatch"),
			expected: "id = 1 AND name = Blockwatch",
		},

		// Complex nested conditions - tests tree building
		{
			name:     "Add Complex Nested Condition",
			initial:  Equal("id", 1),
			add:      And(Equal("name", "Blockwatch"), Or(Gt("age", 18), Lt("age", 65))),
			expected: "id = 1 AND name = Blockwatch AND (age > 18 OR age < 65)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.initial.Add(tt.add)
			if got := tt.initial.String(); got != tt.expected {
				t.Errorf("After Add(), condition = %v, want %v", got, tt.expected)
			}
		})
	}
}

// TestConditionClear tests the Clear method of Condition.
func TestConditionClear(t *testing.T) {
	tests := []struct {
		name    string
		initial Condition
	}{
		{
			name:    "Clear Simple Condition",
			initial: Equal("id", 1),
		},
		{
			name: "Clear Complex Condition",
			initial: And(
				Equal("id", 1),
				Or(
					Equal("name", "Blockwatch"),
					Gt("age", 18),
				),
			),
		},
		{
			name:    "Clear Empty Condition",
			initial: Condition{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.initial.Clear()
			if !tt.initial.IsEmpty() {
				t.Errorf("Condition.Clear() did not result in an empty condition: %v", tt.initial)
			}
		})
	}
}

// TestConditionIsEmpty tests the IsEmpty method of Condition.
func TestConditionIsEmpty(t *testing.T) {
	tests := []struct {
		name      string
		condition Condition
		want      bool
	}{
		{
			name:      "Empty Condition",
			condition: Condition{},
			want:      true,
		},
		{
			name:      "Non-Empty Simple Condition",
			condition: Equal("id", 1),
			want:      false,
		},
		{
			name: "Non-Empty Complex Condition",
			condition: And(
				Equal("id", 1),
				Equal("name", "Blockwatch"),
			),
			want: false,
		},
		{
			name:      "Condition with Invalid Mode",
			condition: Condition{Name: "id", Mode: FilterModeInvalid},
			want:      true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.condition.IsEmpty(); got != tt.want {
				t.Errorf("Condition.IsEmpty() = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestConditionIsLeaf tests the IsLeaf method of Condition.
func TestConditionIsLeaf(t *testing.T) {
	tests := []struct {
		name      string
		condition Condition
		want      bool
	}{
		{
			name:      "Leaf Condition",
			condition: Equal("id", 1),
			want:      true,
		},
		{
			name: "Non-Leaf Condition",
			condition: And(
				Equal("id", 1),
				Equal("name", "Blockwatch"),
			),
			want: false,
		},
		{
			name:      "Empty Condition",
			condition: Condition{},
			want:      false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.condition.IsLeaf(); got != tt.want {
				t.Errorf("Condition.IsLeaf() = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestConditionString tests the String method of Condition.
func TestConditionString(t *testing.T) {
	tests := []struct {
		name      string
		condition Condition
		want      string
	}{
		{
			name:      "Simple Equal Condition",
			condition: Equal("id", 1),
			want:      "id = 1",
		},
		{
			name:      "Range Condition",
			condition: Range("score", 3.5, 4.5),
			want:      "score RANGE [3.5, 4.5]",
		},
		{
			name:      "In Condition with Many Values",
			condition: In("status", []uint16{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17}),
			want:      "status IN [17 values]",
		},
		{
			name: "Complex AND Condition",
			condition: And(
				Equal("id", 1),
				Gt("score", 4.5),
				Regexp("name", "Block.*"),
			),
			want: "id = 1 AND score > 4.5 AND name REGEXP Block.*",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.condition.String(); got != tt.want {
				t.Errorf("Condition.String() = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestConditionValidate tests the Validate method of Condition.
func TestConditionValidate(t *testing.T) {
	tests := []struct {
		name    string
		cond    Condition
		wantErr bool
	}{
		{"Valid Condition", Equal("id", 1), false},
		{"Empty Name", Condition{Mode: FilterModeEqual, Value: 1}, true},
		{"Invalid Mode", Condition{Name: "id", Mode: FilterMode(999), Value: 1}, true},
		{"Nil Value", Condition{Name: "id", Mode: FilterModeEqual}, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := tt.cond.Compile(testSchema)
			if (err != nil) != tt.wantErr {
				t.Errorf("Condition.Compile() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

// TestConditionCompileToFilterTreeNode tests the Compile method of Condition to FilterTreeNode.
func TestConditionCompileToFilterTreeNode(t *testing.T) {
	tests := []struct {
		name    string
		cond    Condition
		checkFn func(*testing.T, *FilterTreeNode)
	}{
		{
			name: "Simple Equal Condition",
			cond: Equal("id", 1),
			checkFn: func(t *testing.T, node *FilterTreeNode) {
				if !node.IsLeaf() {
					t.Errorf("Expected leaf node")
				}
				if node.Filter.Name != "id" || node.Filter.Mode != FilterModeEqual {
					t.Errorf("Unexpected filter: %+v", node.Filter)
				}
			},
		},
		{
			name: "Complex AND Condition",
			cond: And(Equal("id", 1), Gt("score", 4.5)),
			checkFn: func(t *testing.T, node *FilterTreeNode) {
				if node.IsLeaf() {
					t.Errorf("Expected non-leaf node")
				}
				if node.OrKind {
					t.Errorf("Expected AND node")
				}
				if len(node.Children) != 2 {
					t.Errorf("Expected 2 children, got %d", len(node.Children))
				}
			},
		},
		{
			name: "Complex OR Condition",
			cond: Or(Equal("id", 1), Equal("id", 2)),
			checkFn: func(t *testing.T, node *FilterTreeNode) {
				if node.IsLeaf() {
					t.Errorf("Expected non-leaf node")
				}
				if !node.OrKind {
					t.Errorf("Expected OR node")
				}
				if len(node.Children) != 2 {
					t.Errorf("Expected 2 children, got %d", len(node.Children))
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			node, err := tt.cond.Compile(testSchema)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			tt.checkFn(t, node)
		})
	}
}

// TestConditionRename tests the Rename method of Condition.
func TestConditionRename(t *testing.T) {
	cond := And(Equal("old_name", 1), Gt("score", 4.5))
	cond.Rename("new_name")
	expected := "(new_name = 1 AND score > 4.5)"
	if got := cond.String(); got != expected {
		t.Errorf("After Rename(), condition = %v, want %v", got, expected)
	}
}

// TestConditionWithDifferentTypes tests the behavior of conditions with different data types.
func TestConditionWithDifferentTypes(t *testing.T) {
	tests := []struct {
		name string
		c    Condition
		want string
	}{
		{"Integer", Equal("id", 123), "id = 123"},
		{"Float", Gt("score", 4.5), "score > 4.5"},
		{"String", Equal("name", []byte("Blockwatch")), "name = Blockwatch"},
		{"Boolean", Equal("is_active", true), "is_active = true"},
		{"Datetime", Ge("created", time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)), "created >= 2023-01-01 00:00:00 +0000 UTC"},
		{"Enum", In("status", []uint16{1, 2}), "status IN [1 2]"},
		{"Range", Range("score", 3.5, 4.5), "score RANGE [3.5, 4.5]"},
		{"Regexp", Regexp("name", "Block.*"), "name REGEXP Block.*"},
		{"Complex AND", And(Equal("id", 1), Gt("score", 4.5)), "(id = 1 AND score > 4.5)"},
		{"Complex OR", Or(Equal("id", 1), Equal("id", 2)), "(id = 1 OR id = 2)"},
		{"Not Equal", NotEqual("id", 5), "id != 5"},
		{"Less Than", Lt("score", 3.0), "score < 3.0"},
		{"Less Than or Equal", Le("score", 3.0), "score <= 3.0"},
		{"Greater Than or Equal", Ge("score", 4.5), "score >= 4.5"},
		{"Not In", NotIn("status", []uint16{3, 4}), "status NOT IN [3 4]"},
		{"Complex Nested", And(
			Equal("id", 1),
			Or(
				Gt("score", 4.5),
				Lt("score", 2.0),
			),
			In("status", []uint16{1, 2, 3}),
		), "(id = 1 AND (score > 4.5 OR score < 2.0) AND status IN [1 2 3])"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := tt.c.Compile(testSchema)
			if err != nil {
				t.Errorf("Condition.Compile() error = %v", err)
				return
			}
			if got := tt.c.String(); got != tt.want {
				t.Errorf("Condition.String() = %v, want %v", got, tt.want)
			}
		})
	}
}

// BenchmarkConditionParse benchmarks the ParseCondition function.
func BenchmarkConditionParse(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ParseCondition("id", "123", testSchema)
	}
}

// BenchmarkConditionCompile benchmarks the Compile method of a simple Condition.
func BenchmarkConditionCompile(b *testing.B) {
	c := Equal("id", 123)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.Compile(testSchema)
	}
}

// BenchmarkComplexConditionCompile benchmarks the Compile method of a complex Condition.
func BenchmarkComplexConditionCompile(b *testing.B) {
	c := And(
		Equal("id", 1),
		Or(
			Gt("score", 4.5),
			Regexp("name", "Block.*"),
		),
		In("status", []uint16{1, 2}),
	)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.Compile(testSchema)
	}
}

// Helper functions

func equalStringSlices(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func conditionEqual(a, b Condition) bool {
	if a.Name != b.Name || a.Type != b.Type || a.Mode != b.Mode || a.OrKind != b.OrKind {
		return false
	}

	if !reflect.DeepEqual(a.Value, b.Value) {
		return false
	}

	if len(a.Children) != len(b.Children) {
		return false
	}

	for i := range a.Children {
		if !conditionEqual(a.Children[i], b.Children[i]) {
			return false
		}
	}

	return true
}

// TestConditionAddIsolated tests the Add operation in isolation to better understand
// the behavior of combining conditions. It provides detailed logging of the condition
// state before and after the operation.
func TestConditionAddIsolated(t *testing.T) {
	t.Run("Basic AND Operation", func(t *testing.T) {
		c1 := Equal("id", 1)
		c2 := Equal("name", "test")

		before := c1.String()
		c1.Add(c2)
		after := c1.String()

		t.Logf("Before: %s", before)
		t.Logf("After: %s", after)
		t.Logf("Children: %d", len(c1.Children))
		t.Logf("Mode: %v", c1.Mode)
	})
}

// TestByteMatcherTypeConversion verifies the type conversion behavior of bytesMatcher
// with different input types. It ensures proper handling of strings, byte slices,
// and invalid inputs.
func TestByteMatcherTypeConversion(t *testing.T) {
	tests := []struct {
		name    string
		input   any
		wantErr bool
	}{
		{"String Input", "test", false},
		{"Byte Slice Input", []byte("test"), false},
		{"Invalid Input", 123, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					if !tt.wantErr {
						t.Errorf("unexpected panic: %v", r)
					}
				}
			}()

			m := &bytesMatcher{}
			m.WithValue(tt.input)
		})
	}
}

// TestConditionValidationRules verifies that the condition validation logic correctly
// enforces all validation rules, particularly focusing on field name requirements
// and filter mode constraints.
func TestConditionValidationRules(t *testing.T) {
	tests := []struct {
		name      string
		condition Condition
		wantErr   bool
		errMsg    string
	}{
		{
			name: "Empty Field Name",
			condition: Condition{
				Mode:  FilterModeEqual,
				Value: 123,
			},
			wantErr: true,
			errMsg:  "empty field name",
		},
		{
			name: "Invalid Filter Mode",
			condition: Condition{
				Name:  "test",
				Mode:  FilterMode(999),
				Value: 123,
			},
			wantErr: true,
			errMsg:  "invalid filter mode",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := tt.condition.Compile(testSchema)
			if tt.wantErr {
				if err == nil {
					t.Errorf("expected error containing %q, got nil", tt.errMsg)
				} else if !strings.Contains(err.Error(), tt.errMsg) {
					t.Errorf("expected error containing %q, got %q", tt.errMsg, err)
				}
			} else if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}

// TestConditionAddDebug provides detailed debugging information about the Add operation,
// logging the complete state of conditions before and after the operation to help
// diagnose tree building issues.
func TestConditionAddDebug(t *testing.T) {
	c1 := Equal("id", 1)
	c2 := Equal("name", "Blockwatch")

	t.Logf("Initial c1: %+v", c1)
	t.Logf("Initial c2: %+v", c2)

	c1.Add(c2)

	t.Logf("After Add c1: %+v", c1)
	t.Logf("Children count: %d", len(c1.Children))
	t.Logf("OrKind: %v", c1.OrKind)
}

// TestConditionStringDebug tests the string representation of conditions with detailed
// logging of the internal structure. It helps verify that the string output correctly
// reflects the condition tree structure.
func TestConditionStringDebug(t *testing.T) {
	c := And(
		Equal("id", 1),
		Gt("score", 4.5),
		Regexp("name", "Block.*"),
	)

	t.Logf("Condition structure: %+v", c)
	t.Logf("Children count: %d", len(c.Children))
	t.Logf("String output: %s", c.String())

	// Test each child separately
	for i, child := range c.Children {
		t.Logf("Child %d: %s", i, child.String())
	}
}

// TestConditionValidateDebug provides detailed debugging information during condition
// validation, logging the complete validation process including intermediate states
// and error conditions.
func TestConditionValidateDebug(t *testing.T) {
	tests := []struct {
		name string
		c    Condition
	}{
		{
			name: "Empty Name",
			c: Condition{
				Mode:  FilterModeEqual,
				Value: 1,
			},
		},
		{
			name: "Invalid Mode",
			c: Condition{
				Name:  "id",
				Mode:  FilterMode(999),
				Value: 1,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			node, err := tt.c.Compile(testSchema)
			t.Logf("Test: %s", tt.name)
			t.Logf("Condition: %+v", tt.c)
			t.Logf("Error: %v", err)
			t.Logf("Node: %+v", node)
		})
	}
}

// TestConditionRenameDebug verifies the rename operation with detailed logging of
// the condition structure before and after the rename, helping diagnose issues with
// tree traversal during rename operations.
func TestConditionRenameDebug(t *testing.T) {
	c := And(Equal("id", 1), Gt("score", 4.5))

	t.Logf("Before rename: %s", c.String())
	t.Logf("Structure before: %+v", c)

	c.Rename("new_name")

	t.Logf("After rename: %s", c.String())
	t.Logf("Structure after: %+v", c)

	// Check children
	for i, child := range c.Children {
		t.Logf("Child %d after rename: %+v", i, child)
	}
}

// TestByteMatcherTypeHandling verifies that bytesMatcher correctly handles different
// input types and properly reports type conversion errors. It specifically tests
// the behavior with string inputs that should be converted to byte slices.
func TestByteMatcherTypeHandling(t *testing.T) {
	tests := []struct {
		name    string
		input   any
		wantErr bool
		errMsg  string
	}{
		{
			name:    "String Input",
			input:   "test",
			wantErr: true,
			errMsg:  "interface conversion: interface {} is string, not []uint8",
		},
		{
			name:    "Byte Slice Input",
			input:   []byte("test"),
			wantErr: false,
		},
		{
			name:    "Empty String",
			input:   "",
			wantErr: true,
			errMsg:  "interface conversion: interface {} is string, not []uint8",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					if !tt.wantErr {
						t.Errorf("unexpected panic: %v", r)
					} else if !strings.Contains(fmt.Sprint(r), tt.errMsg) {
						t.Errorf("got panic %v, want %v", r, tt.errMsg)
					}
				}
			}()

			c := Condition{
				Name:  "test",
				Mode:  FilterModeEqual,
				Value: tt.input,
			}
			c.Compile(testSchema)
		})
	}
}

// TestConditionAddDetailed provides a detailed verification of the Add operation,
// with extensive logging of the condition state at each step. It helps diagnose
// issues with condition tree building and maintenance.
func TestConditionAddDetailed(t *testing.T) {
	tests := []struct {
		name     string
		initial  Condition
		add      Condition
		expected string
		debug    bool
	}{
		{
			name:     "Add AND with Debug",
			initial:  Equal("id", 1),
			add:      Equal("name", "Blockwatch"),
			expected: "id = 1 AND name = Blockwatch",
			debug:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.debug {
				t.Logf("Initial condition: %+v", tt.initial)
				t.Logf("Initial children: %+v", tt.initial.Children)
				t.Logf("Initial string: %s", tt.initial.String())
			}

			tt.initial.Add(tt.add)

			if tt.debug {
				t.Logf("After Add:")
				t.Logf("  Condition: %+v", tt.initial)
				t.Logf("  Children: %+v", tt.initial.Children)
				t.Logf("  String: %s", tt.initial.String())
				t.Logf("  OrKind: %v", tt.initial.OrKind)
				t.Logf("  Mode: %v", tt.initial.Mode)
			}

			if got := tt.initial.String(); got != tt.expected {
				t.Errorf("\ngot:  %s\nwant: %s", got, tt.expected)
			}
		})
	}
}

// TestConditionStringDebugging verifies the string representation of complex condition
// trees with detailed structure logging. It ensures that the string output correctly
// reflects the logical structure of the condition tree.
func TestConditionStringDebugging(t *testing.T) {
	c := And(
		Equal("id", 1),
		Gt("score", 4.5),
		Regexp("name", "Block.*"),
	)

	t.Log("Condition Structure:")
	t.Logf("Root: %+v", c)
	t.Logf("Children count: %d", len(c.Children))

	for i, child := range c.Children {
		t.Logf("Child %d:", i)
		t.Logf("  Name: %s", child.Name)
		t.Logf("  Mode: %v", child.Mode)
		t.Logf("  Value: %v", child.Value)
		t.Logf("  String: %s", child.String())
	}

	got := c.String()
	want := "id = 1 AND score > 4.5 AND name REGEXP Block.*"

	if got != want {
		t.Errorf("\nString representation mismatch:\ngot:  %s\nwant: %s", got, want)
	}
}

// TestConditionValidationEdgeCases tests boundary conditions and edge cases in
// condition validation, particularly focusing on empty names, invalid modes,
// and other potential error conditions.
func TestConditionValidationEdgeCases(t *testing.T) {
	tests := []struct {
		name    string
		c       Condition
		wantErr bool
		errType string
	}{
		{
			name: "Empty Name with Value",
			c: Condition{
				Mode:  FilterModeEqual,
				Value: 123,
			},
			wantErr: true,
			errType: "empty field name",
		},
		{
			name: "Invalid Mode Max",
			c: Condition{
				Name:  "test",
				Mode:  FilterMode(255),
				Value: 123,
			},
			wantErr: true,
			errType: "invalid filter mode",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Logf("Testing condition: %+v", tt.c)
			_, err := tt.c.Compile(testSchema)

			if tt.wantErr && err == nil {
				t.Errorf("expected error containing %q, got nil", tt.errType)
			} else if err != nil && !tt.wantErr {
				t.Errorf("unexpected error: %v", err)
			} else if err != nil && !strings.Contains(err.Error(), tt.errType) {
				t.Errorf("got error %q, want error containing %q", err.Error(), tt.errType)
			}
		})
	}
}
