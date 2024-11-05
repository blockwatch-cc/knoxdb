// Copyright (c) 2024 Blockwatch Data Inc.
// Author: oliver@blockwatch.cc

package query

import (
	"math"
	"reflect"
	"strings"
	"testing"

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

// Core Tests
// ----------

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
		{"Enum In", "status.in", "pending,inactive", Condition{Name: "status", Type: types.FieldTypeUint16, Mode: FilterModeIn, Value: []uint16{1, 2}}, false},

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

// TestConditionFields tests extraction of field names from conditions.
// It verifies correct field name collection from simple and nested conditions.
func TestConditionFields(t *testing.T) {
	tests := []struct {
		name string
		cond Condition
		want []string
	}{
		{
			name: "Single Field",
			cond: Equal("id", 1),
			want: []string{"id"},
		},
		{
			name: "Multiple Fields",
			cond: And(Equal("id", 1), Equal("name", "test")),
			want: []string{"id", "name"},
		},
		{
			name: "Nested Fields",
			cond: And(
				Equal("id", 1),
				Or(Equal("name", "test"), Gt("score", 4.5)),
			),
			want: []string{"id", "name", "score"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.cond.Fields()
			assertStringSlice(t, got, tt.want)
		})
	}
}

// Tree Manipulation Tests
// -----------------------

// TODO: start with an existing AND condititon (use top level function),
// add an OR condition to it
// and the other way around

// TestConditionAdd tests adding conditions to create compound expressions.
// It verifies AND/OR combinations and proper handling of empty conditions.
func TestConditionAdd(t *testing.T) {
	tests := []struct {
		name     string
		initial  Condition
		add      Condition
		expected string
	}{
		{
			name:     "Add AND Condition",
			initial:  Equal("id", 1),
			add:      Equal("name", "Blockwatch"),
			expected: "id = 1 AND name = Blockwatch",
		},
		{
			name: "Add OR Condition",
			initial: Or(
				Equal("id", 1),
				Equal("id", 2),
			),
			add:      Equal("name", "Blockwatch"),
			expected: "(id = 1 OR id = 2) AND name = Blockwatch",
		},
		{
			name:     "Add to Empty Condition",
			initial:  Condition{},
			add:      Equal("id", 123),
			expected: "id = 123",
		},
		{
			name:     "Add Empty Condition",
			initial:  Equal("id", 1),
			add:      Condition{},
			expected: "id = 1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if testing.Verbose() {
				t.Log("Initial condition state:")
				logConditionState(t, &tt.initial, 1)
				t.Log("Adding condition:")
				logConditionState(t, &tt.add, 1)
			}

			tt.initial.Add(tt.add)

			if testing.Verbose() {
				t.Log("Final state:")
				logConditionState(t, &tt.initial, 1)
			}

			assertConditionString(t, tt.initial.String(), tt.expected)
		})
	}
}

// TestConditionClear tests the clearing of condition state.
// Verifies that all fields are reset to zero values and children are removed.
// TODO: consider new fields Type, Index; currently not being cleared
func TestConditionClear(t *testing.T) {
	tests := []struct {
		name     string
		initial  Condition
		validate func(*testing.T, Condition)
	}{
		{
			name:    "Clear Simple Condition",
			initial: Equal("id", 1),
			validate: func(t *testing.T, c Condition) {
				assertConditionEmpty(t, c)
			},
		},
		{
			name: "Clear Complex Condition",
			initial: And(
				Equal("id", 1),
				Or(
					Equal("name", "Blockwatch"),
					Gt("score", 4.5),
				),
			),
			validate: func(t *testing.T, c Condition) {
				assertConditionEmpty(t, c)
			},
		},
		{
			name:    "Clear Empty Condition",
			initial: Condition{},
			validate: func(t *testing.T, c Condition) {
				assertConditionEmpty(t, c)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if testing.Verbose() {
				t.Log("Initial state:")
				logConditionState(t, &tt.initial, 1)
			}

			tt.initial.Clear()
			tt.validate(t, tt.initial)

			if testing.Verbose() {
				t.Log("After Clear:")
				logConditionState(t, &tt.initial, 1)
			}
		})
	}
}

// TestConditionString tests string representation of conditions.
// Verifies correct formatting of simple and complex conditions for debugging and logging.
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
			name: "In Condition with Many Values",
			condition: In("status", []uint16{1, 2, 3, 4, 5, 6, 7, 8, 9, 10,
				11, 12, 13, 14, 15, 16, 17}),
			want: "status IN [17 values]",
		},
		{
			name: "Complex AND Condition",
			condition: And(
				Equal("id", 1),
				Gt("score", 4.5),
				Regexp("name", "Block.*"),
			),
			want: "id = 1 AND score > 4.5 AND name ~= Block.*",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertConditionString(t, tt.condition.String(), tt.want)
		})
	}
}

// TestConditionRename tests field name replacement in conditions.
// Verifies that field names are correctly updated throughout the condition tree.
func TestConditionRename(t *testing.T) {
	tests := []struct {
		name      string
		condition Condition
		newName   string
		want      string
	}{
		{
			name:      "Simple Condition",
			condition: Equal("old_name", 1),
			newName:   "new_name",
			want:      "new_name = 1",
		},
		{
			name: "Complex AND Condition",
			condition: And(
				Equal("old_name", 1),
				Gt("old_name", 5),
			),
			newName: "new_name",
			want:    "old_name = 1 AND old_name > 5",
		},
		{
			name: "Mixed Fields Condition",
			condition: And(
				Equal("old_name", 1),
				Equal("other_field", "test"),
			),
			newName: "new_name",
			want:    "old_name = 1 AND other_field = test",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cond := tt.condition.Rename(tt.newName)
			assertConditionString(t, cond.String(), tt.want)
		})
	}
}

// Validation Tests
// ----------------

// TestConditionValidate tests condition validation rules.
// Verifies field existence, type compatibility, and value constraints.
func TestConditionValidate(t *testing.T) {
	tests := []struct {
		name    string
		cond    Condition
		wantErr bool
		errMsg  string
	}{
		{
			name:    "Valid Condition",
			cond:    Equal("id", 1),
			wantErr: false,
		},
		{
			name: "Empty Name",
			cond: Condition{
				Mode:  FilterModeEqual,
				Value: 1,
			},
			wantErr: true,
			errMsg:  "empty field name",
		},
		{
			name: "Invalid Mode",
			cond: Condition{
				Name:  "id",
				Mode:  FilterMode(255),
				Value: 1,
			},
			wantErr: true,
			errMsg:  "invalid filter mode",
		},
		{
			name: "Nil Value",
			cond: Condition{
				Name: "id",
				Mode: FilterModeEqual,
			},
			wantErr: true,
			errMsg:  "nil filter value",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.cond.Validate()
			assertError(t, err, tt.wantErr, tt.errMsg)
		})
	}
}

// TestConditionValidateField tests field-specific validation rules.
// Verifies type checking, enum validation, and value range constraints.
func TestConditionValidateField(t *testing.T) {
	tests := []struct {
		name      string
		condition Condition
		wantErr   bool
		errMsg    string
	}{
		{
			name:      "Valid Integer Field",
			condition: Equal("id", 123),
			wantErr:   false,
		},
		{
			name:      "Valid Float Field",
			condition: Equal("score", 4.5),
			wantErr:   false,
		},
		{
			name:      "Valid String Field",
			condition: Equal("name", "test"),
			wantErr:   false,
		},
		{
			name:      "Unknown Field",
			condition: Equal("unknown", 1),
			wantErr:   true,
			errMsg:    "unknown column",
		},
		{
			name:      "Type Mismatch Int",
			condition: Equal("id", "not_an_int"),
			wantErr:   true,
			errMsg:    "cast: unexpected value type string for int64 condition",
		},
		{
			name:      "Valid Enum Field",
			condition: Equal("status", uint16(1)),
			wantErr:   false,
		},
		{
			name:      "Invalid Enum Value",
			condition: Equal("status", uint16(999)),
			wantErr:   true,
			errMsg:    "invalid enum code 999",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := tt.condition.Compile(testSchema)
			assertError(t, err, tt.wantErr, tt.errMsg)
		})
	}
}

// Edge Case Tests
// ---------------

// TestConditionValidationEdgeCases tests boundary conditions and error cases.
// Verifies handling of empty values, invalid modes, and value limits.
func TestConditionValidationEdgeCases(t *testing.T) {
	tests := []struct {
		name    string
		cond    Condition
		wantErr bool
		errMsg  string
	}{
		{
			name: "Empty Name with Value",
			cond: Condition{
				Mode:  FilterModeEqual,
				Value: 123,
			},
			wantErr: true,
			errMsg:  "empty field name",
		},
		{
			name: "Invalid Mode Max",
			cond: Condition{
				Name:  "test",
				Mode:  FilterMode(255),
				Value: 123,
			},
			wantErr: true,
			errMsg:  "invalid filter mode",
		},
		// {
		// 	name:    "Float Infinity",
		// 	cond:    Equal("score", math.Inf(1)),
		// 	wantErr: true,
		// 	errMsg:  "invalid float value",
		// },
		// {
		// 	name:    "Large String",
		// 	cond:    Equal("name", strings.Repeat("a", 1<<16)),
		// 	wantErr: true,
		// 	errMsg:  "value too large",
		// },
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if testing.Verbose() {
				t.Logf("Testing condition: %+v", tt.cond)
			}
			_, err := tt.cond.Compile(testSchema)
			assertError(t, err, tt.wantErr, tt.errMsg)
		})
	}
}

// TestConditionIsEmpty tests empty state detection.
// Verifies correct identification of zero-value and cleared conditions.
func TestConditionIsEmpty(t *testing.T) {
	tests := []struct {
		name      string
		condition Condition
		want      bool
		wantErr   bool
	}{
		{
			name:      "Zero Value Condition",
			condition: Condition{},
			want:      true,
			wantErr:   true,
		},
		{
			name:      "Simple Non-Empty Condition",
			condition: Equal("id", 1),
			want:      false,
			wantErr:   false,
		},
		{
			name: "Empty AND Condition",
			condition: And(
				Condition{},
				Condition{},
			),
			want:    false,
			wantErr: true,
		},
		{
			name: "Partially Empty AND",
			condition: And(
				Equal("id", 1),
				Condition{},
			),
			want:    false,
			wantErr: true,
		},
		{
			name: "Complex Non-Empty",
			condition: And(
				Equal("id", 1),
				Or(
					Gt("score", 4.5),
					Lt("score", 2.0),
				),
			),
			want:    false,
			wantErr: false,
		},
		{
			name: "Cleared Condition",
			condition: func() Condition {
				c := Equal("id", 1)
				c.Clear()
				return c
			}(),
			want:    true,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.condition.IsEmpty()
			if got != tt.want {
				t.Errorf("IsEmpty() = %v, want %v", got, tt.want)
			}
			err := tt.condition.Validate()
			assertError(t, err, tt.wantErr, "")
		})
	}
}

// TestConditionBoundaryValues tests handling of extreme values.
// Verifies proper handling of min/max values for different field types.
func TestConditionBoundaryValues(t *testing.T) {
	tests := []struct {
		name    string
		field   string
		value   interface{}
		wantErr bool
		errMsg  string
	}{
		{
			name:    "Int64 Max",
			field:   "id",
			value:   int64(math.MaxInt64),
			wantErr: false,
		},
		{
			name:    "Int64 Min",
			field:   "id",
			value:   int64(math.MinInt64),
			wantErr: false,
		},
		{
			name:    "Float64 Max",
			field:   "score",
			value:   math.MaxFloat64,
			wantErr: false,
		},
		{
			name:    "Float64 Min",
			field:   "score",
			value:   math.SmallestNonzeroFloat64,
			wantErr: false,
		},
		{
			name:    "Empty String",
			field:   "name",
			value:   "",
			wantErr: false,
		},
		{
			name:    "Max Length String",
			field:   "name",
			value:   strings.Repeat("a", 1<<15), // 32KB
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := Equal(tt.field, tt.value)
			_, err := c.Compile(testSchema)
			assertError(t, err, tt.wantErr, tt.errMsg)
		})
	}
}

// TestConditionNestedConversion tests type conversion in nested conditions.
// Verifies correct handling of mixed types and deep condition trees.
func TestConditionNestedConversion(t *testing.T) {
	tests := []struct {
		name    string
		cond    Condition
		wantErr bool
		errMsg  string
	}{
		{
			name: "Mixed Types AND",
			cond: And(
				Equal("id", 123),
				Equal("score", 45.67),
				Equal("name", "test"),
			),
			wantErr: false,
		},
		{
			name: "Deep Nesting",
			cond: And(
				Or(
					Equal("id", 1),
					And(
						Equal("score", 45.67),
						Equal("status", "active"),
					),
				),
				Equal("name", "test"),
			),
			wantErr: false,
		},
		{
			name: "Invalid Mixed Types",
			cond: And(
				Equal("id", "not_a_number"),
				Equal("score", "invalid_float"),
			),
			wantErr: true,
			errMsg:  "cast: unexpected value type string for int64 condition",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if testing.Verbose() {
				t.Logf("Testing condition: %s", tt.cond.String())
				logConditionState(t, &tt.cond, 1)
			}
			_, err := tt.cond.Compile(testSchema)
			assertError(t, err, tt.wantErr, tt.errMsg)
		})
	}
}

// Benchmarks and Fuzz Tests
// -------------------------

// BenchmarkConditionTree measures performance of condition tree operations.
// Tests compilation performance with varying tree sizes.
func BenchmarkConditionTree(b *testing.B) {
	benchmarks := []struct {
		name string
		size int
	}{
		{"Small Tree", 10},
		{"Medium Tree", 100},
		{"Large Tree", 1000},
	}

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			c := buildLargeConditionTree(bm.size)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, err := c.Compile(testSchema)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// FuzzConditionParse tests condition parsing with random inputs.
// Verifies robustness of parsing logic against unexpected inputs.
func FuzzConditionParse(f *testing.F) {
	f.Add("id", "123")
	f.Add("score", "45.67")
	f.Add("name", "test")

	f.Fuzz(func(t *testing.T, field, value string) {
		c, err := ParseCondition(field, value, testSchema)
		if err == nil {
			_, err = c.Compile(testSchema)
			if err != nil {
				t.Errorf("valid parse but invalid compile: %v", err)
			}
		}
	})
}

// Helper Functions
// -----------------------------------------------------------------------------

// assertCondition compares two conditions for equality, including all nested fields and children.
// Helper function that provides detailed error output on mismatch.
func assertCondition(t *testing.T, got, want Condition) {
	t.Helper()
	if !conditionEqual(got, want) {
		t.Errorf("\ngot:  %v\nwant: %v", got, want)
	}
}

// assertError verifies error outcomes match expectations.
// Checks both error presence/absence and specific error message content.
func assertError(t *testing.T, err error, wantErr bool, errMsg string) {
	t.Helper()
	if (err != nil) != wantErr {
		t.Errorf("got err=%v, wantErr=%v", err, wantErr)
		return
	}
	if wantErr && errMsg != "" && !strings.Contains(err.Error(), errMsg) {
		t.Errorf("got err=%q, want containing %q", err.Error(), errMsg)
	}
}

// assertConditionString verifies string representation matches expected output.
// Used for testing condition formatting and debug output.
func assertConditionString(t *testing.T, got, want string) {
	t.Helper()
	if got != want {
		t.Errorf("\ngot:  %v\nwant: %v", got, want)
	}
}

// assertStringSlice verifies string slice equality.
// Used for testing field name collections and other string lists.
func assertStringSlice(t *testing.T, got, want []string) {
	t.Helper()
	if !equalStringSlices(got, want) {
		t.Errorf("\ngot:  %v\nwant: %v", got, want)
	}
}

// assertConditionEmpty verifies a condition is in empty state.
// Checks both value fields and children are properly cleared.
func assertConditionEmpty(t *testing.T, c Condition) {
	t.Helper()
	if !c.IsEmpty() {
		t.Errorf("condition is not empty: %v", c)
	}
}

// comparison helpers
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

// debug helpers
func logConditionState(t *testing.T, c *Condition, depth int) {
	indent := strings.Repeat("  ", depth)
	t.Logf("%sCondition: %+v", indent, c)
	t.Logf("%sChildren: %d", indent, len(c.Children))
	t.Logf("%sOrKind: %v", indent, c.OrKind)

	for i, child := range c.Children {
		t.Logf("%sChild %d:", indent, i)
		logConditionState(t, &child, depth+1)
	}
}

// test data builders
func buildLargeConditionTree(size int) Condition {
	var c Condition
	for i := 0; i < size; i++ {
		child := Equal("id", i)
		if i%2 == 0 {
			child = Or(child, Equal("score", float64(i)))
		}
		c.Add(child)
	}
	return c
}
