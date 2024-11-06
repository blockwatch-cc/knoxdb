// Copyright (c) 2024 Blockwatch Data Inc.
// Author: oliver@blockwatch.cc

package query

import (
	"testing"

	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/schema"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
		{"Equal Integer", "id", "123", Condition{Name: "id", Mode: FilterModeEqual, Value: int64(123)}, false},

		// Hex integer equality - verifies number parsing and type conversion
		{"Equal Integer", "id", "0xff", Condition{Name: "id", Mode: FilterModeEqual, Value: int64(255)}, false},

		// Float comparison - tests decimal parsing and GT mode
		{"Greater Than Float", "score.gt", "4.5", Condition{Name: "score", Mode: FilterModeGt, Value: 4.5}, false},

		// String pattern matching - validates regexp mode handling
		{"String Contains", "name.re", "Blockwatch", Condition{Name: "name", Mode: FilterModeRegexp, Value: "Blockwatch"}, false},

		// Date range - tests date parsing and range mode handling
		{"Date Range", "created.rg", "2023-01-01,2023-12-31", Condition{Name: "created", Mode: FilterModeRange, Value: RangeValue{int64(1672531200000000000), int64(1703980800000000000)}}, false},

		// Enum in - tests enum parsing and IN mode handling
		{"Enum In", "status.in", "pending,inactive", Condition{Name: "status", Mode: FilterModeIn, Value: []uint16{1, 2}}, false},

		// Invalid field - tests error handling for invalid fields
		{"Invalid Field", "nonexistent", "value", Condition{}, true},

		// Invalid mode - tests error handling for invalid modes
		{"Invalid Mode", "id.invalid", "123", Condition{}, true},

		// Empty string - tests empty string handling
		{"Empty String", "name", "", Condition{Name: "name", Mode: FilterModeEqual, Value: ""}, false},

		// Boolean value - tests boolean parsing and mode handling
		{"Boolean Value", "is_active", "true", Condition{Name: "is_active", Mode: FilterModeEqual, Value: true}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parsed, err := ParseCondition(tt.key, tt.val, testSchema)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			assert.Equal(t, tt.expected, parsed)
		})
	}
}

// TestConditionValidate tests condition validation rules.
// Verifies field existence, type compatibility, and value constraints.
func TestConditionValidate(t *testing.T) {
	tests := []struct {
		name    string
		cond    Condition
		wantErr bool
	}{
		{
			name: "Valid Condition",
			cond: Equal("id", 1),
		},
		{
			name: "Complex Condition",
			cond: And(
				Equal("id", 1),
				Or(
					Gt("score", 4.5),
					Lt("score", 2.0),
				),
			),
		},
		// Error cases
		{
			name:    "Empty Condition",
			cond:    Condition{},
			wantErr: true,
		},
		{
			name: "Empty Name",
			cond: Condition{
				Mode:  FilterModeEqual,
				Value: 1,
			},
			wantErr: true,
		},
		{
			name: "Invalid Mode",
			cond: Condition{
				Name:  "id",
				Mode:  FilterMode(255),
				Value: 1,
			},
			wantErr: true,
		},
		{
			name: "Nil Value",
			cond: Condition{
				Name: "id",
				Mode: FilterModeEqual,
			},
			wantErr: true,
		},
		{
			name: "Empty Children",
			cond: And(
				Condition{},
				Condition{},
			),
			wantErr: true,
		},
		{
			name: "Empty Child",
			cond: And(
				Equal("id", 1),
				Condition{},
			),
			wantErr: true,
		},
		{
			name: "Cleared Condition",
			cond: func() Condition {
				c := Equal("id", 1)
				c.Clear()
				return c
			}(),
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.cond.Validate()
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

// TestConditionCompile tests the Compile method of Condition with different condition types.
func TestConditionCompile(t *testing.T) {
	tests := []struct {
		name    string
		cond    Condition
		fields  []string
		wantErr bool
	}{
		{
			name:    "Int Field",
			cond:    Equal("id", int64(123)),
			fields:  []string{"id"},
			wantErr: false,
		},
		{
			name:    "String Field",
			cond:    Equal("name", "test"),
			fields:  []string{"name"},
			wantErr: false,
		},
		{
			name:    "Float Range",
			cond:    Range("score", 3.5, 4.5),
			fields:  []string{"score"},
			wantErr: false,
		},
		{
			name:    "Enum In",
			cond:    In("status", []uint16{1, 2}),
			fields:  []string{"status"},
			wantErr: false,
		},
		{
			name: "Complex AND Condition",
			cond: And(
				Equal("id", 1),
				Gt("score", 4.5),
				Regexp("name", "Block.*"),
			),
			fields:  []string{"id", "name", "score"},
			wantErr: false,
		},
		{
			name: "Complex OR Condition",
			cond: Or(
				Equal("id", 1),
				Equal("id", 2),
				Equal("id", 3),
			),
			fields:  []string{"id"},
			wantErr: false,
		},
		{
			name: "Nested Fields",
			cond: And(
				Equal("id", 1),
				Or(Equal("name", "test"), Gt("score", 4.5)),
			),
			fields:  []string{"id", "name", "score"},
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
			fields:  []string{"id", "name", "score", "status"},
			wantErr: false,
		},
		// Errors
		{
			name:    "Invalid Field",
			cond:    Equal("invalid", 123),
			fields:  []string{"invalid"},
			wantErr: true,
		},
		{
			name:    "Invalid Value Type",
			cond:    Equal("id", "not_an_int"),
			fields:  []string{"id"},
			wantErr: true,
		},
		{
			name:    "Invalid Enum Value",
			cond:    Equal("status", uint16(999)),
			fields:  []string{"status"},
			wantErr: true,
		},
		{
			name:    "Empty Name with Value",
			cond:    Equal("", 123),
			fields:  nil,
			wantErr: true,
		},
		{
			name: "Invalid Mode",
			cond: Condition{
				Name:  "test",
				Mode:  FilterMode(255),
				Value: 123,
			},
			fields:  nil,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// compile error
			_, err := tt.cond.Compile(testSchema)
			if tt.wantErr {
				assert.Error(t, err, "compile error for %s", tt.cond)
			} else {
				assert.NoError(t, err, "compile error for %s", tt.cond)
			}

			// fields
			assert.Equal(t, tt.fields, tt.cond.Fields())
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
		cond     Condition
		add      func(*Condition)
		expected string
	}{
		// add leaf
		{
			name:     "Empty + AND",
			cond:     Condition{},
			add:      func(c *Condition) { c.And("id", FilterModeEqual, 123) },
			expected: "id = 123",
		},
		{
			name:     "AND + AND",
			cond:     Equal("id", 1),
			add:      func(c *Condition) { c.And("name", FilterModeEqual, "Blockwatch") },
			expected: "id = 1 AND name = Blockwatch",
		},
		{
			name:     "Empty + OR",
			cond:     Condition{},
			add:      func(c *Condition) { c.Or("id", FilterModeEqual, 123) },
			expected: "id = 123",
		},
		{
			name:     "AND + OR",
			cond:     Equal("id", 1),
			add:      func(c *Condition) { c.Or("name", FilterModeEqual, "Blockwatch") },
			expected: "id = 1 OR name = Blockwatch",
		},
		{
			name:     "OR + OR",
			cond:     Or(Equal("id", 1)),
			add:      func(c *Condition) { c.Or("name", FilterModeEqual, "Blockwatch") },
			expected: "id = 1 OR name = Blockwatch",
		},
		{
			name:     "OR + AND",
			cond:     Or(Equal("id", 1)),
			add:      func(c *Condition) { c.And("name", FilterModeEqual, "Blockwatch") },
			expected: "(id = 1) AND name = Blockwatch",
		},
		{
			name:     "AND* + OR",
			cond:     And(Equal("id", 1), Equal("id", 2)),
			add:      func(c *Condition) { c.Or("name", FilterModeEqual, "Blockwatch") },
			expected: "(id = 1 AND id = 2) OR name = Blockwatch",
		},

		// add subtree to leaf
		{
			name:     "Empty + AND*",
			cond:     Condition{},
			add:      func(c *Condition) { c.Add(And(Equal("id", 2), Equal("name", "Blockwatch"))) },
			expected: "id = 2 AND name = Blockwatch",
		},
		{
			name:     "AND + AND*",
			cond:     Equal("id", 1),
			add:      func(c *Condition) { c.Add(And(Equal("id", 2), Equal("name", "Blockwatch"))) },
			expected: "id = 1 AND id = 2 AND name = Blockwatch",
		},
		{
			name:     "Empty + OR*",
			cond:     Condition{},
			add:      func(c *Condition) { c.Add(Or(Equal("id", 2), Equal("name", "Blockwatch"))) },
			expected: "id = 2 OR name = Blockwatch",
		},
		{
			name:     "AND + OR*",
			cond:     Equal("id", 1),
			add:      func(c *Condition) { c.Add(Or(Equal("id", 2), Equal("name", "Blockwatch"))) },
			expected: "id = 1 OR id = 2 OR name = Blockwatch",
		},

		// tree + subtree
		{
			name:     "OR* + AND*",
			cond:     Or(Equal("id", 1), Equal("id", 2)),
			add:      func(c *Condition) { c.Add(And(Equal("id", 3), Equal("name", "Blockwatch"))) },
			expected: "(id = 1 OR id = 2) AND id = 3 AND name = Blockwatch",
		},
		{
			name:     "AND* + OR*",
			cond:     And(Equal("id", 1), Equal("id", 2)),
			add:      func(c *Condition) { c.Add(And(Equal("id", 3), Equal("name", "Blockwatch"))) },
			expected: "id = 1 AND id = 2 AND id = 3 AND name = Blockwatch",
		},
		{
			name:     "OR* + OR*",
			cond:     Or(Equal("id", 1), Equal("id", 2)),
			add:      func(c *Condition) { c.Add(Or(Equal("id", 3), Equal("name", "Blockwatch"))) },
			expected: "id = 1 OR id = 2 OR id = 3 OR name = Blockwatch",
		},
		{
			name:     "AND* + AND*",
			cond:     And(Equal("id", 1), Equal("id", 2)),
			add:      func(c *Condition) { c.Add(And(Equal("id", 3), Equal("name", "Blockwatch"))) },
			expected: "id = 1 AND id = 2 AND id = 3 AND name = Blockwatch",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.add(&tt.cond)
			assert.Equal(t, tt.expected, tt.cond.String())
		})
	}
}

// TestConditionClear tests the clearing of condition state.
// Verifies that all fields are reset to zero values and children are removed.
func TestConditionClear(t *testing.T) {
	tests := []struct {
		name string
		cond Condition
	}{
		{
			name: "Simple",
			cond: Equal("id", 1),
		},
		{
			name: "Nested",
			cond: And(
				Equal("id", 1),
				Or(
					Equal("name", "Blockwatch"),
					Gt("score", 4.5),
				),
			),
		},
		{
			name: "Empty",
			cond: Condition{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.cond.Clear()
			assert.True(t, tt.cond.IsEmpty())
		})
	}
}

// TestConditionString tests string representation of conditions.
// Verifies correct formatting of simple and complex conditions for debugging and logging.
func TestConditionString(t *testing.T) {
	tests := []struct {
		name     string
		cond     Condition
		expected string
	}{
		{
			name:     "Simple Equal Condition",
			cond:     Equal("id", 1),
			expected: "id = 1",
		},
		{
			name:     "Range Condition",
			cond:     Range("score", 3.5, 4.5),
			expected: "score RANGE [3.5, 4.5]",
		},
		{
			name: "In Condition with Many Values",
			cond: In("status", []uint16{1, 2, 3, 4, 5, 6, 7, 8, 9, 10,
				11, 12, 13, 14, 15, 16, 17}),
			expected: "status IN [17 values]",
		},
		{
			name: "Complex AND Condition",
			cond: And(
				Equal("id", 1),
				Gt("score", 4.5),
				Regexp("name", "Block.*"),
			),
			expected: "id = 1 AND score > 4.5 AND name ~= Block.*",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.cond.String())
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
			assert.Equal(t, tt.want, cond.String())
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
		{"Single", 1},
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
