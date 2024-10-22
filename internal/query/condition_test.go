// Copyright (c) 2024 Blockwatch Data Inc.
// Author: oliver@blockwatch.cc

package query

import (
	"reflect"
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

// TestConditionParse tests the ParseCondition function with various input scenarios.
func TestConditionParse(t *testing.T) {
	tests := []struct {
		name     string
		key      string
		val      string
		expected Condition
		wantErr  bool
	}{
		{"Equal Integer", "id", "123", Condition{Name: "id", Type: types.FieldTypeInt64, Mode: FilterModeEqual, Value: int64(123)}, false},
		{"Greater Than Float", "score.gt", "4.5", Condition{Name: "score", Type: types.FieldTypeFloat64, Mode: FilterModeGt, Value: 4.5}, false},
		{"String Contains", "name.re", "Blockwatch", Condition{Name: "name", Type: types.FieldTypeString, Mode: FilterModeRegexp, Value: "Blockwatch"}, false},
		{"Date Range", "created.rg", "2023-01-01,2023-12-31", Condition{Name: "created", Type: types.FieldTypeDatetime, Mode: FilterModeRange, Value: RangeValue{int64(1672531200000000000), int64(1703980800000000000)}}, false},
		{"Enum In", "status.in", "1,2", Condition{Name: "status", Type: types.FieldTypeUint16, Mode: FilterModeIn, Value: []uint16{1, 2}}, false},
		{"Invalid Field", "nonexistent", "value", Condition{}, true},
		{"Invalid Mode", "id.invalid", "123", Condition{}, true},
		{"Empty String", "name", "", Condition{Name: "name", Type: types.FieldTypeString, Mode: FilterModeEqual, Value: ""}, false},
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

// TestConditionAdd tests the Add method of Condition for different addition scenarios.
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
			name:     "Add OR Condition",
			initial:  Or(Equal("id", 1), Equal("name", "Blockwatch")),
			add:      Gt("age", 6),
			expected: "(id = 1 OR name = Blockwatch) AND age > 6",
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
