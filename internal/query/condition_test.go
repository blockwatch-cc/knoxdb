// Copyright (c) 2024 Blockwatch Data Inc.
// Author: oliver@blockwatch.cc

package query

import (
	"fmt"
	"reflect"
	"strings"
	"testing"

	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/schema"
)

func TestConditionParse(t *testing.T) {
	s := createTestSchema()

	tests := []struct {
		name     string
		key      string
		val      string
		expected Condition
		wantErr  bool
	}{
		{"Equal Integer", "id", "123", Condition{Name: "id", Type: types.FieldTypeInt64, Index: 0, Mode: FilterModeEqual, Value: int64(123)}, false},
		{"Greater Than Float", "score.gt", "4.5", Condition{Name: "score", Type: types.FieldTypeFloat64, Index: 1, Mode: FilterModeGt, Value: 4.5}, false},
		{"String Contains", "name.re", "Blockwatch", Condition{Name: "name", Type: types.FieldTypeString, Index: 2, Mode: FilterModeRegexp, Value: "Blockwatch"}, false},
		{"Date Range", "created.rg", "2023-01-01,2023-12-31", Condition{Name: "created", Type: types.FieldTypeDatetime, Index: 3, Mode: FilterModeRange, Value: RangeValue{int64(1672531200000000000), int64(1703980800000000000)}}, false},
		{"Enum In", "status.in", "1,2", Condition{Name: "status", Type: types.FieldTypeUint16, Index: 4, Mode: FilterModeIn, Value: []uint16{1, 2}}, false},
		{"Enum In (String Values)", "status.in", "active,pending", Condition{}, true},
		{"Invalid Field", "nonexistent", "value", Condition{}, true},
		{"Invalid Mode", "id.invalid", "123", Condition{}, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseCondition(tt.key, tt.val, s)
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

func TestConditionCompile(t *testing.T) {
	s := createTestSchema()

	tests := []struct {
		name    string
		c       Condition
		wantErr bool
	}{
		{
			name: "Simple Equal",
			c:    Condition{Name: "id", Mode: FilterModeEqual, Value: int64(123)},
		},
		{
			name: "Range Condition",
			c:    Condition{Name: "score", Mode: FilterModeRange, Value: RangeValue{3.5, 4.5}},
		},
		{
			name: "Enum In",
			c:    Condition{Name: "status", Mode: FilterModeIn, Value: []uint16{1, 2}},
		},
		{
			name:    "Invalid Field",
			c:       Condition{Name: "invalid", Mode: FilterModeEqual, Value: 123},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := tt.c.Compile(s)
			if (err != nil) != tt.wantErr {
				t.Errorf("Condition.Compile() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

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
			expected: "id = 1\nname = Blockwatch",
		},
		{
			name:     "Add OR Condition",
			initial:  Or(Equal("id", 1), Equal("name", "Blockwatch")),
			add:      Gt("age", 6),
			expected: "id = 1\nname = Blockwatch\nage > 6",
		},
		{
			name:     "Add to Empty Condition",
			initial:  Condition{},
			add:      Equal("id", 123),
			expected: "id = 123",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.initial.Add(tt.add)
			result := conditionToString(tt.initial)
			if result != tt.expected {
				t.Errorf("After Add(), condition = %v, want %v", result, tt.expected)
			}
		})
	}
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

func createTestSchema() *schema.Schema {
	statusEnum := schema.NewEnumDictionary("status")
	statusEnum.Append("active", "pending", "inactive")

	return schema.NewSchema().
		WithName("test").
		WithField(schema.NewField(types.FieldTypeInt64).WithName("id").WithFlags(types.FieldFlagPrimary)).
			WithField(schema.NewField(types.FieldTypeFloat64).WithName("score")).
			WithField(schema.NewField(types.FieldTypeString).WithName("name")).
			WithField(schema.NewField(types.FieldTypeDatetime).WithName("created")).
			WithField(schema.NewField(types.FieldTypeUint16).WithName("status").WithFlags(types.FieldFlagEnum)).
			WithEnum(statusEnum).
			Finalize()
}

func conditionEqual(a, b Condition) bool {
	if a.Name != b.Name || a.Type != b.Type || a.Index != b.Index || a.Mode != b.Mode || a.OrKind != b.OrKind {
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

// Add this new helper function to convert a Condition to a string representation
func conditionToString(c Condition) string {
	if len(c.Children) > 0 {
		childStrings := make([]string, len(c.Children))
		for i, child := range c.Children {
			childStrings[i] = conditionToString(child)
		}
		return strings.Join(childStrings, "\n")
	}

	return fmt.Sprintf("%s %s %v", c.Name, c.Mode.Symbol(), c.Value)
}
