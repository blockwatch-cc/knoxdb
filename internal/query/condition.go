// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package query

import (
	"fmt"
	"reflect"
	"strings"

	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/schema"
	"blockwatch.cc/knoxdb/pkg/slicex"
	"blockwatch.cc/knoxdb/pkg/util"
)

const (
	COND_OR  = true
	COND_AND = false
)

type RangeValue [2]any

// Condition represents a tree of user-defined query filters
type Condition struct {
	Name     string          // schema field name
	Type     types.FieldType // schema field type
	Index    uint16          // schema field id
	Mode     FilterMode      // eq|ne|gt|ge|lt|le|in|ni|rg|re
	Value    any             // typed value ([2]any for range)
	OrKind   bool            // true to represent all children are ORed
	Children []Condition     // child conditions
}

func ParseCondition(key, val string, s *schema.Schema) (c Condition, err error) {
	name, mode, ok := strings.Cut(key, ".")
	if !ok {
		mode = "eq"
	}
	field, ok := s.FieldByName(name)
	if !ok {
		err = fmt.Errorf("unknown column %q", name)
		return
	}
	c.Name = field.Name()
	c.Type = field.Type()
	c.Index = field.Id() - 1
	c.Mode = types.ParseFilterMode(mode)
	if !c.Mode.IsValid() {
		err = fmt.Errorf("invalid filter mode '%s'", mode)
		return
	}
	parser := schema.NewParser(c.Type, field.Scale())
	switch c.Mode {
	case FilterModeRange:
		v1, v2, ok := strings.Cut(val, ",")
		if ok {
			var res RangeValue
			res[0], err = parser.ParseValue(v1)
			if err == nil {
				res[1], err = parser.ParseValue(v2)
			}
			c.Value = res
		} else {
			err = fmt.Errorf("range conditions require exactly two arguments")
			return
		}
	case FilterModeIn, FilterModeNotIn:
		c.Value, err = parser.ParseSlice(val)
	default:
		c.Value, err = parser.ParseValue(val)
	}
	if err != nil {
		err = fmt.Errorf("error parsing condition value '%s': %v", val, err)
	}
	return
}

// translate condition to filter operator
func (c Condition) Compile(s *schema.Schema) (*FilterTreeNode, error) {
	// bind single leaf node condition
	if c.Name != "" {
		// lookup field and fill missing values
		field, ok := s.FieldByName(c.Name)
		if !ok {
			return nil, fmt.Errorf("unknown column %q", c.Name)
		}
		c.Index = field.Id() - 1
		c.Type = field.Type()

		// Use matcher factory to generate matcher impl for type and mode
		matcher := NewFactory(c.Type).New(c.Mode)

		// Cast types of condition values since we allow external use.
		// The wire format code path is safe because data encoding follows
		// schema field types.
		caster := schema.NewCaster(c.Type)

		// init matcher impl from value(s)
		var (
			node *FilterTreeNode
			err  error
		)
		switch c.Mode {
		case FilterModeIn, FilterModeNotIn:
			switch BlockTypes[c.Type] {
			case BlockFloat64, BlockFloat32, BlockBool, BlockInt128, BlockInt256:
				// special case for unsupported IN/NI block types
				// we rewrite IN -> OR(EQ) and NIN -> AND(NE) subtrees
				n := reflectSliceLen(c.Value)
				node := &FilterTreeNode{
					OrKind:   c.Mode == FilterModeIn,
					Children: make([]*FilterTreeNode, n),
				}
				mode := FilterModeEqual
				if c.Mode == FilterModeNotIn {
					mode = FilterModeNotEqual
				}
				for i := 0; i < n; i++ {
					val := reflectSliceIndex(c.Value, i)
					val, err = caster.CastValue(val)
					if err != nil {
						break
					}
					node.Children[i] = &FilterTreeNode{
						Filter: &Filter{
							Name:    c.Name,
							Type:    BlockTypes[c.Type],
							Mode:    mode,
							Index:   c.Index,
							Value:   val,
							Matcher: NewFactory(c.Type).New(mode).WithValue(val),
						},
					}
				}
			default:
				// ensure slice type matches blocks
				var slice any
				slice, err = caster.CastSlice(c.Value)
				if err == nil {
					matcher.WithSlice(slice)
					c.Value = slice
				}
			}
		case FilterModeRange:
			// ensure range type matches blocks
			var from, to any
			from, err = caster.CastValue(c.Value.(RangeValue)[0])
			if err == nil {
				to, err = caster.CastValue(c.Value.(RangeValue)[1])
				if err == nil {
					c.Value = RangeValue{from, to}
					matcher.WithValue(c.Value)
				}
			}
		default:
			// ensure value type matches blocks
			var val any
			val, err = caster.CastValue(c.Value)
			if err == nil {
				matcher.WithValue(val)
				c.Value = val
			}
		}
		if err != nil {
			return nil, err
		}

		// use the subtree node from rewrite above or create a new child
		// node from matcher
		if node == nil {
			node = &FilterTreeNode{
				Filter: &Filter{
					Name:    c.Name,
					Type:    BlockTypes[c.Type],
					Mode:    c.Mode,
					Index:   c.Index,
					Value:   c.Value,
					Matcher: matcher,
				},
			}
		}

		return node, nil
	}

	// bind children
	node := &FilterTreeNode{
		OrKind:   c.OrKind,
		Children: make([]*FilterTreeNode, 0),
	}
	for _, v := range c.Children {
		cc, err := v.Compile(s)
		if err != nil {
			return nil, err
		}
		node.Children = append(node.Children, cc)
	}
	return node, nil
}

// returns unique list of fields
func (c Condition) Fields() []string {
	if c.IsEmpty() {
		return nil
	}
	if c.IsLeaf() {
		return []string{c.Name}
	}
	names := make([]string, 0)
	for _, v := range c.Children {
		names = append(names, v.Fields()...)
	}
	return slicex.UniqueStrings(names)
}

func (c Condition) Rename(name string) Condition {
	if name != "" {
		c.Name = name
	}
	return c
}

func (c *Condition) Clear() {
	c.Name = ""
	c.Mode = 0
	c.Value = nil
	c.OrKind = false
	c.Children = nil
}

func (c Condition) IsEmpty() bool {
	return len(c.Children) == 0 && !c.Mode.IsValid()
}

func (c Condition) IsLeaf() bool {
	return c.Name != ""
}

func (c Condition) String() string {
	switch c.Mode {
	case FilterModeRange:
		return fmt.Sprintf("%s %s [%s, %s]",
			c.Name,
			c.Mode.Symbol(),
			util.ToString(c.Value.(RangeValue)[0]),
			util.ToString(c.Value.(RangeValue)[1]),
		)
	case FilterModeIn, FilterModeNotIn:
		size := reflect.ValueOf(c.Value).Len()
		if size > 16 {
			return fmt.Sprintf("%s %s [%d values]", c.Name, c.Mode.Symbol(), size)
		} else {
			return fmt.Sprintf("%s %s [%#v]", c.Name, c.Mode.Symbol(), c.Value)
		}
	default:
		return fmt.Sprintf("%s %s %s", c.Name, c.Mode.Symbol(), util.ToString(c.Value))
	}
}

func (c *Condition) And(col string, mode FilterMode, value any) {
	c.Add(Condition{
		Name:   col,
		Mode:   mode,
		Value:  value,
		OrKind: COND_AND,
	})
}

func (c *Condition) AndRange(col string, from, to any) {
	c.Add(Condition{
		Name:   col,
		Mode:   FilterModeRange,
		Value:  RangeValue{from, to},
		OrKind: COND_AND,
	})
}

func (c *Condition) Or(col string, mode FilterMode, value any) {
	c.Add(Condition{
		Name:   col,
		Mode:   mode,
		Value:  value,
		OrKind: COND_OR,
	})
}

func (c *Condition) OrRange(col string, from, to any) {
	c.Add(Condition{
		Name:   col,
		Mode:   FilterModeRange,
		Value:  RangeValue{from, to},
		OrKind: COND_OR,
	})
}

func (c *Condition) Add(a Condition) {
	if a.IsEmpty() {
		return
	}
	if c.IsLeaf() {
		clone := Condition{
			Name:     c.Name,
			Mode:     c.Mode,
			Value:    c.Value,
			OrKind:   c.OrKind,
			Children: c.Children,
		}
		c.Children = []Condition{clone}
	}

	// append new condition to this element
	if c.OrKind == a.OrKind && !a.IsLeaf() {
		c.Children = append(c.Children, a.Children...)
	} else {
		c.Children = append(c.Children, a)
	}
}

func And(conds ...Condition) Condition {
	return Condition{
		Mode:     FilterModeInvalid,
		OrKind:   COND_AND,
		Children: conds,
	}
}

func Or(conds ...Condition) Condition {
	return Condition{
		Mode:     FilterModeInvalid,
		OrKind:   COND_OR,
		Children: conds,
	}
}

func Equal(col string, val any) Condition {
	return Condition{Name: col, Mode: FilterModeEqual, Value: val}
}

func NotEqual(col string, val any) Condition {
	return Condition{Name: col, Mode: FilterModeNotEqual, Value: val}
}

func In(col string, val any) Condition {
	return Condition{Name: col, Mode: FilterModeIn, Value: val}
}

func NotIn(col string, val any) Condition {
	return Condition{Name: col, Mode: FilterModeNotIn, Value: val}
}

func Lt(col string, val any) Condition {
	return Condition{Name: col, Mode: FilterModeLt, Value: val}
}

func Le(col string, val any) Condition {
	return Condition{Name: col, Mode: FilterModeLe, Value: val}
}

func Gt(col string, val any) Condition {
	return Condition{Name: col, Mode: FilterModeGt, Value: val}
}

func Ge(col string, val any) Condition {
	return Condition{Name: col, Mode: FilterModeGe, Value: val}
}

func Regexp(col string, val any) Condition {
	return Condition{Name: col, Mode: FilterModeRegexp, Value: val}
}

func Range(col string, from, to any) Condition {
	return Condition{Name: col, Mode: FilterModeRange, Value: RangeValue{from, to}}
}
