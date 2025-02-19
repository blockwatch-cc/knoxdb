// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package query

import (
	"fmt"
	"strings"

	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/schema"
	"blockwatch.cc/knoxdb/pkg/slicex"
)

const (
	COND_OR  = true
	COND_AND = false
)

type RangeValue [2]any

// Condition represents a tree of user-defined query filters
type Condition struct {
	Name     string      // schema field name
	Mode     FilterMode  // eq|ne|gt|ge|lt|le|in|ni|rg|re
	Value    any         // typed value ([2]any for range)
	OrKind   bool        // true to represent all children are ORed
	Children []Condition // child conditions
}

func (c *Condition) Clear() {
	*c = Condition{}
}

func (c Condition) IsEmpty() bool {
	return len(c.Children) == 0 && c.Value == nil && c.Name == ""
}

func (c Condition) IsLeaf() bool {
	return len(c.Children) == 0
}

// returns unique list of fields
func (c Condition) Fields() []string {
	if c.IsEmpty() {
		return nil
	}
	if c.IsLeaf() {
		if c.Name == "" {
			return nil
		}
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
	c.Mode = types.ParseFilterMode(mode)
	if !c.Mode.IsValid() {
		err = fmt.Errorf("invalid filter mode '%s'", mode)
		return
	}
	c.Name = field.Name()
	var enum *schema.EnumDictionary
	if s.HasEnums() {
		enum, _ = s.Enums().Lookup(c.Name)
	}
	parser := schema.NewParser(field.Type(), field.Scale(), enum)
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

func (c Condition) Validate() error {
	return c.validate(true)
}

func (c Condition) validate(isRoot bool) error {
	// empty root is ok, but empty branch/child is not
	if c.IsEmpty() {
		if isRoot {
			return nil
		}
		return fmt.Errorf("empty non-root condition")
	}

	if c.IsLeaf() {
		if c.Name == "" {
			return fmt.Errorf("empty field name")
		}
		if !c.Mode.IsValid() {
			return fmt.Errorf("invalid filter mode")
		}
		if c.Value == nil {
			return fmt.Errorf("nil filter value")
		}
	} else {
		for i := range c.Children {
			if err := c.Children[i].validate(false); err != nil {
				return err
			}
		}
	}
	return nil
}

// translate condition to filter operator
func (c Condition) Compile(s *schema.Schema) (*FilterTreeNode, error) {
	// validate condition field invariants
	if err := c.Validate(); err != nil {
		return nil, err
	}

	// empty root condition produces an always true match
	if c.IsEmpty() {
		node := &FilterTreeNode{
			Children: []*FilterTreeNode{
				{
					Filter: &Filter{
						Name:    s.Pk().Name(),
						Type:    BlockTypes[s.Pk().Type()],
						Mode:    FilterModeTrue,
						Index:   uint16(s.PkIndex()),
						Value:   nil,
						Matcher: &noopMatcher{},
					},
				},
			},
		}
		return node, nil
	}

	// bind leaf node condition
	if c.IsLeaf() {
		// lookup field and fill missing values
		field, ok := s.FieldByName(c.Name)
		if !ok {
			return nil, fmt.Errorf("unknown column %q", c.Name)
		}
		fid, ok := s.FieldIndexById(field.Id())
		if !ok {
			return nil, fmt.Errorf("unknown column %q", c.Name)
		}
		typ := field.Type()

		// Use matcher factory to generate matcher impl for type and mode
		matcher := NewFactory(typ).New(c.Mode)

		// Cast types of condition values since we allow external use.
		// The wire format code path is safe because data encoding follows
		// schema field types.
		var enum *schema.EnumDictionary
		if s.HasEnums() {
			enum, _ = s.Enums().Lookup(c.Name)
		}
		caster := schema.NewCaster(typ, enum)

		// init matcher impl from value(s)
		var (
			node *FilterTreeNode
			err  error
		)
		switch c.Mode {
		case FilterModeIn, FilterModeNotIn:
			switch BlockTypes[typ] {
			case BlockFloat64, BlockFloat32, BlockBool, BlockInt128, BlockInt256:
				// special case for unsupported IN/NI block types
				// we rewrite IN -> OR(EQ) and NIN -> AND(NE) subtrees
				n := reflectSliceLen(c.Value)
				node = &FilterTreeNode{
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
					matcher := NewFactory(typ).New(mode)
					matcher.WithValue(val)
					node.Children[i] = &FilterTreeNode{
						Filter: &Filter{
							Name:    c.Name,
							Type:    BlockTypes[typ],
							Mode:    mode,
							Index:   uint16(fid),
							Value:   val,
							Matcher: matcher,
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
				Children: []*FilterTreeNode{
					{
						Filter: &Filter{
							Name:    c.Name,
							Type:    BlockTypes[typ],
							Mode:    c.Mode,
							Index:   uint16(fid),
							Value:   c.Value,
							Matcher: matcher,
						},
					},
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
	for i := range c.Children {
		cc, err := c.Children[i].Compile(s)
		if err != nil {
			return nil, err
		}
		node.Children = append(node.Children, cc)
	}
	return node, nil
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
	if c.IsEmpty() {
		*c = a
		return
	}

	// determine OR kind polarity change
	if c.OrKind != a.OrKind {
		// push down leaf/branch on flip
		clone := *c
		c.Clear()
		c.OrKind = a.OrKind
		c.Children = []Condition{clone}
	} else if c.IsLeaf() {
		// convert leaf to branch node
		kind := c.OrKind
		clone := *c
		c.Clear()
		c.OrKind = kind
		c.Children = []Condition{clone}
	}

	// append new condition to this node
	if a.IsLeaf() {
		c.Children = append(c.Children, a)
	} else {
		c.Children = append(c.Children, a.Children...)
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
