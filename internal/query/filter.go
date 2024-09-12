// Copyright (c) 2023 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package query

import (
	"errors"
	"fmt"

	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/bitmap"
	"blockwatch.cc/knoxdb/pkg/slicex"
	"blockwatch.cc/knoxdb/pkg/util"
)

var (
	ErrNoName    = errors.New("missing field name")
	ErrNoMode    = errors.New("invalid filter mode")
	ErrNoMatcher = errors.New("missing matcher")
	ErrNoValue   = errors.New("missing value")
)

type FilterMode = types.FilterMode

const (
	FilterModeInvalid  = types.FilterModeInvalid
	FilterModeEqual    = types.FilterModeEqual
	FilterModeNotEqual = types.FilterModeNotEqual
	FilterModeGt       = types.FilterModeGt
	FilterModeGe       = types.FilterModeGe
	FilterModeLt       = types.FilterModeLt
	FilterModeLe       = types.FilterModeLe
	FilterModeIn       = types.FilterModeIn
	FilterModeNotIn    = types.FilterModeNotIn
	FilterModeRange    = types.FilterModeRange
	FilterModeRegexp   = types.FilterModeRegexp
)

type Filter struct {
	Name    string     // schema field name
	Type    BlockType  // block type (we need for opimizing filter trees)
	Mode    FilterMode // eq|ne|gt|gte|lt|lte|rg|in|nin|re
	Index   uint16     // schema field id
	Matcher Matcher    // encapsulated match data and function
	Value   any        // direct val for eq|ne|gt|ge|lt|le, [2]any for rg, nil for in|nin|re
}

func (f Filter) Weight() int {
	return f.Matcher.Weight()
}

func (f Filter) String() string {
	return fmt.Sprintf("%s[%d] %s %s",
		f.Name, f.Index, f.Mode.Symbol(), util.ToString(f.Value))
}

func (f Filter) Validate() error {
	if f.Name == "" {
		return ErrNoName
	}
	if !f.Mode.IsValid() {
		return ErrNoMode
	}
	if f.Matcher == nil {
		return ErrNoMatcher
	}
	if f.Value == nil {
		return ErrNoValue
	}
	return nil
}

type FilterTreeNode struct {
	OrKind   bool              // AND|OR
	Children []*FilterTreeNode // sub filter
	Filter   *Filter           // ptr to condition
	Bits     bitmap.Bitmap     // index scan result
	Skip     bool              // sub-tree or leaf filter has been processed already
	Empty    bool              // index result is empty
}

func (n FilterTreeNode) IsEmpty() bool {
	return len(n.Children) == 0 && n.Filter == nil && !n.Bits.IsValid()
}

func (n FilterTreeNode) IsLeaf() bool {
	return n.Filter != nil
}

func (n FilterTreeNode) IsProcessed() bool {
	if n.IsLeaf() {
		return n.Skip
	}

	for _, v := range n.Children {
		if !v.IsProcessed() {
			return false
		}
	}
	return true
}

func (n FilterTreeNode) IsEmptyMatch() bool {
	if n.IsEmpty() {
		return false
	}

	if n.IsLeaf() {
		return n.Empty
	}

	if n.Bits.IsValid() {
		return n.Bits.Count() == 0
	}

	if n.OrKind {
		for _, v := range n.Children {
			if !v.IsEmptyMatch() {
				return false
			}
		}
		return true
	} else {
		for _, v := range n.Children {
			if v.IsEmptyMatch() {
				return true
			}
		}
		return false
	}
}

func (n FilterTreeNode) Validate(pos string) error {
	if n.IsLeaf() {
		if n.Filter == nil {
			return fmt.Errorf("[%s] missing filter on leaf", pos)
		}
		if err := n.Filter.Validate(); err != nil {
			return fmt.Errorf("[%s] %s: %v", pos, n.Filter.Name, err)
		}
	}

	for i, child := range n.Children {
		if err := child.Validate(fmt.Sprintf("%s/%d", pos, i)); err != nil {
			return err
		}
	}

	return nil
}

func (n FilterTreeNode) Fields() []string {
	if n.IsEmpty() {
		return nil
	}
	if n.IsLeaf() {
		return []string{n.Filter.Name}
	}
	names := make([]string, 0)
	for _, v := range n.Children {
		names = append(names, v.Fields()...)
	}
	return slicex.UniqueStrings(names)
}

// Size returns the total number of condition leaf nodes
func (n FilterTreeNode) Size() int {
	if n.IsLeaf() {
		return 1
	}
	l := 0
	for _, v := range n.Children {
		l += v.Size()
	}
	return l
}

// Depth returns the max number of tree levels
func (n FilterTreeNode) Depth() int {
	return n.depth(0)
}

func (n FilterTreeNode) depth(level int) int {
	if n.IsEmpty() {
		return level
	}
	if n.IsLeaf() {
		return level + 1
	}
	d := level + 1
	for _, v := range n.Children {
		d = util.Max(d, v.depth(level+1))
	}
	return d
}

// returns the decision tree size (including sub-conditions)
func (n FilterTreeNode) Weight() int {
	if n.Bits.IsValid() {
		return 0
	}
	if n.IsLeaf() {
		return n.Filter.Weight()
	}
	w := 0
	for _, v := range n.Children {
		w += v.Weight()
	}
	return w
}

// returns the subtree execution cost based on the number of rows
// that may be visited in the given pack for a full scan times the
// number of comparisons
func (n FilterTreeNode) Cost(nValues int) int {
	return n.Weight() * nValues
}

func (n *FilterTreeNode) AddAndFilter(c *Filter) {
	node := &FilterTreeNode{
		OrKind: COND_AND,
		Filter: c,
	}
	n.AddNode(node)
}

func (n *FilterTreeNode) AddOrFilter(c *Filter) {
	node := &FilterTreeNode{
		OrKind: COND_OR,
		Filter: c,
	}
	n.AddNode(node)
}

// Invariants
// - root is always and AND node
// - root is never a leaf node
// - root may be empty
func (n *FilterTreeNode) AddNode(node *FilterTreeNode) {
	if n.IsLeaf() {
		clone := &FilterTreeNode{
			OrKind:   n.OrKind,
			Children: n.Children,
			Filter:   n.Filter,
		}
		n.Filter = nil
		n.Children = []*FilterTreeNode{clone}
	}

	// append new condition to this element
	if n.OrKind == node.OrKind && !node.IsLeaf() {
		n.Children = append(n.Children, node.Children...)
	} else {
		n.Children = append(n.Children, node)
	}
}
