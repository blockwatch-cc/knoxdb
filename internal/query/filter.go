// Copyright (c) 2023 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package query

import (
	"errors"
	"fmt"

	"blockwatch.cc/knoxdb/internal/engine"
	"blockwatch.cc/knoxdb/pkg/bitmap"
	"blockwatch.cc/knoxdb/pkg/schema"
	"blockwatch.cc/knoxdb/pkg/slicex"
	"blockwatch.cc/knoxdb/pkg/util"
)

var (
	ErrNoName    = errors.New("missing field name")
	ErrNoMode    = errors.New("invalid filter mode")
	ErrNoMatcher = errors.New("missing matcher")
	ErrNoValue   = errors.New("missing value")
)

type Filter struct {
	Name    string     // schema field name
	Type    BlockType  // block type (we need for opimizing filter trees)
	Mode    FilterMode // eq|ne|gt|gte|lt|lte|rg|in|nin|re
	Index   uint16     // field index (NOT field id, index = id - 1!!; compat with pack.Package.Block() and schema.View.Get())
	Matcher Matcher    // encapsulated match data and function
	Value   any        // direct val for eq|ne|gt|ge|lt|le, [2]any for rg, slice for in|nin, string re
}

func (f *Filter) Weight() int {
	return f.Matcher.Weight()
}

func (f *Filter) Validate() error {
	if f.Name == "" {
		return ErrNoName
	}
	if !f.Mode.IsValid() {
		return ErrNoMode
	}
	switch f.Mode {
	case FilterModeTrue, FilterModeFalse:
		// empty matcher or value ok
	default:
		if f.Matcher == nil {
			return ErrNoMatcher
		}
		if f.Value == nil {
			return ErrNoValue
		}
	}
	return nil
}

// type FilterFlags byte

// const (
// 	FilterFlagIsOr  FilterFlags = 1 << iota // or kind
// 	FilterFlagCanSkip                       // processed, may skip
// 	FilterFlagIsIndexResult                 // index scan result
// 	FilterFlagUseBloom                      // leaf node may use bloom filter match
// )

// Invariants
// - root is always an AND node
// - root is never a leaf node
// - root may not be empty (no children)
type FilterTreeNode struct {
	Children []*FilterTreeNode // sub filter
	Filter   *Filter           // ptr to condition
	Bits     bitmap.Bitmap     // index scan result
	OrKind   bool              // AND|OR
	Skip     bool              // sub-tree or leaf filter has been processed already

	// Flags FilterFlags // lifecycle flags
}

func (n *FilterTreeNode) IsLeaf() bool {
	return n.Filter != nil && len(n.Children) == 0
}

func (n *FilterTreeNode) IsProcessed() bool {
	if n.Skip || n.Bits.IsValid() {
		return true
	}
	if n.IsLeaf() {
		return n.Skip || n.Bits.IsValid()
	}
	for _, v := range n.Children {
		if !v.IsProcessed() {
			return false
		}
	}
	return true
}

// filter tree is a tautology, i.e. all possible values match
func (n *FilterTreeNode) IsAnyMatch() bool {
	return n.IsLeaf() && n.Filter.Mode == FilterModeTrue
}

// filter tree is a contradiction (i.e. also when index match was found)
func (n *FilterTreeNode) IsNoMatch() bool {
	return n.IsLeaf() && n.Filter.Mode == FilterModeFalse
}

func (n *FilterTreeNode) Validate(pos string) error {
	// Check if node is invalid (no children and no filter)
	if len(n.Children) == 0 && n.Filter == nil {
		return fmt.Errorf("[%s] invalid leaf node: missing filter", pos)
	}

	// Validate leaf node filter
	if n.IsLeaf() {
		if err := n.Filter.Validate(); err != nil {
			return fmt.Errorf("[%s] %s: %v", pos, n.Filter.Name, err)
		}
	}

	// Validate children nodes recursively
	for i, child := range n.Children {
		if err := child.Validate(fmt.Sprintf("%s/%d", pos, i)); err != nil {
			return err
		}
	}

	return nil
}

// Fields returns a unique ordered list of field names referenced by
// filters in this tree.
func (n *FilterTreeNode) Fields() []string {
	if n.IsLeaf() {
		return []string{n.Filter.Name}
	}
	names := make([]string, 0)
	for _, v := range n.Children {
		names = append(names, v.Fields()...)
	}
	return slicex.UniqueStrings(names)
}

// Indexes returns a unique ordered list of field indexes referenced by
// filters in this tree.
func (n *FilterTreeNode) Indexes() []uint16 {
	ord := slicex.NewOrderedNumbers[uint16](make([]uint16, 0)).SetUnique()
	n.collectIndexes(ord)
	return ord.Values
}

func (n *FilterTreeNode) collectIndexes(s *slicex.OrderedNumbers[uint16]) {
	if n.IsLeaf() {
		s.Insert(n.Filter.Index)
		return
	}
	for _, v := range n.Children {
		v.collectIndexes(s)
	}
}

// Size returns the total number of condition leaf nodes
func (n *FilterTreeNode) Size() int {
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
func (n *FilterTreeNode) Depth() int {
	return n.depth(0)
}

func (n *FilterTreeNode) depth(level int) int {
	if n.IsLeaf() {
		return level
	}
	d := level
	for _, v := range n.Children {
		d = util.Max(d, v.depth(level+1))
	}
	return d
}

// returns the decision tree size (including sub-conditions)
func (n *FilterTreeNode) Weight() int {
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
func (n *FilterTreeNode) Cost(nValues int) int {
	return n.Weight() * nValues
}

// engine matcher interface
func (n *FilterTreeNode) MatchView(v *schema.View) bool {
	return MatchTree(n, v)
}

func (n *FilterTreeNode) Overlaps(v engine.ConditionMatcher) bool {
	_, ok := v.(*FilterTreeNode)
	if !ok {
		return false
	}
	// TODO: required for LockManager predicate locks
	return false
}

// ForEach visits each filter in the tree
func (n *FilterTreeNode) ForEach(fn func(*Filter) error) error {
	if n.IsLeaf() {
		return fn(n.Filter)
	}

	for _, v := range n.Children {
		if err := v.ForEach(fn); err != nil {
			return err
		}
	}
	return nil
}
