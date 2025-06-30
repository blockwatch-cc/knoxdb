// Copyright (c) 2023-2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package filter

import (
	"fmt"
	"strings"

	"blockwatch.cc/knoxdb/internal/engine"
	"blockwatch.cc/knoxdb/internal/xroar"
	"blockwatch.cc/knoxdb/pkg/slicex"
	"blockwatch.cc/knoxdb/pkg/util"
)

// type FilterFlags byte

// const (
//  FilterFlagIsOr  FilterFlags = 1 << iota // or kind
//  FilterFlagCanSkip                       // processed, may skip
//  FilterFlagIsIndexResult                 // index scan result
//  FilterFlagUseBloom                      // leaf node may use bloom filter match
// )

// Invariants
// - root is always an AND node
// - root is never a leaf node
// - root may not be empty (no children)
type Node struct {
	Children []*Node       // sub filter
	Filter   *Filter       // ptr to condition
	Bits     *xroar.Bitmap // index scan result
	OrKind   bool          // AND|OR
	Skip     bool          // sub-tree or leaf filter has been processed already

	// Flags FilterFlags // lifecycle flags
}

func NewNode() *Node {
	return &Node{}
}

func (n *Node) SetFilter(f *Filter) *Node {
	n.Filter = f
	n.Children = nil
	return n
}

func (n *Node) AddChild(c *Node) *Node {
	n.Children = append(n.Children, c)
	return n
}

func (n *Node) AddLeaf(f *Filter) *Node {
	n.Children = append(n.Children, NewNode().SetFilter(f))
	return n
}

func (n *Node) SetOr(b bool) *Node {
	n.OrKind = b
	return n
}

func (n *Node) IsLeaf() bool {
	return n.Filter != nil && len(n.Children) == 0
}

func (n *Node) IsProcessed() bool {
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
func (n *Node) IsAnyMatch() bool {
	return n.IsLeaf() && n.Filter.Mode == FilterModeTrue
}

// filter tree is a contradiction (i.e. also when index match was found)
func (n *Node) IsNoMatch() bool {
	return n.IsLeaf() && n.Filter.Mode == FilterModeFalse
}

func (n *Node) Validate(pos string) error {
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
func (n *Node) Fields() []string {
	if n.IsLeaf() {
		return []string{n.Filter.Name}
	}
	names := make([]string, 0)
	for _, v := range n.Children {
		names = append(names, v.Fields()...)
	}
	return slicex.UniqueStrings(names)
}

// returns a unique ordered list of field ids
func (n *Node) FieldIds() []uint16 {
	if n.IsLeaf() {
		return []uint16{n.Filter.Id}
	}
	ids := make([]uint16, 0)
	for _, v := range n.Children {
		ids = append(ids, v.FieldIds()...)
	}
	return slicex.Unique(ids)
}

// Indexes returns a unique ordered list of field indexes referenced by
// filters in this tree.
// func (n *Node) Indexes() []int {
//  ord := slicex.NewOrderedIntegers(make([]int, 0)).SetUnique()
//  n.collectIndexes(ord)
//  return ord.Values
// }

func (n *Node) collectIndexes(s *slicex.OrderedIntegers[int]) {
	if n.IsLeaf() {
		s.Insert(n.Filter.Index)
		return
	}
	for _, v := range n.Children {
		v.collectIndexes(s)
	}
}

// Size returns the total number of condition leaf nodes
func (n *Node) Size() int {
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
func (n *Node) Depth() int {
	return n.depth(0)
}

func (n *Node) depth(level int) int {
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
func (n *Node) Weight() int {
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
func (n *Node) Cost(nValues int) int {
	return n.Weight() * nValues
}

// engine matcher interface
// func (n *Node) MatchView(v *schema.View) bool {
//  return MatchTree(n, v)
// }

func (n *Node) Overlaps(v engine.ConditionMatcher) bool {
	_, ok := v.(*Node)
	if !ok {
		return false
	}
	// TODO: required for LockManager predicate locks
	return false
}

// ForEach visits each filter in the tree
func (n *Node) ForEach(fn func(*Filter) error) error {
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

func (n *Node) String() string {
	var b strings.Builder
	n.WriteString(0, &b)
	return b.String()
}

func (n *Node) WriteString(level int, w *strings.Builder) {
	if n.IsLeaf() {
		fmt.Fprint(w, n.Filter.String())
		if n.Skip {
			fmt.Fprint(w, " [SKIP] ")
		}
	}
	kind := " AND "
	if n.OrKind {
		kind = " OR "
	}
	if level > 0 && len(n.Children) > 0 {
		fmt.Fprint(w, " ( ")
		defer fmt.Fprint(w, " ) ")
	}
	for i, v := range n.Children {
		if i > 0 {
			fmt.Fprint(w, kind)
		}
		v.WriteString(level+1, w)
	}
}
