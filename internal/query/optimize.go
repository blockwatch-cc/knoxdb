// Copyright (c) 2023 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package query

import (
	"reflect"
	"sort"

	"blockwatch.cc/knoxdb/internal/cmp"
	"blockwatch.cc/knoxdb/pkg/slicex"
)

// Optimize filter conditions by removing or replacing them with semantically
// equal but less costly to check conditions
//
// - remove skip nodes
// - lift/merge single child nodes
// - lift/merge nested nodes of same kind
//   - OR ( OR (A, B), C) ) => OR (A, B, C)
//   - AND ( AND (A, B), C) => AND (A, B, C)
//
// - replace/simplify
//   - any: LE(A) + GE(A) => RG(A)
//   - any: IN(single A) => EQ(A)
//   - any: NI(single A) => NE(A)
//   - and-only: LT|LE(A) + LT|LE(A) => LT|LE(A) -- minimum
//   - and-only: GT|GE(A) + GT|GE(A) => GT|GE(A) -- maximum
//   - and-only: IN(A) + IN(A) => IN(A) -- intersect (handle empty case)
//   - and-only: NI(A) + NI(A) => NI(A) -- union! (does work for OR!!!)
//   - or-only: IN(A) + IN(A) => IN(A) -- union
//   - or-only: EQ(A) + EQ(A) => IN(A)
//   - or-only: NE(A) + NE(A) => NI(A)
//
// - TODO: mark illogical conditions empty
//   - LT|LE(uint, 0)
//   - empty IN
//   - RG from>to
func (n *FilterTreeNode) Optimize() {
	// stop at leaf nodes
	if n.IsLeaf() {
		return
	}

	// work bottom up
	for i := range n.Children {
		n.Children[i].Optimize()
	}

	newChilds := make([]*FilterTreeNode, 0, len(n.Children))

	// remove or lift child nodes
	for _, child := range n.Children {
		// skip processed nodes
		if child.Skip {
			continue
		}

		// keep leaf nodes
		if child.IsLeaf() {
			newChilds = append(newChilds, child)
		}

		// skip empty children
		if len(child.Children) == 0 {
			continue
		}

		// lift nested single-child node's child
		if len(child.Children) == 1 {
			newChilds = append(newChilds, child.Children[1])
			continue
		}

		// lift nested child node's children of same kind
		if n.OrKind == child.OrKind {
			newChilds = append(newChilds, child.Children...)
			continue
		}

		// keep nested child node as is
		newChilds = append(newChilds, child)
	}

	// merge/simplify child nodes
	newChilds = simplifyNodes(newChilds, n.OrKind)

	// sort by weight
	sort.Slice(newChilds, func(i, j int) bool {
		return newChilds[i].Weight() < newChilds[j].Weight()
	})

	// replace current children
	n.Children = newChilds
}

func simplifyNodes(nodes []*FilterTreeNode, orKind bool) []*FilterTreeNode {
	// split leafs from nested nodes
	nested, leafs, ok := slicex.CutFunc(nodes, func(n *FilterTreeNode) bool {
		return !n.IsLeaf()
	})

	// nothing to do if there are no leafs
	if !ok {
		return nodes
	}

	// order leafs by field index
	sort.Slice(leafs, func(i, j int) bool {
		return leafs[i].Filter.Index < leafs[j].Filter.Index
	})

	// first apply simplifications that apply to any kind
	leafs = simplifyAnyNodes(leafs)

	// next apply special simplifications that depend on the aggregation type
	if orKind {
		leafs = simplifyOrNodes(leafs)
	} else {
		leafs = simplifyAndNodes(leafs)
	}

	// TODO: mark illogical conditions empty
	//   - LT|LE(uint, 0)
	//   - empty IN
	//   - RG from>to

	// return the optimized leafs combined with nested nodes
	return append(leafs, nested...)
}

// Simplifications for any kind (and|or)
//   - any: LE(A) + GE(A) => RG(A)
//   - any: IN(single A) => EQ(A)
//   - any: NI(single A) => NE(A)
func simplifyAnyNodes(nodes []*FilterTreeNode) []*FilterTreeNode {
	var (
		le, ge   *FilterTreeNode
		lastId   uint16
		needSkip bool
	)
	for _, node := range nodes {
		f := node.Filter

		// reset when field id changes
		if lastId != f.Index {
			lastId = f.Index
			le, ge = nil, nil
		}

		// we decide based on filter mode
		switch f.Mode {
		case FilterModeLe:
			le = node
		case FilterModeGe:
			ge = node
		case FilterModeIn:
			// update inplace
			if f.Matcher.Len() == 1 {
				f.Mode = FilterModeEqual
				f.Value = reflectSliceIndex(f.Matcher.Value(), 0)
				f.Matcher = newFactory(f.Type).
					New(FilterModeEqual).
					WithValue(f.Value)
			}
			continue
		case FilterModeNotIn:
			// update inplace
			if f.Matcher.Len() == 1 {
				f.Mode = FilterModeNotEqual
				f.Value = reflectSliceIndex(f.Matcher.Value(), 0)
				f.Matcher = newFactory(f.Type).
					New(FilterModeNotEqual).
					WithValue(f.Value)
			}
			continue
		}

		// combine ranges if possible; based on our selection method
		// above this depends on how same-field conditions are ordered
		// for the sake of limited complexity we support simple cases only
		if le != nil && ge != nil {
			// LE+GE can only form a range when LE.value >= GE.value
			if cmp.GE(le.Filter.Type, le.Filter.Value, ge.Filter.Value) {
				le.Skip = true
				ge.Skip = true
				val := RangeValue{ge.Filter.Value, le.Filter.Value}
				f := le.Filter
				nodes = append(nodes, &FilterTreeNode{
					Filter: &Filter{
						Name:  f.Name,
						Type:  f.Type,
						Mode:  FilterModeRange,
						Index: f.Index,
						Matcher: newFactory(f.Type).
							New(FilterModeRange).
							WithValue(val),
						Value: val,
					},
				})
				le = nil
				ge = nil
				needSkip = true
			}
		}
	}

	// filter out all replaced nodes
	if needSkip {
		nodes, _, _ = slicex.CutFunc(nodes, func(n *FilterTreeNode) bool {
			return !n.Skip
		})
	}

	return nodes
}

// Simplifications that apply to AND nodes only
//   - and-only: LT|LE(A) + LT|LE(A) => LT|LE(A) -- minimum
//   - and-only: GT|GE(A) + GT|GE(A) => GT|GE(A) -- maximum
//   - and-only: IN(A) + IN(A) => IN(A) -- intersect (handle empty case)
//   - and-only: NI(A) + NI(A) => NI(A) -- union! (does not work for OR!!!)
func simplifyAndNodes(nodes []*FilterTreeNode) []*FilterTreeNode {
	var (
		le, ge, lt, gt, in, ni *FilterTreeNode
		lastId                 uint16
		needSkip               bool
	)
	for _, node := range nodes {
		f := node.Filter

		// reset when field id changes
		if lastId != f.Index {
			lastId = f.Index
			le, ge, lt, gt, in, ni = nil, nil, nil, nil, nil, nil
		}

		// rewrite node filter and skip second node on match,
		// aggregates multiple occurences of same type filters
		// as long as they are for the same field
		switch f.Mode {
		case FilterModeLe:
			if le != nil {
				f.Value = cmp.Min(f.Type, f.Value, le.Filter.Value)
				f.Matcher.WithValue(f.Value)
				le.Skip = true
				needSkip = true
			}
			le = node
		case FilterModeLt:
			if lt != nil {
				f.Value = cmp.Min(f.Type, f.Value, lt.Filter.Value)
				f.Matcher.WithValue(f.Value)
				lt.Skip = true
				needSkip = true
			}
			lt = node
		case FilterModeGe:
			if ge != nil {
				f.Value = cmp.Max(f.Type, f.Value, ge.Filter.Value)
				f.Matcher.WithValue(f.Value)
				ge.Skip = true
				needSkip = true
			}
			ge = node
		case FilterModeGt:
			if gt != nil {
				f.Value = cmp.Max(f.Type, f.Value, gt.Filter.Value)
				f.Matcher.WithValue(f.Value)
				gt.Skip = true
				needSkip = true
			}
			gt = node
		case FilterModeIn:
			if in != nil {
				f.Value = cmp.Intersect(f.Type, f.Value, in.Filter.Value)
				f.Matcher.WithSlice(f.Value)
				in.Skip = true
				needSkip = true
			}
			in = node
		case FilterModeNotIn:
			if ni != nil {
				f.Value = cmp.Union(f.Type, f.Value, ni.Filter.Value)
				f.Matcher.WithSlice(f.Value)
				ni.Skip = true
				needSkip = true
			}
			ni = node
		}
	}

	// filter out all replaced nodes
	if needSkip {
		nodes, _, _ = slicex.CutFunc(nodes, func(n *FilterTreeNode) bool {
			return !n.Skip
		})
	}

	return nodes
}

// Simplifications that apply to OR nodes only
//   - or-only: IN(A) + IN(A) => IN(A) -- union
//   - or-only: IN(A) + EQ(A) => IN(A)
//   - or-only: EQ(A) + EQ(A) => IN(A)
func simplifyOrNodes(nodes []*FilterTreeNode) []*FilterTreeNode {
	var (
		eq, in   *FilterTreeNode
		lastId   uint16
		needSkip bool
	)
	for _, node := range nodes {
		f := node.Filter

		// reset when field id changes
		if lastId != f.Index {
			lastId = f.Index
			eq, in = nil, nil
		}

		// rewrite node filter and skip second node on match,
		// aggregates multiple occurences of same type filters
		// as long as they are for the same field
		switch f.Mode {
		case FilterModeIn:
			switch {
			case in != nil:
				f.Value = cmp.Union(f.Type, f.Value, in.Filter.Value)
				f.Matcher.WithSlice(f.Value)
				in.Skip = true
				needSkip = true
				in = node
			case eq != nil && in != nil:
				// append to existing in condition
				in.Filter.Value = reflectSliceAppend(in.Filter.Value, f.Value)
				in.Filter.Matcher.WithSlice(in.Filter.Value)
				node.Skip = true
				needSkip = true
			case eq != nil && in == nil:
				// convert eq to in, drop other eq
				f.Mode = FilterModeIn
				f.Value = reflectSliceMake(eq.Filter.Value, f.Value)
				f.Matcher = newFactory(f.Type).
					New(FilterModeIn).
					WithValue(f.Value)
				eq.Skip = true
				needSkip = true
				eq = nil
				in = node
			}
		case FilterModeEqual:
			switch {
			case in != nil:
				// append to existing in condition
				in.Filter.Value = reflectSliceAppend(in.Filter.Value, f.Value)
				in.Filter.Matcher.WithSlice(in.Filter.Value)
				node.Skip = true
				needSkip = true
			case eq != nil:
				// convert eq to in and add value from other eq
				f.Mode = FilterModeIn
				f.Value = reflectSliceMake(eq.Filter.Value, f.Value)
				f.Matcher = newFactory(f.Type).
					New(FilterModeIn).
					WithValue(f.Value)
				eq.Skip = true
				needSkip = true
				eq = nil
				in = node
			}
		}
	}

	// filter out all replaced nodes
	if needSkip {
		nodes, _, _ = slicex.CutFunc(nodes, func(n *FilterTreeNode) bool {
			return !n.Skip
		})
	}

	return nodes
}

func reflectSliceIndex(slice any, index int) any {
	return reflect.ValueOf(slice).Index(index).Interface()
}

func reflectSliceAppend(a, b any) any {
	return reflect.Append(reflect.ValueOf(a), reflect.ValueOf(b)).Interface()
}

func reflectSliceMake(a, b any) any {
	slice := reflect.MakeSlice(reflect.TypeOf(a), 0, 2)
	slice = reflect.Append(slice, reflect.ValueOf(a), reflect.ValueOf(b))
	return slice.Interface()
}

func reflectSliceLen(s any) int {
	return reflect.ValueOf(s).Len()
}
