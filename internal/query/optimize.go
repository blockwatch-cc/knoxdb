// Copyright (c) 2023-2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package query

import (
	"reflect"
	"slices"
	"sort"

	"blockwatch.cc/knoxdb/internal/cmp"
	"blockwatch.cc/knoxdb/internal/xroar"
	"blockwatch.cc/knoxdb/pkg/slicex"
)

// Optimize filter conditions by removing or replacing them with semantically
// equal but less costly to check conditions
//
// - mark contradicting (always false) conditions (set empty)
// - mark tautological (always true) conditions (set skip)
// - reduce AND/OR branches when always true/false conds are present
//   - and: always true node => remove unless it is last child
//   - and: always false node => replace subtree with always false leaf
//   - or: always true node => replace subtree with always true leaf
//   - or: always false node => remove unless its last child
//
// - remove skip nodes
// - lift/merge single child nodes
// - lift/merge nested nodes of same kind
//   - OR ( OR (A, B), C) ) => OR (A, B, C)
//   - AND ( AND (A, B), C) => AND (A, B, C)
//
// - replace/simplify sets
//   - any: IN(single A) => EQ(A)
//   - any: NI(single A) => NE(A)
//   - any: EQ(A) + EQ(A) => EQ(A) -- same value, duplicate
//   - any: empty IN => false
//   - any: empty NIN => true
//   - any: IN(A,B,C) => RG(A,C)
//   - and: EQ(A) + EQ(B) => false iff A != B
//   - and: IN(A) + IN(B) => IN(A-B) -- intersect (handle empty case)
//   - and: NI(A) + NI(B) => NI(A+B) -- union! (does work for OR!!!)
//   - and: EQ(A) + NE(A) => false
//   - and: disjunct IN + IN => false
//   - and: disjunct IN + EQ => false
//   - or: IN(A) + IN(B) => IN(A+B) -- union
//   - or: IN(A) + EQ(B) => IN(A+B)
//   - or: EQ(A) + EQ(B) => IN(A+B)
//   - or: NI(A) + NI(B) => NI(A/B), true iff A / B = ø
//   - or: NE(A) + NE(B) => true iff A != B
//   - or: IN(A) + NE(B) => true iff B in [A] (set + antiset covers all universe)
//
// - replace/simplify ranges
//   - any: LT(min) => false
//   - any: GT(max) => false
//   - any: LE(max) => true
//   - any: GE(min) => true
//   - any: GE(max) => EQ(max)
//   - any: LE(min) => EQ(min)
//   - any: RG(min,max) => true
//   - any: RG(A,B) => false iff A > B
//   - any: RG(min,N) => LE(N)
//   - any: RG(N,max) => GE(N)
//   - any: RG(N,N) => EQ(N)
//   - and: LT|LE(A) + LT|LE(A) => LT|LE(A) -- minimum
//   - and: GT|GE(A) + GT|GE(A) => GT|GE(A) -- maximum
//   - and: RG(A,B) + RG(C,D) => RG(B,C) iff C ≤ B
//   - and: RG(A,B) + EQ(C) => EQ(C) iff A ≤ C ≤ B
//   - and: GE(A) + LE(B) => RG(A,B) iff A ≤ B
//   - and: GT(A) + LT(B) => RG(A+1,B-1) iff A ≤ B
//   - and: RG(A,B) + EQ(C) => EQ(C) iff A ≤ C ≤ B -- replace weaker with stronger
//   - and: RG(A,B) + EQ(C) => false iff C ¢ [A,B]
//   - and: GE(uint, 0), LE(uint, uint_max) => true
//   - and: GE(int, int_min), LE(int, int_max) => true
//   - or: RG(A,B) + RG(C,D) => RG(A,D) iff C ≤ B
//   - or: RG(A,B) + EQ(C) => RG(A,B) iff A ≤ C ≤ B -- replace weaker with stronger
//
// TODO: range and set type modes
//   - or: RG(A,B) + NE(C) => true iff C ¢ [A,B]
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
		// skip discardable nodes
		if child.Skip {
			continue
		}

		// keep leaf nodes
		if child.IsLeaf() {
			newChilds = append(newChilds, child)
			continue
		}

		// skip empty children
		if len(child.Children) == 0 {
			continue
		}

		// lift nested single-child node's child
		if len(child.Children) == 1 {
			newChilds = append(newChilds, child.Children[0])
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

func simplifyNodes(nodes []*FilterTreeNode, isOrNode bool) []*FilterTreeNode {
	// split leafs from nested nodes
	branches, leafs, ok := slicex.CutFunc(nodes, func(n *FilterTreeNode) bool {
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

	// first apply simplifications for single nodes
	leafs = simplifySingle(leafs, isOrNode)

	// then merge ranges (LT, LE, GT, GE, RG, EQ)
	leafs = simplifyRanges(leafs, isOrNode)

	// then merge sets (EQ, NE, IN, NI)
	leafs = simplifySets(leafs, isOrNode)

	// recombine optimized leafs with nested branch nodes
	nodes = nodes[:0]
	nodes = append(nodes, leafs...)
	nodes = append(nodes, branches...)

	// simplify AND/OR when always true/false conds are present
	if isOrNode {
		// one node true => everything true
		var trueNode *FilterTreeNode
		for _, n := range nodes {
			if n.IsLeaf() && n.Filter.Mode == FilterModeTrue {
				trueNode = n
				break
			}
		}
		if trueNode != nil {
			return []*FilterTreeNode{trueNode}
		}
		// remove always false nodes unless its last
		if len(nodes) > 1 {
			nodes = slices.DeleteFunc(nodes, func(n *FilterTreeNode) bool {
				return n.IsLeaf() && n.Filter.Mode == FilterModeFalse
			})
		}
	} else {
		// one node false => everything false
		var falseNode *FilterTreeNode
		for _, n := range nodes {
			if n.IsLeaf() && n.Filter.Mode == FilterModeFalse {
				falseNode = n
				break
			}
		}
		if falseNode != nil {
			return []*FilterTreeNode{falseNode}
		}
		// remove always true nodes unless its last
		if len(nodes) > 1 {
			nodes = slices.DeleteFunc(nodes, func(n *FilterTreeNode) bool {
				return n.IsLeaf() && n.Filter.Mode == FilterModeTrue
			})
		}
	}

	// return the optimized leafs combined with nested nodes
	return nodes
}

// Simplifications for any kind (and|or)
// - any: IN(single A) => EQ(A)
// - any: NI(single A) => NE(A)
// - any: empty IN => false
// - any: empty NIN => true
// - any: LT(min) => false
// - any: GT(max) => false
// - any: LE(max) => true
// - any: GE(min) => true
// - any: GE(max) => EQ(max)
// - any: LE(min) => EQ(min)
// - any: RG(from>to) => false
// - any: RG(min,max) => true
// - any: RG(min,N) => LE(N)
// - any: RG(N,max) => GE(N)
// - any: RG(N,N) => EQ(N)
// - any: IN(A,B,C) => RG(A,C)
func simplifySingle(nodes []*FilterTreeNode, _ bool) []*FilterTreeNode {
	var res []*FilterTreeNode

	for _, node := range nodes {
		f := node.Filter

		// we decide based on filter mode
		switch f.Mode {
		case FilterModeIn:
			switch f.Matcher.Len() {
			case 0:
				res = append(res, &FilterTreeNode{
					Filter: makeFalseFilterFrom(f),
				})
			case 1:
				res = append(res, &FilterTreeNode{
					Filter: makeFilterFrom(f, FilterModeEqual, reflectSliceIndex(f.Value, 0)),
				})
			default:
				// convert full set to range for integer types
				minv, maxv, isFull := cmp.Range(f.Type, f.Value)
				if isFull && minv != nil {
					rg := RangeValue{minv, maxv}
					if isFullDomain(f.Type, rg) {
						res = append(res, &FilterTreeNode{
							Filter: makeTrueFilterFrom(f),
						})
					} else {
						res = append(res, &FilterTreeNode{
							Filter: makeFilterFrom(f, FilterModeRange, rg),
						})
					}
				} else {
					res = append(res, node)
				}
			}

		case FilterModeNotIn:
			switch f.Matcher.Len() {
			case 0:
				res = append(res, &FilterTreeNode{
					Filter: makeTrueFilterFrom(f),
				})
			case 1:
				res = append(res, &FilterTreeNode{
					Filter: makeFilterFrom(f, FilterModeNotEqual, reflectSliceIndex(f.Value, 0)),
				})
			default:
				res = append(res, node)

			}
		case FilterModeLt:
			if cmp.Cmp(f.Type, f.Value, cmp.MinNumericVal(f.Type)) == 0 {
				res = append(res, &FilterTreeNode{
					Filter: makeFalseFilterFrom(f),
				})
			} else {
				res = append(res, node)
			}
		case FilterModeGt:
			if cmp.Cmp(f.Type, f.Value, cmp.MaxNumericVal(f.Type)) == 0 {
				res = append(res, &FilterTreeNode{
					Filter: makeFalseFilterFrom(f),
				})
			} else {
				res = append(res, node)
			}
		case FilterModeLe:
			switch {
			case cmp.Cmp(f.Type, f.Value, cmp.MaxNumericVal(f.Type)) == 0:
				res = append(res, &FilterTreeNode{
					Filter: makeTrueFilterFrom(f),
				})
			case cmp.Cmp(f.Type, f.Value, cmp.MinNumericVal(f.Type)) == 0:
				res = append(res, &FilterTreeNode{
					Filter: makeFilterFrom(f, FilterModeEqual, f.Value),
				})
			default:
				res = append(res, node)
			}
		case FilterModeGe:
			switch {
			case cmp.Cmp(f.Type, f.Value, cmp.MinNumericVal(f.Type)) == 0:
				res = append(res, &FilterTreeNode{
					Filter: makeTrueFilterFrom(f),
				})
			case cmp.Cmp(f.Type, f.Value, cmp.MaxNumericVal(f.Type)) == 0:
				res = append(res, &FilterTreeNode{
					Filter: makeFilterFrom(f, FilterModeEqual, f.Value),
				})
			default:
				res = append(res, node)
			}
		case FilterModeRange:
			rg := f.Value.(RangeValue)
			c := cmp.Cmp(f.Type, rg[0], rg[1])
			switch {
			case c > 0:
				res = append(res, &FilterTreeNode{
					Filter: makeFalseFilterFrom(f),
				})
			case c == 0:
				res = append(res, &FilterTreeNode{
					Filter: makeFilterFrom(f, FilterModeEqual, rg[0]),
				})
			case isFullDomain(f.Type, rg):
				res = append(res, &FilterTreeNode{
					Filter: makeTrueFilterFrom(f),
				})
			case cmp.Cmp(f.Type, rg[0], cmp.MinNumericVal(f.Type)) == 0:
				res = append(res, &FilterTreeNode{
					Filter: makeFilterFrom(f, FilterModeLe, rg[1]),
				})
			case cmp.Cmp(f.Type, rg[1], cmp.MaxNumericVal(f.Type)) == 0:
				res = append(res, &FilterTreeNode{
					Filter: makeFilterFrom(f, FilterModeGe, rg[0]),
				})
			default:
				res = append(res, node)
			}
		default:
			res = append(res, node)
		}
	}

	return res
}

// Simplifications for ranges that apply to AND or OR nodes
func simplifyRanges(nodes []*FilterTreeNode, isOrNode bool) []*FilterTreeNode {
	var (
		resultNodes  []*FilterTreeNode
		sameIdNodes  []*FilterTreeNode
		sameIdRanges []RangeValue
		lastId       uint16
		eqMode       byte // bit flags: 1 = LE/GE/EQ/RG, 2 = LT/GT, 3 = both
		f            *Filter
	)

	// stop early on empty node list
	if len(nodes) == 0 {
		return nodes
	}

	postProcess := func() {
		// try merge multiple ranges
		if len(sameIdNodes) > 1 {
			var mergedRanges []RangeValue
			if isOrNode {
				mergedRanges = mergeRangesOr(f.Type, sameIdRanges)
			} else {
				mergedRanges = mergeRangesAnd(f.Type, sameIdRanges)
			}

			// convert merged ranges back to filters; cases:
			// - len(merged) == len(nodes) => keep originals (no merge possible)
			// - len(merged) == 0 => always false (no intersection)
			// - len(merged) == 1 && min == MinVal && max == MaxVal => always true
			// - min == MinVal => LE(max)
			// - max == MaxVal => GE(min)
			// - min == max => EQ(min)
			// - other => RG(min, max)
			switch {
			case len(mergedRanges) == len(sameIdNodes):
				// keep originals
				resultNodes = append(resultNodes, sameIdNodes...)
			case len(mergedRanges) == 0:
				// replace with always false node
				resultNodes = append(resultNodes, &FilterTreeNode{
					Filter: makeFalseFilterFrom(f),
				})
			case len(mergedRanges) == 1 && isFullDomain(f.Type, mergedRanges[0]):
				// replace with always true node
				resultNodes = append(resultNodes, &FilterTreeNode{
					Filter: makeTrueFilterFrom(f),
				})
			default:
				// generate nodes
				for _, rg := range mergedRanges {
					resultNodes = append(resultNodes, &FilterTreeNode{
						Filter: makeRangeFilterFrom(f, rg, eqMode),
					})
				}
			}

		} else {
			// keep original when only a single range-like condition exists
			resultNodes = append(resultNodes, sameIdNodes...)
		}
	}

	for _, node := range nodes {
		// try optimize when field id changes
		if lastId != node.Filter.Index {
			postProcess()

			// prepare next round
			lastId = node.Filter.Index
			eqMode = 0
			if sameIdNodes != nil {
				sameIdNodes = sameIdNodes[:0]
			}
			if sameIdRanges != nil {
				sameIdRanges = sameIdRanges[:0]
			}
		}

		// keep this node's filter around for potential use in post-process
		f = node.Filter

		// construct ranges for ordered types from numeric conditions
		switch f.Mode {
		case FilterModeEqual:
			sameIdNodes = append(sameIdNodes, node)
			sameIdRanges = append(sameIdRanges, RangeValue{
				f.Value,
				f.Value,
			})
			eqMode |= 1

		case FilterModeRange:
			// check contradiction
			rg := f.Value.(RangeValue)
			if cmp.Cmp(f.Type, rg[0], rg[1]) > 0 {
				resultNodes = append(resultNodes, &FilterTreeNode{
					Filter: makeFalseFilterFrom(f),
				})
				continue
			}
			sameIdNodes = append(sameIdNodes, node)
			sameIdRanges = append(sameIdRanges, rg)
			eqMode |= 1

		case FilterModeLt:
			// check contradiction (this also happens in simplifySingle)
			if cmp.Cmp(f.Type, f.Value, cmp.MinNumericVal(f.Type)) == 0 {
				resultNodes = append(resultNodes, &FilterTreeNode{
					Filter: makeFalseFilterFrom(f),
				})
				continue
			}

			sameIdNodes = append(sameIdNodes, node)
			sameIdRanges = append(sameIdRanges, RangeValue{
				cmp.MinNumericVal(f.Type),
				cmp.Dec(f.Type, f.Value),
			})
			eqMode |= 2

		case FilterModeLe:
			sameIdNodes = append(sameIdNodes, node)
			sameIdRanges = append(sameIdRanges, RangeValue{
				cmp.MinNumericVal(f.Type),
				f.Value,
			})
			eqMode |= 1

		case FilterModeGt:
			// check contradiction (this also happens in simplifySingle)
			if cmp.Cmp(f.Type, f.Value, cmp.MaxNumericVal(f.Type)) == 0 {
				resultNodes = append(resultNodes, &FilterTreeNode{
					Filter: makeFalseFilterFrom(f),
				})
				continue
			}

			sameIdNodes = append(sameIdNodes, node)
			sameIdRanges = append(sameIdRanges, RangeValue{
				cmp.Inc(f.Type, f.Value),
				cmp.MaxNumericVal(f.Type),
			})
			eqMode |= 2

		case FilterModeGe:
			sameIdNodes = append(sameIdNodes, node)
			sameIdRanges = append(sameIdRanges, RangeValue{
				f.Value,
				cmp.MaxNumericVal(f.Type),
			})
			eqMode |= 1

		default:
			// skip NE, IN, NI, RE

			// TODO: merge set + range in OR conditions
			//
			// - goal: eliminate sets or reduce set size
			// - pre-checks (as this strategy is expensive)
			//   - a range-like condition exists
			//   - set overlaps with any range (only then it may become smaller)
			//   - set is smaller than a limit (counted in elements or clusters?)
			// - strategy:
			//   - split set into range clusters (single values form independent cluster)
			//   - optimize together with other ranges and output as RG + EQ list
			//   - a later stage will re-combine EQ filters into IN again
			// case FilterModeIn:
			// if isOrNode {
			// } else {
			// 	resultNodes = append(resultNodes, node)
			// }

			resultNodes = append(resultNodes, node)
		}
	}

	// handle last round
	postProcess()

	return resultNodes
}

// Simplifications for sets that apply to AND or OR nodes
// - and: IN(A) + IN(A) => IN(A) -- intersect (handle empty case)
// - and: NI(A) + NI(A) => NI(A) -- union! (does not work for OR!!!)
// - and: EQ(A) + EQ(B) => false iff A != B
// - and: IN(A) + NI(B) => IN(A-B) -- A and not B, false when empty
// - and: EQ(A) + NE(A) => false
// - and: NE(A) + NE(B) => NI(A,B) -- union
// - and: NI(A) + NE(B) => NI(A,B) -- union
// - and: disjunct IN + IN => false
// - and: disjunct IN + EQ => false
// - and: IN(1,4,5) + GT(4) => EQ(5) -- intersect set with range
// - or: IN(A) + IN(B) => IN(A+B) -- union
// - or: IN(A) + EQ(B) => IN(A+B)
// - or: EQ(A) + EQ(B) => IN(A+B)
// - or: NE(A) + NE(B) => true
// - or: NI(A) + NI(B) => NI(A/B), true iff A / B = ø
// - or: NE(A) + NE(B) => true iff A != B
// - or: IN(A) + NE(B) => true iff B in [A] (set + antiset covers all universe)
// - any: EQ(A) + EQ(A) => EQ(A) -- same value, duplicate
// - any: NE(A) + NE(A) => NE(A) -- same value, duplicate
func simplifySets(nodes []*FilterTreeNode, isOrNode bool) []*FilterTreeNode {
	var (
		ins, nis    any
		lastId      uint16
		res         []*FilterTreeNode
		f           *Filter
		plus, minus func(BlockType, any, any) any
	)

	// order nodes by field index and move sets first
	sort.Slice(nodes, func(i, j int) bool {
		ix, jx := nodes[i].Filter.Index, nodes[j].Filter.Index
		if ix != jx {
			return ix < jx
		}
		var is, js uint16
		switch nodes[i].Filter.Mode {
		case FilterModeIn, FilterModeNotIn:
			is++
		}
		switch nodes[j].Filter.Mode {
		case FilterModeIn, FilterModeNotIn:
			js++
		}
		return is > js
	})

	postProcess := func() {
		// produce zero or one combined filter from sets
		if flt := makeSetFilterFrom(f, ins, nis, isOrNode); flt != nil {
			res = append(res, &FilterTreeNode{
				Filter: flt,
			})
		}
	}

	// stop early on empty node list
	if len(nodes) == 0 {
		return nodes
	}

	// set aggregation functions
	if isOrNode {
		plus, minus = cmp.Intersect, cmp.Union
	} else {
		plus, minus = cmp.Union, cmp.Intersect
	}

	// walk all nodes
	for _, node := range nodes {
		// reset when field id changes
		if lastId != node.Filter.Index {
			postProcess()

			// reset state
			lastId = node.Filter.Index
			ins, nis = nil, nil
		}

		// keep this node's filter around for potential use in post-process
		f = node.Filter

		// construct eq & ne sets
		switch f.Mode {
		case FilterModeEqual:
			if ins == nil {
				ins = makeReflectSlice(f.Value)
			} else {
				ins = minus(f.Type, ins, makeReflectSlice(f.Value))
			}
		case FilterModeIn:
			if ins == nil {
				ins = f.Value
			} else {
				ins = minus(f.Type, ins, f.Value)
			}
		case FilterModeNotEqual:
			if nis == nil {
				nis = makeReflectSlice(f.Value)
			} else {
				nis = plus(f.Type, nis, makeReflectSlice(f.Value))
			}
		case FilterModeNotIn:
			if nis == nil {
				nis = f.Value
			} else {
				nis = plus(f.Type, nis, f.Value)
			}
		case FilterModeGt, FilterModeGe, FilterModeLt, FilterModeLe, FilterModeRange:
			if !isOrNode && ins != nil {
				// intersect set with range (must be AND node and have a set)
				// this drops the original range filter and alters the IN set
				minv, maxv := cmp.MinNumericVal(f.Type), cmp.MaxNumericVal(f.Type)
				switch f.Mode {
				case FilterModeGt:
					minv = cmp.Inc(f.Type, f.Value)
				case FilterModeGe:
					minv = f.Value
				case FilterModeLt:
					maxv = cmp.Dec(f.Type, f.Value)
				case FilterModeLe:
					maxv = f.Value
				case FilterModeRange:
					rg := f.Value.(RangeValue)
					minv, maxv = rg[0], rg[1]
				}
				ins = cmp.IntersectRange(f.Type, ins, minv, maxv)
			} else {
				res = append(res, node)
			}

		default:
			// pass through non-set filter modes
			res = append(res, node)
		}
	}

	// handle last round
	postProcess()

	return res
}

func reflectSliceLen(s any) int {
	return reflect.ValueOf(s).Len()
}

func reflectSliceIndex(slice any, index int) any {
	return reflect.ValueOf(slice).Index(index).Interface()
}

// func appendReflectValue(a, b any) any {
// 	return reflect.Append(reflect.ValueOf(a), reflect.ValueOf(b)).Interface()
// }

// func appendReflectSlice(a, b any) any {
// 	return reflect.AppendSlice(reflect.ValueOf(a), reflect.ValueOf(b)).Interface()
// }

func makeReflectSlice(vals ...any) any {
	if len(vals) == 0 {
		return nil
	}
	slice := reflect.MakeSlice(reflect.SliceOf(reflect.TypeOf(vals[0])), 0, len(vals))
	for _, v := range vals {
		slice = reflect.Append(slice, reflect.ValueOf(v))
	}
	return slice.Interface()
}

func sortRanges(typ BlockType, vals []RangeValue) {
	// sort by lower and upper range bound, replacing nil with math min/max
	sort.Slice(vals, func(i, j int) bool {
		// sort by lows
		c := cmp.Cmp(typ, vals[i][0], vals[j][0])
		if c != 0 {
			return c < 0
		}
		// on equal lows, sort by highs
		return cmp.Cmp(typ, vals[i][1], vals[j][1]) < 0
	})
}

// intersect multiple ranges (len must be >= 1), result can at most be a single range or none
func mergeRangesAnd(typ BlockType, vals []RangeValue) []RangeValue {
	// pre-sort ranges
	sortRanges(typ, vals)

	// start at the first range
	merged := vals[0]

	// intersect ranges
	for i := 1; i < len(vals); i++ {
		// check if the next value intersects at all (next.min <= merged.max)
		if cmp.Cmp(typ, merged[1], vals[i][0]) < 0 {
			return nil
		}

		// update result range minimum (because vals is sorted we can take the equal
		// or higer minimum from the current element)
		merged[0] = vals[i][0]

		// update result range maximum to the minimum of both
		merged[1] = cmp.Min(typ, merged[1], vals[i][1])
	}

	return []RangeValue{merged}
}

// merge overlapping ranges, vals must have length > 1
func mergeRangesOr(typ BlockType, vals []RangeValue) []RangeValue {
	// pre-sort ranges
	sortRanges(typ, vals)

	// combine overlaps and adjacent ranges
	j := 0
	for i := 1; i < len(vals); i++ {
		if cmp.Cmp(typ, vals[j][1], cmp.Dec(typ, vals[i][0])) >= 0 {
			if cmp.Cmp(typ, vals[j][1], vals[i][1]) < 0 {
				vals[j][1] = vals[i][1]
			}
		} else {
			j++
			vals[j] = vals[i]
		}
	}

	return vals[:j+1]
}

// - min == max => EQ(min)
// - min == MinVal => LE(max)
// - max == MaxVal => GE(min)
// - other => RG(min, max)
func makeRangeFilterFrom(f *Filter, rg RangeValue, eqMode byte) *Filter {
	switch {
	case cmp.Cmp(f.Type, rg[0], rg[1]) == 0:
		// equal min == max => EQ(min)
		m := newFactory(f.Type).New(FilterModeEqual)
		m.WithValue(rg[0])
		return &Filter{
			Name:    f.Name,
			Type:    f.Type,
			Mode:    FilterModeEqual,
			Index:   f.Index,
			Matcher: m,
			Value:   rg[0],
		}

	case cmp.Cmp(f.Type, rg[0], cmp.MinNumericVal(f.Type)) == 0:
		// range start is min val => LE(max) or LT(max+1)
		if eqMode&1 > 0 {
			m := newFactory(f.Type).New(FilterModeLe)
			m.WithValue(rg[1])
			return &Filter{
				Name:    f.Name,
				Type:    f.Type,
				Mode:    FilterModeLe,
				Index:   f.Index,
				Matcher: m,
				Value:   rg[1],
			}
		} else {
			m := newFactory(f.Type).New(FilterModeLt)
			val := cmp.Inc(f.Type, rg[1])
			m.WithValue(val)
			return &Filter{
				Name:    f.Name,
				Type:    f.Type,
				Mode:    FilterModeLt,
				Index:   f.Index,
				Matcher: m,
				Value:   val,
			}
		}

	case cmp.Cmp(f.Type, rg[1], cmp.MaxNumericVal(f.Type)) == 0:
		// range end is max val => GE(min) or GT(min-1)
		if eqMode&1 > 0 {
			m := newFactory(f.Type).New(FilterModeGe)
			m.WithValue(rg[0])
			return &Filter{
				Name:    f.Name,
				Type:    f.Type,
				Mode:    FilterModeGe,
				Index:   f.Index,
				Matcher: m,
				Value:   rg[0],
			}
		} else {
			m := newFactory(f.Type).New(FilterModeGt)
			val := cmp.Dec(f.Type, rg[0])
			m.WithValue(val)
			return &Filter{
				Name:    f.Name,
				Type:    f.Type,
				Mode:    FilterModeGt,
				Index:   f.Index,
				Matcher: m,
				Value:   val,
			}
		}
	default:
		// some other range => RG(min, max)
		m := newFactory(f.Type).New(FilterModeRange)
		m.WithValue(rg)
		return &Filter{
			Name:    f.Name,
			Type:    f.Type,
			Mode:    FilterModeRange,
			Index:   f.Index,
			Matcher: m,
			Value:   rg,
		}
	}
}

func makeSetFilterFrom(f *Filter, ins, nis any, isOrNode bool) *Filter {
	// aggregate sets into new nodes
	switch {
	case ins != nil && nis != nil:
		// both IN and NI conditions exist, make filter from set difference
		// direction of set difference is defined by AND/OR type
		var set any
		if isOrNode {
			set = cmp.Difference(f.Type, nis, ins)
		} else {
			set = cmp.Difference(f.Type, ins, nis)
		}
		switch reflectSliceLen(set) {
		case 0:
			if isOrNode {
				return makeTrueFilterFrom(f) // tautology
			} else {
				return makeFalseFilterFrom(f) // contradiction
			}
		case 1:
			if isOrNode {
				return makeFilterFrom(f, FilterModeNotEqual, reflectSliceIndex(set, 0))
			} else {
				return makeFilterFrom(f, FilterModeEqual, reflectSliceIndex(set, 0))
			}
		default:
			if isOrNode {
				return makeFilterFrom(f, FilterModeNotIn, set)
			} else {
				return makeFilterFrom(f, FilterModeIn, set)
			}
		}
	case ins != nil:
		// only IN (or EQ) conditions exist
		switch reflectSliceLen(ins) {
		case 0:
			return makeFalseFilterFrom(f) // contradiction
		case 1:
			return makeFilterFrom(f, FilterModeEqual, reflectSliceIndex(ins, 0))
		default:
			return makeFilterFrom(f, FilterModeIn, ins)
		}
	case nis != nil:
		// only NI (or NE) conditions exist
		switch reflectSliceLen(nis) {
		case 0:
			return makeTrueFilterFrom(f) // tautology
		case 1:
			return makeFilterFrom(f, FilterModeNotEqual, reflectSliceIndex(nis, 0))
		default:
			return makeFilterFrom(f, FilterModeNotIn, nis)
		}
	default:
		return nil
	}
}

func makeFalseFilterFrom(f *Filter) *Filter {
	return &Filter{
		Name:    f.Name,
		Type:    f.Type,
		Mode:    FilterModeFalse,
		Index:   f.Index,
		Matcher: &noopMatcher{},
		Value:   nil,
	}
}

func makeTrueFilterFrom(f *Filter) *Filter {
	return &Filter{
		Name:    f.Name,
		Type:    f.Type,
		Mode:    FilterModeTrue,
		Index:   f.Index,
		Matcher: &noopMatcher{},
		Value:   nil,
	}
}

func makeFilterFrom(f *Filter, mode FilterMode, val any) *Filter {
	m := newFactory(f.Type).New(mode)
	m.WithValue(val)
	return &Filter{
		Name:    f.Name,
		Type:    f.Type,
		Mode:    mode,
		Index:   f.Index,
		Matcher: m,
		Value:   val,
	}
}

func makeFilterFromSet(f *Filter, set *xroar.Bitmap) *Filter {
	m := newFactory(f.Type).New(FilterModeIn)
	m.WithSet(set)
	return &Filter{
		Name:    f.Name,
		Type:    f.Type,
		Mode:    FilterModeIn,
		Index:   f.Index,
		Matcher: m,
		Value:   m.Value(), // FIXME: optimizer expects []T which is expensive
	}
}

func isFullDomain(typ BlockType, rg RangeValue) bool {
	switch typ {
	case BlockString, BlockBytes:
		return false
	}
	isMin := cmp.Cmp(typ, rg[0], cmp.MinNumericVal(typ)) == 0
	if !isMin {
		return false
	}
	isMax := cmp.Cmp(typ, rg[1], cmp.MaxNumericVal(typ)) == 0
	return isMax
}
