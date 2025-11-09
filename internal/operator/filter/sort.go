// Copyright (c) 2023-2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package filter

// sorts nodes by weight
type ByWeight []*Node

func (l ByWeight) Len() int           { return len(l) }
func (l ByWeight) Swap(i, j int)      { l[i], l[j] = l[j], l[i] }
func (l ByWeight) Less(i, j int) bool { return l[i].Weight() < l[j].Weight() }

// sorts nodes by filter index
type ByFilterIndex []*Node

func (l ByFilterIndex) Len() int           { return len(l) }
func (l ByFilterIndex) Swap(i, j int)      { l[i], l[j] = l[j], l[i] }
func (l ByFilterIndex) Less(i, j int) bool { return l[i].Filter.Index < l[j].Filter.Index }

// sorts nodes by filter index with sets first
type ByFilterIndexSetsFirst []*Node

func (l ByFilterIndexSetsFirst) Len() int      { return len(l) }
func (l ByFilterIndexSetsFirst) Swap(i, j int) { l[i], l[j] = l[j], l[i] }
func (l ByFilterIndexSetsFirst) Less(i, j int) bool {
	ix, jx := l[i].Filter.Index, l[j].Filter.Index
	if ix != jx {
		return ix < jx
	}
	var is, js uint16
	switch l[i].Filter.Mode {
	case FilterModeIn, FilterModeNotIn:
		is++
	}
	switch l[j].Filter.Mode {
	case FilterModeIn, FilterModeNotIn:
		js++
	}
	return is > js
}
