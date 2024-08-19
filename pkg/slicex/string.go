// Copyright (c) 2023 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package slicex

import (
	"strings"

	"golang.org/x/exp/slices"
)

// Optimized algorithms for ordered string slices
type OrderedStrings struct {
	NonZero bool
	Unique  bool
	Values  []string
}

func NewOrderedStrings(s []string) *OrderedStrings {
	if s == nil {
		s = make([]string, 0)
	}
	slices.Sort(s)
	return &OrderedStrings{
		Values: s,
	}
}

func (o *OrderedStrings) SetNonZero() *OrderedStrings {
	// remove zero values
	var zero string
	o.Values = slices.DeleteFunc(o.Values, func(v string) bool { return v == zero })
	o.NonZero = true
	return o
}

func (o *OrderedStrings) SetUnique() *OrderedStrings {
	// remove duplicates
	o.Values = removeDuplicates(o.Values)
	o.Unique = true
	return o
}

func (o OrderedStrings) MinMax() (string, string) {
	switch l := len(o.Values); l {
	case 0:
		var zero string
		return zero, zero
	case 1:
		return o.Values[0], o.Values[0]
	default:
		return o.Values[0], o.Values[l-1]
	}
}

func (o *OrderedStrings) Insert(val ...string) *OrderedStrings {
	// remove incoming zeros
	if o.NonZero {
		val, _ = removeZeros(val)
	}
	// sort incoming slice
	slices.Sort(val)

	// shortcut for empty target
	if len(o.Values) == 0 {
		if o.Unique {
			val = removeDuplicates(val)
		}
		o.Values = append(o.Values, val...)
		return o
	}

	// merge slices
	o.Values = merge(o.Values, o.Unique, val...)
	return o
}

func (o *OrderedStrings) Remove(val ...string) *OrderedStrings {
	if len(val) == 0 {
		return o
	}
	slices.Sort(val)
	var j, k int
	for _, v := range o.Values {
		if v == val[k] {
			k++
			continue
		}
		o.Values[j] = v
		j++
	}
	o.Values = o.Values[:j]
	return o
}

func (o *OrderedStrings) Index(val string) (int, bool) {
	return indexString(o.Values, val, 0)
}

func (o *OrderedStrings) IndexStart(val string, start int) (int, bool) {
	return indexString(o.Values, val, start)
}

func (o OrderedStrings) Contains(val string) bool {
	return containsString(o.Values, val)
}

func (o OrderedStrings) ContainsAny(val ...string) bool {
	slices.Sort(val)
	var (
		last int
		ok   bool
	)
	for _, v := range val {
		last, ok = indexString(o.Values, v, last)
		if ok {
			return true
		}
	}
	return false
}

func (o OrderedStrings) ContainsAll(val ...string) bool {
	slices.Sort(val)
	var (
		last int
		ok   bool
	)
	for _, v := range val {
		last, ok = indexString(o.Values, v, last)
		if !ok {
			return false
		}
	}
	return true
}

func (o OrderedStrings) ContainsRange(from, to string) bool {
	return containsRange(o.Values, from, to)
}

func (o OrderedStrings) Intersect(v *OrderedStrings) *OrderedStrings {
	if v == nil {
		return nil
	}
	return &OrderedStrings{
		NonZero: o.NonZero || v.NonZero,
		Unique:  o.Unique || v.Unique,
		Values:  intersect(o.Values, v.Values, make([]string, 0)),
	}
}

func containsString(s []string, val string) bool {
	// empty s cannot contain values
	if len(s) == 0 {
		return false
	}

	// s is sorted, check against first (min) and last (max) entries
	if strings.Compare(s[0], val) > 0 {
		return false
	}
	if strings.Compare(s[len(s)-1], val) < 0 {
		return false
	}

	// use binary search to find value in sorted s
	_, ok := slices.BinarySearch(s, val)
	return ok
}

// returns where val was found or would appear
func indexString(s []string, val string, last int) (int, bool) {
	if len(s) <= last {
		return len(s), false
	}

	// search for value in slice starting at last index
	slice := s[last:]
	l := len(slice)
	min, max := slice[0], slice[l-1]
	if val < min {
		return 0, false
	}
	if val > max {
		return l, false
	}

	// for sparse slices, use binary search (slice is sorted)
	idx, ok := slices.BinarySearch(s, val)
	return idx + last, ok
}
