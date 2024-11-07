// Copyright (c) 2023 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package slicex

import (
	"slices"

	"golang.org/x/exp/constraints"
)

type Number interface {
	constraints.Integer | constraints.Float
}

// Optimized algorithms for ordered numeric slices
type OrderedNumbers[T Number] struct {
	NonZero bool
	Unique  bool
	Values  []T
}

func NewOrderedNumbers[T Number](s []T) *OrderedNumbers[T] {
	if s == nil {
		s = make([]T, 0)
	}
	slices.Sort(s)
	return &OrderedNumbers[T]{
		Values: s,
	}
}

func Unique[T Number](s []T) []T {
	slices.Sort(s)
	return removeDuplicates(s)
}

func RemoveZeros[T Number](s []T) []T {
	var zero T
	return slices.DeleteFunc(s, func(v T) bool { return v == zero })
}

func (o *OrderedNumbers[T]) SetNonZero() *OrderedNumbers[T] {
	// remove zero values
	var zero T
	o.Values = slices.DeleteFunc(o.Values, func(v T) bool { return v == zero })
	o.NonZero = true
	return o
}

func (o *OrderedNumbers[T]) SetUnique() *OrderedNumbers[T] {
	// remove duplicates
	o.Values = removeDuplicates(o.Values)
	o.Unique = true
	return o
}

func (o OrderedNumbers[T]) Len() int {
	return len(o.Values)
}

func (o OrderedNumbers[T]) MinMax() (T, T) {
	switch l := len(o.Values); l {
	case 0:
		var zero T
		return zero, zero
	case 1:
		return o.Values[0], o.Values[0]
	default:
		return o.Values[0], o.Values[l-1]
	}
}

func (o *OrderedNumbers[T]) Insert(val ...T) *OrderedNumbers[T] {
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

func (o *OrderedNumbers[T]) Remove(val ...T) *OrderedNumbers[T] {
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

func (o *OrderedNumbers[T]) Index(val T) (int, bool) {
	return index(o.Values, val, 0, o.NonZero && o.Unique)
}

func (o *OrderedNumbers[T]) IndexStart(val T, start int) (int, bool) {
	return index(o.Values, val, start, o.NonZero && o.Unique)
}

func (o OrderedNumbers[T]) Contains(val T) bool {
	return contains(o.Values, val, o.NonZero && o.Unique)
}

func (o OrderedNumbers[T]) ContainsAny(val ...T) bool {
	slices.Sort(val)
	var (
		last int
		ok   bool
	)
	for _, v := range val {
		last, ok = index(o.Values, v, last, o.NonZero && o.Unique)
		if ok {
			return true
		}
	}
	return false
}

func (o OrderedNumbers[T]) ContainsAll(val ...T) bool {
	slices.Sort(val)
	var (
		last int
		ok   bool
	)
	for _, v := range val {
		last, ok = index(o.Values, v, last, o.NonZero && o.Unique)
		if !ok {
			return false
		}
	}
	return true
}

func (o OrderedNumbers[T]) Equal(o2 *OrderedNumbers[T]) bool {
	if o.Len() != o2.Len() {
		return false
	}
	for i := range o.Values {
		if o.Values[i] != o2.Values[i] {
			return false
		}
	}
	return true
}

func (o OrderedNumbers[T]) ContainsRange(from, to T) bool {
	return containsRange(o.Values, from, to)
}

func (o OrderedNumbers[T]) Intersect(v *OrderedNumbers[T]) *OrderedNumbers[T] {
	if v == nil {
		return nil
	}
	return &OrderedNumbers[T]{
		NonZero: o.NonZero || v.NonZero,
		Unique:  o.Unique || v.Unique,
		Values:  intersect(o.Values, v.Values, make([]T, 0)),
	}
}

func (o *OrderedNumbers[T]) Union(v *OrderedNumbers[T]) *OrderedNumbers[T] {
	if v == nil {
		return o
	}
	res := &OrderedNumbers[T]{
		NonZero: o.NonZero && v.NonZero,
		Unique:  o.Unique && v.Unique,
		Values:  make([]T, len(o.Values)),
	}
	copy(res.Values, o.Values)
	res.Values = merge(res.Values, v.Unique, v.Values...)
	return res
}

func (o *OrderedNumbers[T]) Difference(v *OrderedNumbers[T]) *OrderedNumbers[T] {
	if v == nil {
		return o
	}
	res := &OrderedNumbers[T]{
		NonZero: o.NonZero && v.NonZero,
		Unique:  o.Unique && v.Unique,
		Values:  make([]T, len(o.Values)),
	}
	copy(res.Values, o.Values)
	return res.Remove(v.Values...)
}
