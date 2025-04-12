// Copyright (c) 2023-2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package slicex

import (
	"math"
	"slices"

	"blockwatch.cc/knoxdb/pkg/util"
	"golang.org/x/exp/constraints"
)

type Float interface {
	constraints.Float
}

// Optimized algorithms for ordered numeric slices
type OrderedFloats[T Float] struct {
	NonZero bool
	Unique  bool
	Values  []T
}

func NewOrderedFloats[T Float](s []T) *OrderedFloats[T] {
	if s == nil {
		s = make([]T, 0)
	}
	slices.Sort(s)
	return &OrderedFloats[T]{
		Values: s,
	}
}

func UniqueFloats[T Float](s []T) []T {
	slices.Sort(s)
	return removeDuplicates(s)
}

func ShuffleFloats[T Float](s []T) []T {
	util.RandShuffle(len(s), func(i, j int) {
		s[i], s[j] = s[j], s[i]
	})
	return s
}

func RemoveZeroFloats[T Float](s []T) []T {
	var zero T
	return slices.DeleteFunc(s, func(v T) bool { return v == zero })
}

func RemoveSpecialFloats[T Float](s []T) []T {
	return slices.DeleteFunc(s, func(v T) bool {
		return math.IsInf(float64(v), 0) || math.IsNaN(float64(v)) ||
			(math.Signbit(float64(v)) && v == 0.0)
	})
}

func (o *OrderedFloats[T]) SetNonZero() *OrderedFloats[T] {
	// remove zero values
	var zero T
	o.Values = slices.DeleteFunc(o.Values, func(v T) bool { return v == zero })
	o.NonZero = true
	return o
}

func (o *OrderedFloats[T]) SetUnique() *OrderedFloats[T] {
	// remove duplicates
	o.Values = removeDuplicates(o.Values)
	o.Unique = true
	return o
}

func (o OrderedFloats[T]) Len() int {
	return len(o.Values)
}

func (o OrderedFloats[T]) MinMax() (T, T) {
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

func (o OrderedFloats[T]) Min() T {
	if len(o.Values) == 0 {
		return 0
	}
	return o.Values[0]
}

func (o OrderedFloats[T]) Max() T {
	if l := len(o.Values); l > 0 {
		return o.Values[l-1]
	}
	return 0
}

func (o *OrderedFloats[T]) Insert(val ...T) *OrderedFloats[T] {
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

func (o *OrderedFloats[T]) Remove(val ...T) *OrderedFloats[T] {
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

func (o *OrderedFloats[T]) Index(val T) (int, bool) {
	return index(o.Values, val, 0, o.NonZero && o.Unique)
}

func (o *OrderedFloats[T]) IndexStart(val T, start int) (int, bool) {
	return index(o.Values, val, start, o.NonZero && o.Unique)
}

func (o OrderedFloats[T]) Contains(val T) bool {
	return contains(o.Values, val, o.NonZero && o.Unique)
}

func (o OrderedFloats[T]) ContainsAny(val ...T) bool {
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

func (o OrderedFloats[T]) ContainsAll(val ...T) bool {
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

func (o OrderedFloats[T]) Equal(o2 *OrderedFloats[T]) bool {
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

func (o OrderedFloats[T]) ContainsRange(from, to T) bool {
	return containsRange(o.Values, from, to)
}

func (o OrderedFloats[T]) RemoveRange(from, to T) *OrderedFloats[T] {
	return &OrderedFloats[T]{
		NonZero: o.NonZero,
		Unique:  o.Unique,
		Values:  removeRange(o.Values, from, to, make([]T, 0)),
	}
}

func (o OrderedFloats[T]) IntersectRange(from, to T) *OrderedFloats[T] {
	return &OrderedFloats[T]{
		NonZero: o.NonZero,
		Unique:  o.Unique,
		Values:  intersectRange(o.Values, from, to, make([]T, 0)),
	}
}

func (o OrderedFloats[T]) Intersect(v *OrderedFloats[T]) *OrderedFloats[T] {
	if v == nil {
		return nil
	}
	return &OrderedFloats[T]{
		NonZero: o.NonZero || v.NonZero,
		Unique:  o.Unique || v.Unique,
		Values:  intersect(o.Values, v.Values, make([]T, 0)),
	}
}

func (o *OrderedFloats[T]) Union(v *OrderedFloats[T]) *OrderedFloats[T] {
	if v == nil {
		return o
	}
	res := &OrderedFloats[T]{
		NonZero: o.NonZero && v.NonZero,
		Unique:  o.Unique && v.Unique,
		Values:  make([]T, len(o.Values), len(o.Values)+len(v.Values)),
	}
	copy(res.Values, o.Values)
	res.Values = merge(res.Values, res.Unique, v.Values...)
	return res
}

func (o *OrderedFloats[T]) Difference(v *OrderedFloats[T]) *OrderedFloats[T] {
	if v == nil {
		return o
	}
	res := &OrderedFloats[T]{
		NonZero: o.NonZero && v.NonZero,
		Unique:  o.Unique && v.Unique,
		Values:  make([]T, len(o.Values)),
	}
	copy(res.Values, o.Values)
	return res.Remove(v.Values...)
}
