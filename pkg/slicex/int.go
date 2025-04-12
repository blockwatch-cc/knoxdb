// Copyright (c) 2023-2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package slicex

import (
	"slices"

	"blockwatch.cc/knoxdb/pkg/util"
)

type Integer interface {
	int | uint | int8 | int16 | int32 | int64 | uint8 | uint16 | uint32 | uint64
}

// Optimized algorithms for ordered numeric slices
type OrderedIntegers[T Integer] struct {
	NonZero bool
	Unique  bool
	Values  []T
}

func NewOrderedIntegers[T Integer](s []T) *OrderedIntegers[T] {
	if s == nil {
		s = make([]T, 0)
	}
	util.Sort(s, 0)
	return &OrderedIntegers[T]{
		Values: s,
	}
}

func Unique[T Integer](s []T) []T {
	util.Sort(s, 0)
	return removeDuplicates(s)
}

func Shuffle[T Integer](s []T) []T {
	util.RandShuffle(len(s), func(i, j int) {
		s[i], s[j] = s[j], s[i]
	})
	return s
}

func RemoveZeros[T Integer](s []T) []T {
	var zero T
	return slices.DeleteFunc(s, func(v T) bool { return v == zero })
}

func (o *OrderedIntegers[T]) SetNonZero() *OrderedIntegers[T] {
	// remove zero values
	var zero T
	o.Values = slices.DeleteFunc(o.Values, func(v T) bool { return v == zero })
	o.NonZero = true
	return o
}

func (o *OrderedIntegers[T]) SetUnique() *OrderedIntegers[T] {
	// remove duplicates
	o.Values = removeDuplicates(o.Values)
	o.Unique = true
	return o
}

func (o OrderedIntegers[T]) Len() int {
	return len(o.Values)
}

func (o OrderedIntegers[T]) MinMax() (T, T) {
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

func (o OrderedIntegers[T]) Min() T {
	if len(o.Values) == 0 {
		return 0
	}
	return o.Values[0]
}

func (o OrderedIntegers[T]) Max() T {
	if l := len(o.Values); l > 0 {
		return o.Values[l-1]
	}
	return 0
}

func (o OrderedIntegers[T]) IsContinuous() bool {
	a, b := o.MinMax()
	return int(b-a)+1 == len(o.Values)
}

func (o *OrderedIntegers[T]) Insert(val ...T) *OrderedIntegers[T] {
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

func (o *OrderedIntegers[T]) Remove(val ...T) *OrderedIntegers[T] {
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

func (o *OrderedIntegers[T]) Index(val T) (int, bool) {
	return index(o.Values, val, 0, o.NonZero && o.Unique)
}

func (o *OrderedIntegers[T]) IndexStart(val T, start int) (int, bool) {
	return index(o.Values, val, start, o.NonZero && o.Unique)
}

func (o OrderedIntegers[T]) Contains(val T) bool {
	return contains(o.Values, val, o.NonZero && o.Unique)
}

func (o OrderedIntegers[T]) ContainsAny(val ...T) bool {
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

func (o OrderedIntegers[T]) ContainsAll(val ...T) bool {
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

func (o OrderedIntegers[T]) Equal(o2 *OrderedIntegers[T]) bool {
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

func (o OrderedIntegers[T]) ContainsRange(from, to T) bool {
	return containsRange(o.Values, from, to)
}

func (o OrderedIntegers[T]) RemoveRange(from, to T) *OrderedIntegers[T] {
	return &OrderedIntegers[T]{
		NonZero: o.NonZero,
		Unique:  o.Unique,
		Values:  removeRange(o.Values, from, to, make([]T, 0)),
	}
}

func (o OrderedIntegers[T]) IntersectRange(from, to T) *OrderedIntegers[T] {
	return &OrderedIntegers[T]{
		NonZero: o.NonZero,
		Unique:  o.Unique,
		Values:  intersectRange(o.Values, from, to, make([]T, 0)),
	}
}

func (o OrderedIntegers[T]) Intersect(v *OrderedIntegers[T]) *OrderedIntegers[T] {
	if v == nil {
		return nil
	}
	return &OrderedIntegers[T]{
		NonZero: o.NonZero || v.NonZero,
		Unique:  o.Unique || v.Unique,
		Values:  intersect(o.Values, v.Values, make([]T, 0)),
	}
}

func (o *OrderedIntegers[T]) Union(v *OrderedIntegers[T]) *OrderedIntegers[T] {
	if v == nil {
		return o
	}
	res := &OrderedIntegers[T]{
		NonZero: o.NonZero && v.NonZero,
		Unique:  o.Unique && v.Unique,
		Values:  make([]T, len(o.Values), len(o.Values)+len(v.Values)),
	}
	copy(res.Values, o.Values)
	res.Values = merge(res.Values, res.Unique, v.Values...)
	return res
}

func (o *OrderedIntegers[T]) Difference(v *OrderedIntegers[T]) *OrderedIntegers[T] {
	if v == nil {
		return o
	}
	res := &OrderedIntegers[T]{
		NonZero: o.NonZero && v.NonZero,
		Unique:  o.Unique && v.Unique,
		Values:  make([]T, len(o.Values)),
	}
	copy(res.Values, o.Values)
	return res.Remove(v.Values...)
}
