// Copyright (c) 2023-2026 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package slicex

import (
	"slices"

	"blockwatch.cc/knoxdb/pkg/sortx"
)

func ContainsSorted[T Integer | Float](s []T, v T) bool {
	return contains(s, v, false)
}

func ContainsRangeSorted[T Integer | Float](s []T, from, to T) bool {
	return containsRange(s, from, to)
}

func Intersect[T Integer](s, t []T) []T {
	return intersect(nil, Unique(s), Unique(t))
}

func IntersectRange[T Integer](s []T, from, to T) []T {
	sortx.Sort(s, 0)
	return IntersectRangeSorted(s, from, to)
}

func IntersectRangeSorted[T Integer | Float](s []T, from, to T) []T {
	return intersectRange(nil, s, from, to)
}

func Range[T Integer](s []T) (T, T, bool) {
	sortx.Sort(s, 0)
	return RangeSorted(s)
}

func RangeSorted[T Integer | Float](s []T) (T, T, bool) {
	switch l := len(s); l {
	case 0:
		var zero T
		return zero, zero, true
	case 1:
		return s[0], s[0], true
	default:
		x, y := s[0], s[l-1]
		return x, y, int(y-x)+1 == l
	}
}

func Remove[T Integer](s []T, t ...T) []T {
	sortx.Sort(s, 0)
	sortx.Sort(t, 0)
	return remove(s, t)
}

func RemoveSorted[T Integer | Float](s []T, t ...T) []T {
	return remove(s, t)
}

func RemoveZeros[T Integer | Float](s []T) []T {
	var zero T
	return slices.DeleteFunc(s, func(v T) bool { return v == zero })
}

// func Shuffle[T Integer | Float | ~string](s []T) []T {
// 	util.RandShuffle(len(s), func(i, j int) {
// 		s[i], s[j] = s[j], s[i]
// 	})
// 	return s
// }

func Union[T Integer](s, t []T) []T {
	if len(s) == 0 {
		return Unique(t)
	}
	if len(t) == 0 {
		return Unique(s)
	}
	return mergeUnique(Unique(s), Unique(t)...)
}

func Unique[T Integer](s []T) []T {
	sortx.Sort(s, 0)
	return unique(s)
}
