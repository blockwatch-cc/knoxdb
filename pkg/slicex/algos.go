// Copyright (c) 2023-2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package slicex

import (
	"cmp"
	"slices"
	"sort"
)

type Integer interface {
	~int | ~uint | ~int8 | ~int16 | ~int32 | ~int64 |
		~uint8 | ~uint16 | ~uint32 | ~uint64 | ~uintptr
}

type Float interface{ ~float32 | ~float64 }

type Number interface {
	Integer | Float
}

func SizeFor[T Integer]() int {
	x := uint16(1 << 8)
	y := uint32(2 << 16)
	z := uint64(4 << 32)
	return 1 + int(T(x))>>8 + int(T(y))>>16 + int(T(z))>>32
}

// assumes src and rem are sorted
func remove[T cmp.Ordered](src, rem []T) []T {
	if len(src) == 0 || len(rem) == 0 {
		return src
	}

	var i, j, k int

	for i < len(src) && j < len(rem) {
		switch {
		case src[i] < rem[j]:
			src[k] = src[i]
			k++
			i++
		case src[i] > rem[j]:
			j++
		default:
			i++
			j++
		}
	}

	for ; i < len(src); i++ {
		src[k] = src[i]
		k++
	}

	return src[:k]
}

// assumes s is already sorted
func unique[T cmp.Ordered](s []T) []T {
	if len(s) == 0 {
		return s
	}
	j := 0
	for i := 1; i < len(s); i++ {
		if s[j] == s[i] {
			continue
		}
		j++
		s[j] = s[i]
	}
	return s[:j+1]
}

// returns true when val is in s assuming s is at least sorted
func contains[T Number](s []T, val T, canOptimize bool) bool {
	l := len(s)
	// empty s cannot contain values
	if l == 0 {
		return false
	}

	// s is sorted, check against first (min) and last (max) entries
	if s[0] > val || s[l-1] < val {
		return false
	}

	// for dense slices (continuous, no dups) compute offset directly
	// when both unique+nonzero flags are true
	if canOptimize {
		if ofs := int(val - s[0]); ofs >= 0 && ofs < l && s[ofs] == val {
			return true
		}
	}

	// use binary search to find value in sorted s
	_, ok := slices.BinarySearch(s, val)
	return ok
}

// containsRange returns true when slice s contains any values between
// from and to. Note that from/to do not necessarily have to be members
// themselves, but some intermediate values are. Slice s is expected
// to be sorted and from must be less than or equal to to.
func containsRange[T cmp.Ordered](s []T, from, to T) bool {
	n := len(s)
	if n == 0 {
		return false
	}
	// Case A
	if to < s[0] {
		return false
	}
	// shortcut for B.1
	if to == s[0] {
		return true
	}
	// Case E
	if from > s[n-1] {
		return false
	}
	// shortcut for D.3
	if from == s[n-1] {
		return true
	}
	// Case B-D
	// search if lower interval bound is within slice
	minv := sort.Search(n, func(i int) bool {
		return s[i] >= from
	})
	// exit when from was found (no need to check if min < n)
	if s[minv] == from {
		return true
	}
	// continue search for upper interval bound in the remainder of the slice
	maxv := sort.Search(n-minv, func(i int) bool {
		return s[i+minv] >= to
	})
	maxv += minv

	// exit when to was found (also solves case C1a)
	if maxv < n && s[maxv] == to {
		return true
	}

	// range is contained iff min < max; note that from/to do not necessarily
	// have to be members, but some intermediate values are
	return minv < maxv
}

// intersect adds all values to out that are members in both input slices x and y
func intersect[T cmp.Ordered](dst, x, y []T) []T {
	if len(x) == 0 && len(y) == 0 {
		return dst
	}
	if dst == nil {
		dst = make([]T, 0, min(len(x), len(y)))
	}
	count := 0
	for i, j, il, jl := 0, 0, len(x), len(y); i < il && j < jl; {
		if x[i] < y[j] {
			i++
			continue
		}
		if x[i] > y[j] {
			j++
			continue
		}
		if count > 0 {
			// skip duplicates
			last := dst[count-1]
			if last == x[i] {
				i++
				continue
			}
			if last == y[j] {
				j++
				continue
			}
		}
		if i == il || j == jl {
			break
		}
		if x[i] == y[j] {
			dst = append(dst, x[i])
			count++
			i++
			j++
		}
	}
	return dst
}

func mergeUnique[T cmp.Ordered](s []T, v ...T) []T {
	ls, lv := len(s), len(v)
	// extend cap(s) if necessary
	if cap(s) < ls+lv {
		tmp := make([]T, ls, ls+lv)
		copy(tmp, s)
		s = tmp
	}
	s = s[:ls+lv]

	// fast path (append only)
	if ls == 0 {
		copy(s, v)
		return s
	}

	// merge backward
	// skip duplicate values (note: v does not contain duplicates at this point!)
	in1, in2, out := ls-1, lv-1, ls+lv-1
	for in2 >= 0 {
		// insert new vals as long as they are larger or all old vals have been
		// copied (i.e. every new val is smaller than the first old val)
		for in2 >= 0 && (in1 < 0 || s[in1] < v[in2]) {
			s[out] = v[in2]
			in2--
			out--
		}

		// insert old vals as long as they are strictly larger
		for in1 >= 0 && (in2 < 0 || s[in1] > v[in2]) {
			s[out] = s[in1]
			in1--
			out--
		}

		// skip duplicates in v
		for in1 >= 0 && in2 >= 0 && s[in1] == v[in2] {
			in2--
		}
	}

	// when duplicates were dropped, close the gap at slice front
	for in1 >= 0 {
		s[out] = s[in1]
		in1--
		out--
	}
	s = s[out+1:]

	return s
}

func intersectRange[T cmp.Ordered](dst, s []T, from, to T) []T {
	start, _ := slices.BinarySearch(s, from)
	if start == len(s) {
		return dst
	}
	end, ok := slices.BinarySearch(s[start:], to)
	if ok {
		end++
	}
	if dst == nil || cap(dst) < end {
		dst = make([]T, end)
	}
	dst = dst[:end]
	copy(dst, s[start:start+end])
	return dst
}
