// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package num

import (
	"sort"

	"blockwatch.cc/knoxdb/vec"
)

func MatchInt64Equal(src []int64, val int64, bits, mask *vec.Bitset) *vec.Bitset {
	bits = bits.Grow(len(src))
	bits.ResetCount(int(matchInt64Equal(src, val, bits.Bytes())))
	return bits
}

func MatchInt64NotEqual(src []int64, val int64, bits, mask *vec.Bitset) *vec.Bitset {
	bits = bits.Grow(len(src))
	bits.ResetCount(int(matchInt64NotEqual(src, val, bits.Bytes())))
	return bits
}

func MatchInt64LessThan(src []int64, val int64, bits, mask *vec.Bitset) *vec.Bitset {
	bits = bits.Grow(len(src))
	bits.ResetCount(int(matchInt64LessThan(src, val, bits.Bytes())))
	return bits
}

func MatchInt64LessThanEqual(src []int64, val int64, bits, mask *vec.Bitset) *vec.Bitset {
	bits = bits.Grow(len(src))
	bits.ResetCount(int(matchInt64LessThanEqual(src, val, bits.Bytes())))
	return bits
}

func MatchInt64GreaterThan(src []int64, val int64, bits, mask *vec.Bitset) *vec.Bitset {
	bits = bits.Grow(len(src))
	bits.ResetCount(int(matchInt64GreaterThan(src, val, bits.Bytes())))
	return bits
}

func MatchInt64GreaterThanEqual(src []int64, val int64, bits, mask *vec.Bitset) *vec.Bitset {
	bits = bits.Grow(len(src))
	bits.ResetCount(int(matchInt64GreaterThanEqual(src, val, bits.Bytes())))
	return bits
}

func MatchInt64Between(src []int64, a, b int64, bits, mask *vec.Bitset) *vec.Bitset {
	bits = bits.Grow(len(src))
	bits.ResetCount(int(matchInt64Between(src, a, b, bits.Bytes())))
	return bits
}

var Int64 = struct {
	Sort          func([]int64) []int64
	Unique        func([]int64) []int64
	RemoveZeros   func([]int64) []int64
	AddUnique     func([]int64, int64) []int64
	Insert        func([]int64, int, ...int64) []int64
	Remove        func([]int64, int64) []int64
	Contains      func([]int64, int64) bool
	Index         func([]int64, int64, int) int
	MinMax        func([]int64) (int64, int64)
	ContainsRange func([]int64, int64, int64) bool
	Intersect     func([]int64, []int64, []int64) []int64
	MatchEqual    func([]int64, int64, *vec.Bitset, *vec.Bitset) *vec.Bitset
}{
	Sort: func(s []int64) []int64 {
		return Int64Sorter(s).Sort()
	},
	Unique: func(s []int64) []int64 {
		return UniqueInt64Slice(s)
	},
	RemoveZeros: func(s []int64) []int64 {
		s, _ = int64RemoveZeros(s)
		return s
	},
	AddUnique: func(s []int64, v int64) []int64 {
		s, _ = int64AddUnique(s, v)
		return s
	},
	Insert: func(s []int64, k int, v ...int64) []int64 {
		return int64Insert(s, k, v...)
	},
	Remove: func(s []int64, v int64) []int64 {
		s, _ = int64Remove(s, v)
		return s
	},
	Contains: func(s []int64, v int64) bool {
		return int64Contains(s, v)
	},
	Index: func(s []int64, v int64, last int) int {
		return int64Index(s, v, last)
	},
	MinMax: func(s []int64) (int64, int64) {
		return int64MinMax(s)
	},
	ContainsRange: func(s []int64, from, to int64) bool {
		return int64ContainsRange(s, from, to)
	},
	Intersect: func(x, y, out []int64) []int64 {
		return IntersectSortedInt64(x, y, out)
	},
	MatchEqual: func(s []int64, val int64, bits, mask *vec.Bitset) *vec.Bitset {
		return MatchInt64Equal(s, val, bits, mask)
	},
}

func int64AddUnique(s []int64, val int64) ([]int64, bool) {
	idx := int64Index(s, val, 0)
	if idx > -1 {
		return s, false
	}
	s = append(s, val)
	Int64Sorter(s).Sort()
	return s, true
}

func int64Insert(s []int64, k int, vs ...int64) []int64 {
	if n := len(s) + len(vs); n <= cap(s) {
		s2 := s[:n]
		copy(s2[k+len(vs):], s[k:])
		copy(s2[k:], vs)
		return s2
	}
	s2 := make([]int64, len(s)+len(vs))
	copy(s2, s[:k])
	copy(s2[k:], vs)
	copy(s2[k+len(vs):], s[k:])
	return s2
}

func int64Remove(s []int64, val int64) ([]int64, bool) {
	idx := int64Index(s, val, 0)
	if idx < 0 {
		return s, false
	}
	s = append(s[:idx], s[idx+1:]...)
	return s, true
}

func int64RemoveZeros(s []int64) ([]int64, int) {
	var n int
	for i, v := range s {
		if v == 0 {
			continue
		}
		s[n] = s[i]
		n++
	}
	s = s[:n]
	return s, n
}

func int64Contains(s []int64, val int64) bool {
	// empty s cannot contain values
	if len(s) == 0 {
		return false
	}

	// s is sorted, check against first (min) and last (max) entries
	if s[0] > val || s[len(s)-1] < val {
		return false
	}

	// for dense slices (continuous, no dups) compute offset directly
	if ofs := int(val - s[0]); ofs >= 0 && ofs < len(s) && s[ofs] == val {
		return true
	}

	// use binary search to find value in sorted s
	i := sort.Search(len(s), func(i int) bool { return s[i] >= val })
	if i < len(s) && s[i] == val {
		return true
	}

	return false
}

func int64Index(s []int64, val int64, last int) int {
	if len(s) <= last {
		return -1
	}

	// search for value in slice starting at last index
	slice := s[last:]
	l := len(slice)
	min, max := slice[0], slice[l-1]
	if val < min || val > max {
		return -1
	}

	// for dense slices (values are continuous) compute offset directly
	if l == int(max-min)+1 {
		return int(val-min) + last
	}

	// for sparse slices, use binary search (slice is sorted)
	idx := sort.Search(l, func(i int) bool { return slice[i] >= val })
	if idx < l && slice[idx] == val {
		return idx + last
	}
	return -1
}

func int64MinMax(s []int64) (int64, int64) {
	var min, max int64

	switch l := len(s); l {
	case 0:
		// nothing
	case 1:
		min, max = s[0], s[0]
	default:
		// If there is more than one element, then initialize min and max
		if s[0] > s[1] {
			max = s[0]
			min = s[1]
		} else {
			max = s[1]
			min = s[0]
		}

		for i := 2; i < l; i++ {
			if s[i] > max {
				max = s[i]
			} else if s[i] < min {
				min = s[i]
			}
		}
	}

	return min, max
}

// ContainsRange returns true when slice s contains any values between
// from and to. Note that from/to do not necessarily have to be members
// themselves, but some intermediate values are. Slice s is expected
// to be sorted and from must be less than or equal to to.
func int64ContainsRange(s []int64, from, to int64) bool {
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
	min := sort.Search(n, func(i int) bool {
		return s[i] >= from
	})
	// exit when from was found (no need to check if min < n)
	if s[min] == from {
		return true
	}
	// continue search for upper interval bound in the remainder of the slice
	max := sort.Search(n-min, func(i int) bool {
		return s[i+min] >= to
	})
	max = max + min

	// exit when to was found (also solves case C1a)
	if max < n && s[max] == to {
		return true
	}

	// range is contained iff min < max; note that from/to do not necessarily
	// have to be members, but some intermediate values are
	return min < max
}
