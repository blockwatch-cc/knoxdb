// Copyright (c) 2020 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

package vec

import (
	"sort"
)

func MatchInt32Equal(src []int32, val int32, bits, mask *BitSet) *BitSet {
	bits = ensureBitfieldSize(bits, len(src))
	bits.cnt = int(matchInt32Equal(src, val, bits.Bytes()))
	return bits
}

func MatchInt32NotEqual(src []int32, val int32, bits, mask *BitSet) *BitSet {
	bits = ensureBitfieldSize(bits, len(src))
	bits.cnt = int(matchInt32NotEqual(src, val, bits.Bytes()))
	return bits
}

func MatchInt32LessThan(src []int32, val int32, bits, mask *BitSet) *BitSet {
	bits = ensureBitfieldSize(bits, len(src))
	bits.cnt = int(matchInt32LessThan(src, val, bits.Bytes()))
	return bits
}

func MatchInt32LessThanEqual(src []int32, val int32, bits, mask *BitSet) *BitSet {
	bits = ensureBitfieldSize(bits, len(src))
	bits.cnt = int(matchInt32LessThanEqual(src, val, bits.Bytes()))
	return bits
}

func MatchInt32GreaterThan(src []int32, val int32, bits, mask *BitSet) *BitSet {
	bits = ensureBitfieldSize(bits, len(src))
	bits.cnt = int(matchInt32GreaterThan(src, val, bits.Bytes()))
	return bits
}

func MatchInt32GreaterThanEqual(src []int32, val int32, bits, mask *BitSet) *BitSet {
	bits = ensureBitfieldSize(bits, len(src))
	bits.cnt = int(matchInt32GreaterThanEqual(src, val, bits.Bytes()))
	return bits
}

func MatchInt32Between(src []int32, a, b int32, bits, mask *BitSet) *BitSet {
	bits = ensureBitfieldSize(bits, len(src))
	bits.cnt = int(matchInt32Between(src, a, b, bits.Bytes()))
	return bits
}

var Int32 = struct {
	Sort          func([]int32) []int32
	Unique        func([]int32) []int32
	RemoveZeros   func([]int32) []int32
	AddUnique     func([]int32, int32) []int32
	Remove        func([]int32, int32) []int32
	Contains      func([]int32, int32) bool
	Index         func([]int32, int32, int) int
	MinMax        func([]int32) (int32, int32)
	ContainsRange func([]int32, int32, int32) bool
	Intersect     func([]int32, []int32, []int32) []int32
	MatchEqual    func([]int32, int32, *BitSet, *BitSet) *BitSet
}{
	Sort: func(s []int32) []int32 {
		return Int32Sorter(s).Sort()
	},
	Unique: func(s []int32) []int32 {
		return UniqueInt32Slice(s)
	},
	RemoveZeros: func(s []int32) []int32 {
		s, _ = int32RemoveZeros(s)
		return s
	},
	AddUnique: func(s []int32, v int32) []int32 {
		s, _ = int32AddUnique(s, v)
		return s
	},
	Remove: func(s []int32, v int32) []int32 {
		s, _ = int32Remove(s, v)
		return s
	},
	Contains: func(s []int32, v int32) bool {
		return int32Contains(s, v)
	},
	Index: func(s []int32, v int32, last int) int {
		return int32Index(s, v, last)
	},
	MinMax: func(s []int32) (int32, int32) {
		return int32MinMax(s)
	},
	ContainsRange: func(s []int32, from, to int32) bool {
		return int32ContainsRange(s, from, to)
	},
	Intersect: func(x, y, out []int32) []int32 {
		return IntersectSortedInt32(x, y, out)
	},
	MatchEqual: func(s []int32, val int32, bits, mask *BitSet) *BitSet {
		return MatchInt32Equal(s, val, bits, mask)
	},
}

func int32AddUnique(s []int32, val int32) ([]int32, bool) {
	idx := int32Index(s, val, 0)
	if idx > -1 {
		return s, false
	}
	s = append(s, val)
	Int32Sorter(s).Sort()
	return s, true
}

func int32Remove(s []int32, val int32) ([]int32, bool) {
	idx := int32Index(s, val, 0)
	if idx < 0 {
		return s, false
	}
	s = append(s[:idx], s[idx+1:]...)
	return s, true
}

func int32RemoveZeros(s []int32) ([]int32, int) {
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

func int32Contains(s []int32, val int32) bool {
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

func int32Index(s []int32, val int32, last int) int {
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

func int32MinMax(s []int32) (int32, int32) {
	var min, max int32

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
func int32ContainsRange(s []int32, from, to int32) bool {
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
