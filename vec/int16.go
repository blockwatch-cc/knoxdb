// Copyright (c) 2020 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

package vec

import (
	"sort"
)

func MatchInt16Equal(src []int16, val int16, bits, mask *Bitset) *Bitset {
	bits = ensureBitfieldSize(bits, len(src))
	bits.cnt = int(matchInt16Equal(src, val, bits.Bytes()))
	return bits
}

func MatchInt16NotEqual(src []int16, val int16, bits, mask *Bitset) *Bitset {
	bits = ensureBitfieldSize(bits, len(src))
	bits.cnt = int(matchInt16NotEqual(src, val, bits.Bytes()))
	return bits
}

func MatchInt16LessThan(src []int16, val int16, bits, mask *Bitset) *Bitset {
	bits = ensureBitfieldSize(bits, len(src))
	bits.cnt = int(matchInt16LessThan(src, val, bits.Bytes()))
	return bits
}

func MatchInt16LessThanEqual(src []int16, val int16, bits, mask *Bitset) *Bitset {
	bits = ensureBitfieldSize(bits, len(src))
	bits.cnt = int(matchInt16LessThanEqual(src, val, bits.Bytes()))
	return bits
}

func MatchInt16GreaterThan(src []int16, val int16, bits, mask *Bitset) *Bitset {
	bits = ensureBitfieldSize(bits, len(src))
	bits.cnt = int(matchInt16GreaterThan(src, val, bits.Bytes()))
	return bits
}

func MatchInt16GreaterThanEqual(src []int16, val int16, bits, mask *Bitset) *Bitset {
	bits = ensureBitfieldSize(bits, len(src))
	bits.cnt = int(matchInt16GreaterThanEqual(src, val, bits.Bytes()))
	return bits
}

func MatchInt16Between(src []int16, a, b int16, bits, mask *Bitset) *Bitset {
	bits = ensureBitfieldSize(bits, len(src))
	bits.cnt = int(matchInt16Between(src, a, b, bits.Bytes()))
	return bits
}

var Int16 = struct {
	Sort          func([]int16) []int16
	Unique        func([]int16) []int16
	RemoveZeros   func([]int16) []int16
	AddUnique     func([]int16, int16) []int16
	Insert        func([]int16, int, ...int16) []int16
	Remove        func([]int16, int16) []int16
	Contains      func([]int16, int16) bool
	Index         func([]int16, int16, int) int
	MinMax        func([]int16) (int16, int16)
	ContainsRange func([]int16, int16, int16) bool
	Intersect     func([]int16, []int16, []int16) []int16
	MatchEqual    func([]int16, int16, *Bitset, *Bitset) *Bitset
}{
	Sort: func(s []int16) []int16 {
		return Int16Sorter(s).Sort()
	},
	Unique: func(s []int16) []int16 {
		return UniqueInt16Slice(s)
	},
	RemoveZeros: func(s []int16) []int16 {
		s, _ = int16RemoveZeros(s)
		return s
	},
	AddUnique: func(s []int16, v int16) []int16 {
		s, _ = int16AddUnique(s, v)
		return s
	},
	Insert: func(s []int16, k int, v ...int16) []int16 {
		return int16Insert(s, k, v...)
	},
	Remove: func(s []int16, v int16) []int16 {
		s, _ = int16Remove(s, v)
		return s
	},
	Contains: func(s []int16, v int16) bool {
		return int16Contains(s, v)
	},
	Index: func(s []int16, v int16, last int) int {
		return int16Index(s, v, last)
	},
	MinMax: func(s []int16) (int16, int16) {
		return int16MinMax(s)
	},
	ContainsRange: func(s []int16, from, to int16) bool {
		return int16ContainsRange(s, from, to)
	},
	Intersect: func(x, y, out []int16) []int16 {
		return IntersectSortedInt16(x, y, out)
	},
	MatchEqual: func(s []int16, val int16, bits, mask *Bitset) *Bitset {
		return MatchInt16Equal(s, val, bits, mask)
	},
}

func int16AddUnique(s []int16, val int16) ([]int16, bool) {
	idx := int16Index(s, val, 0)
	if idx > -1 {
		return s, false
	}
	s = append(s, val)
	Int16Sorter(s).Sort()
	return s, true
}

func int16Insert(s []int16, k int, vs ...int16) []int16 {
	if n := len(s) + len(vs); n <= cap(s) {
		s2 := s[:n]
		copy(s2[k+len(vs):], s[k:])
		copy(s2[k:], vs)
		return s2
	}
	s2 := make([]int16, len(s)+len(vs))
	copy(s2, s[:k])
	copy(s2[k:], vs)
	copy(s2[k+len(vs):], s[k:])
	return s2
}

func int16Remove(s []int16, val int16) ([]int16, bool) {
	idx := int16Index(s, val, 0)
	if idx < 0 {
		return s, false
	}
	s = append(s[:idx], s[idx+1:]...)
	return s, true
}

func int16RemoveZeros(s []int16) ([]int16, int) {
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

func int16Contains(s []int16, val int16) bool {
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

func int16Index(s []int16, val int16, last int) int {
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

func int16MinMax(s []int16) (int16, int16) {
	var min, max int16

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
func int16ContainsRange(s []int16, from, to int16) bool {
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
