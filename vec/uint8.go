// Copyright (c) 2020 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

package vec

import (
	"sort"
)

func MatchUint8Equal(src []uint8, val uint8, bits, mask *BitSet) *BitSet {
	bits = ensureBitfieldSize(bits, len(src))
	bits.cnt = int(matchUint8Equal(src, val, bits.Bytes()))
	return bits
}

func MatchUint8NotEqual(src []uint8, val uint8, bits, mask *BitSet) *BitSet {
	bits = ensureBitfieldSize(bits, len(src))
	bits.cnt = int(matchUint8NotEqual(src, val, bits.Bytes()))
	return bits
}

func MatchUint8LessThan(src []uint8, val uint8, bits, mask *BitSet) *BitSet {
	bits = ensureBitfieldSize(bits, len(src))
	bits.cnt = int(matchUint8LessThan(src, val, bits.Bytes()))
	return bits
}

func MatchUint8LessThanEqual(src []uint8, val uint8, bits, mask *BitSet) *BitSet {
	bits = ensureBitfieldSize(bits, len(src))
	bits.cnt = int(matchUint8LessThanEqual(src, val, bits.Bytes()))
	return bits
}

func MatchUint8GreaterThan(src []uint8, val uint8, bits, mask *BitSet) *BitSet {
	bits = ensureBitfieldSize(bits, len(src))
	bits.cnt = int(matchUint8GreaterThan(src, val, bits.Bytes()))
	return bits
}

func MatchUint8GreaterThanEqual(src []uint8, val uint8, bits, mask *BitSet) *BitSet {
	bits = ensureBitfieldSize(bits, len(src))
	bits.cnt = int(matchUint8GreaterThanEqual(src, val, bits.Bytes()))
	return bits
}

func MatchUint8Between(src []uint8, a, b uint8, bits, mask *BitSet) *BitSet {
	bits = ensureBitfieldSize(bits, len(src))
	bits.cnt = int(matchUint8Between(src, a, b, bits.Bytes()))
	return bits
}

var Uint8 = struct {
	Sort          func([]uint8) []uint8
	Unique        func([]uint8) []uint8
	RemoveZeros   func([]uint8) []uint8
	AddUnique     func([]uint8, uint8) []uint8
	Remove        func([]uint8, uint8) []uint8
	Contains      func([]uint8, uint8) bool
	Index         func([]uint8, uint8, int) int
	MinMax        func([]uint8) (uint8, uint8)
	ContainsRange func([]uint8, uint8, uint8) bool
	Intersect     func([]uint8, []uint8, []uint8) []uint8
	MatchEqual    func([]uint8, uint8, *BitSet, *BitSet) *BitSet
}{
	Sort: func(s []uint8) []uint8 {
		Uint8Sorter(s).Sort()
		return s
	},
	Unique: func(s []uint8) []uint8 {
		UniqueUint8Slice(s)
		return s
	},
	RemoveZeros: func(s []uint8) []uint8 {
		s, _ = uint8RemoveZeros(s)
		return s
	},
	AddUnique: func(s []uint8, v uint8) []uint8 {
		s, _ = uint8AddUnique(s, v)
		return s
	},
	Remove: func(s []uint8, v uint8) []uint8 {
		s, _ = uint8Remove(s, v)
		return s
	},
	Contains: func(s []uint8, v uint8) bool {
		return uint8Contains(s, v)
	},
	Index: func(s []uint8, v uint8, last int) int {
		return uint8Index(s, v, last)
	},
	MinMax: func(s []uint8) (uint8, uint8) {
		return uint8MinMax(s)
	},
	ContainsRange: func(s []uint8, from, to uint8) bool {
		return uint8ContainsRange(s, from, to)
	},
	Intersect: func(x, y, out []uint8) []uint8 {
		return IntersectSortedUint8(x, y, out)
	},
	MatchEqual: func(s []uint8, val uint8, bits, mask *BitSet) *BitSet {
		return MatchUint8Equal(s, val, bits, mask)
	},
}

func uint8AddUnique(s []uint8, val uint8) ([]uint8, bool) {
	idx := uint8Index(s, val, 0)
	if idx > -1 {
		return s, false
	}
	s = append(s, val)
	Uint8Sorter(s).Sort()
	return s, true
}

func uint8Remove(s []uint8, val uint8) ([]uint8, bool) {
	idx := uint8Index(s, val, 0)
	if idx < 0 {
		return s, false
	}
	s = append(s[:idx], s[idx+1:]...)
	return s, true
}

func uint8RemoveZeros(s []uint8) ([]uint8, int) {
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

func uint8Contains(s []uint8, val uint8) bool {
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

func uint8Index(s []uint8, val uint8, last int) int {
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

func uint8MinMax(s []uint8) (uint8, uint8) {
	var min, max uint8

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
func uint8ContainsRange(s []uint8, from, to uint8) bool {
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

	// otherwise range is contained iff min < max
	return min < max
}
