// Copyright (c) 2020 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

package vec

import (
	"sort"
)

func MatchFloat32Equal(src []float32, val float32, bits, mask *BitSet) *BitSet {
	bits = ensureBitfieldSize(bits, len(src))
	bits.cnt = int(matchFloat32Equal(src, val, bits.Bytes()))
	return bits
}

func MatchFloat32NotEqual(src []float32, val float32, bits, mask *BitSet) *BitSet {
	bits = ensureBitfieldSize(bits, len(src))
	bits.cnt = int(matchFloat32NotEqual(src, val, bits.Bytes()))
	return bits
}

func MatchFloat32LessThan(src []float32, val float32, bits, mask *BitSet) *BitSet {
	bits = ensureBitfieldSize(bits, len(src))
	bits.cnt = int(matchFloat32LessThan(src, val, bits.Bytes()))
	return bits
}

func MatchFloat32LessThanEqual(src []float32, val float32, bits, mask *BitSet) *BitSet {
	bits = ensureBitfieldSize(bits, len(src))
	bits.cnt = int(matchFloat32LessThanEqual(src, val, bits.Bytes()))
	return bits
}

func MatchFloat32GreaterThan(src []float32, val float32, bits, mask *BitSet) *BitSet {
	bits = ensureBitfieldSize(bits, len(src))
	bits.cnt = int(matchFloat32GreaterThan(src, val, bits.Bytes()))
	return bits
}

func MatchFloat32GreaterThanEqual(src []float32, val float32, bits, mask *BitSet) *BitSet {
	bits = ensureBitfieldSize(bits, len(src))
	bits.cnt = int(matchFloat32GreaterThanEqual(src, val, bits.Bytes()))
	return bits
}

func MatchFloat32Between(src []float32, a, b float32, bits, mask *BitSet) *BitSet {
	bits = ensureBitfieldSize(bits, len(src))
	bits.cnt = int(matchFloat32Between(src, a, b, bits.Bytes()))
	return bits
}

var Float32 = struct {
	Sort          func([]float32) []float32
	Unique        func([]float32) []float32
	RemoveZeros   func([]float32) []float32
	AddUnique     func([]float32, float32) []float32
	Remove        func([]float32, float32) []float32
	Contains      func([]float32, float32) bool
	Index         func([]float32, float32, int) int
	MinMax        func([]float32) (float32, float32)
	ContainsRange func([]float32, float32, float32) bool
	Intersect     func([]float32, []float32, []float32) []float32
	MatchEqual    func([]float32, float32, *BitSet, *BitSet) *BitSet
}{
	Sort: func(s []float32) []float32 {
		Float32Sorter(s).Sort()
		return s
	},
	Unique: func(s []float32) []float32 {
		UniqueFloat32Slice(s)
		return s
	},
	RemoveZeros: func(s []float32) []float32 {
		s, _ = float32RemoveZeros(s)
		return s
	},
	AddUnique: func(s []float32, v float32) []float32 {
		s, _ = float32AddUnique(s, v)
		return s
	},
	Remove: func(s []float32, v float32) []float32 {
		s, _ = float32Remove(s, v)
		return s
	},
	Contains: func(s []float32, v float32) bool {
		return float32Contains(s, v)
	},
	Index: func(s []float32, v float32, last int) int {
		return float32Index(s, v, last)
	},
	MinMax: func(s []float32) (float32, float32) {
		return float32MinMax(s)
	},
	ContainsRange: func(s []float32, from, to float32) bool {
		return float32ContainsRange(s, from, to)
	},
	Intersect: func(x, y, out []float32) []float32 {
		return IntersectSortedFloat32(x, y, out)
	},
	MatchEqual: func(s []float32, val float32, bits, mask *BitSet) *BitSet {
		return MatchFloat32Equal(s, val, bits, mask)
	},
}

func float32AddUnique(s []float32, val float32) ([]float32, bool) {
	idx := float32Index(s, val, 0)
	if idx > -1 {
		return s, false
	}
	s = append(s, val)
	Float32Sorter(s).Sort()
	return s, true
}

func float32Remove(s []float32, val float32) ([]float32, bool) {
	idx := float32Index(s, val, 0)
	if idx < 0 {
		return s, false
	}
	s = append(s[:idx], s[idx+1:]...)
	return s, true
}

func float32RemoveZeros(s []float32) ([]float32, int) {
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

func float32Contains(s []float32, val float32) bool {
	// empty s cannot contain values
	if len(s) == 0 {
		return false
	}

	// s is sorted, check against first (min) and last (max) entries
	if s[0] > val || s[len(s)-1] < val {
		return false
	}

	// use binary search to find value in sorted s
	i := sort.Search(len(s), func(i int) bool { return s[i] >= val })
	if i < len(s) && s[i] == val {
		return true
	}

	return false
}

func float32Index(s []float32, val float32, last int) int {
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

func float32MinMax(s []float32) (float32, float32) {
	var min, max float32

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
func float32ContainsRange(s []float32, from, to float32) bool {
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
