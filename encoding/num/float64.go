// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package num

import (
	"sort"

	"blockwatch.cc/knoxdb/vec"
)

func MatchFloat64Equal(src []float64, val float64, bits, mask *vec.Bitset) *vec.Bitset {
	bits = bits.Grow(len(src))
	bits.ResetCount(int(matchFloat64Equal(src, val, bits.Bytes())))
	return bits
}

func MatchFloat64NotEqual(src []float64, val float64, bits, mask *vec.Bitset) *vec.Bitset {
	bits = bits.Grow(len(src))
	bits.ResetCount(int(matchFloat64NotEqual(src, val, bits.Bytes())))
	return bits
}

func MatchFloat64LessThan(src []float64, val float64, bits, mask *vec.Bitset) *vec.Bitset {
	bits = bits.Grow(len(src))
	bits.ResetCount(int(matchFloat64LessThan(src, val, bits.Bytes())))
	return bits
}

func MatchFloat64LessThanEqual(src []float64, val float64, bits, mask *vec.Bitset) *vec.Bitset {
	bits = bits.Grow(len(src))
	bits.ResetCount(int(matchFloat64LessThanEqual(src, val, bits.Bytes())))
	return bits
}

func MatchFloat64GreaterThan(src []float64, val float64, bits, mask *vec.Bitset) *vec.Bitset {
	bits = bits.Grow(len(src))
	bits.ResetCount(int(matchFloat64GreaterThan(src, val, bits.Bytes())))
	return bits
}

func MatchFloat64GreaterThanEqual(src []float64, val float64, bits, mask *vec.Bitset) *vec.Bitset {
	bits = bits.Grow(len(src))
	bits.ResetCount(int(matchFloat64GreaterThanEqual(src, val, bits.Bytes())))
	return bits
}

func MatchFloat64Between(src []float64, a, b float64, bits, mask *vec.Bitset) *vec.Bitset {
	bits = bits.Grow(len(src))
	bits.ResetCount(int(matchFloat64Between(src, a, b, bits.Bytes())))
	return bits
}

var Float64 = struct {
	Sort          func([]float64) []float64
	Unique        func([]float64) []float64
	RemoveZeros   func([]float64) []float64
	AddUnique     func([]float64, float64) []float64
	Insert        func([]float64, int, ...float64) []float64
	Remove        func([]float64, float64) []float64
	Contains      func([]float64, float64) bool
	Index         func([]float64, float64, int) int
	MinMax        func([]float64) (float64, float64)
	ContainsRange func([]float64, float64, float64) bool
	Intersect     func([]float64, []float64, []float64) []float64
	MatchEqual    func([]float64, float64, *vec.Bitset, *vec.Bitset) *vec.Bitset
}{
	Sort: func(s []float64) []float64 {
		return Float64Sorter(s).Sort()
	},
	Unique: func(s []float64) []float64 {
		return UniqueFloat64Slice(s)
	},
	RemoveZeros: func(s []float64) []float64 {
		s, _ = float64RemoveZeros(s)
		return s
	},
	AddUnique: func(s []float64, v float64) []float64 {
		s, _ = float64AddUnique(s, v)
		return s
	},
	Insert: func(s []float64, k int, v ...float64) []float64 {
		return float64Insert(s, k, v...)
	},
	Remove: func(s []float64, v float64) []float64 {
		s, _ = float64Remove(s, v)
		return s
	},
	Contains: func(s []float64, v float64) bool {
		return float64Contains(s, v)
	},
	Index: func(s []float64, v float64, last int) int {
		return float64Index(s, v, last)
	},
	MinMax: func(s []float64) (float64, float64) {
		return float64MinMax(s)
	},
	ContainsRange: func(s []float64, from, to float64) bool {
		return float64ContainsRange(s, from, to)
	},
	Intersect: func(x, y, out []float64) []float64 {
		return IntersectSortedFloat64(x, y, out)
	},
	MatchEqual: func(s []float64, val float64, bits, mask *vec.Bitset) *vec.Bitset {
		return MatchFloat64Equal(s, val, bits, mask)
	},
}

func float64AddUnique(s []float64, val float64) ([]float64, bool) {
	idx := float64Index(s, val, 0)
	if idx > -1 {
		return s, false
	}
	s = append(s, val)
	Float64Sorter(s).Sort()
	return s, true
}

func float64Insert(s []float64, k int, vs ...float64) []float64 {
	if n := len(s) + len(vs); n <= cap(s) {
		s2 := s[:n]
		copy(s2[k+len(vs):], s[k:])
		copy(s2[k:], vs)
		return s2
	}
	s2 := make([]float64, len(s)+len(vs))
	copy(s2, s[:k])
	copy(s2[k:], vs)
	copy(s2[k+len(vs):], s[k:])
	return s2
}

func float64Remove(s []float64, val float64) ([]float64, bool) {
	idx := float64Index(s, val, 0)
	if idx < 0 {
		return s, false
	}
	s = append(s[:idx], s[idx+1:]...)
	return s, true
}

func float64RemoveZeros(s []float64) ([]float64, int) {
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

func float64Contains(s []float64, val float64) bool {
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

func float64Index(s []float64, val float64, last int) int {
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

func float64MinMax(s []float64) (float64, float64) {
	var min, max float64

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
func float64ContainsRange(s []float64, from, to float64) bool {
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
