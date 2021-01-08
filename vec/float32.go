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

type Float32Slice []float32

func (s Float32Slice) Sort() Float32Slice {
	Float32Sorter(s).Sort()
	return s
}

func (s Float32Slice) Less(i, j int) bool { return s[i] < s[j] }
func (s Float32Slice) Len() int           { return len(s) }
func (s Float32Slice) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }

func (s *Float32Slice) AddUnique(val float32) bool {
	idx := s.Index(val, 0)
	if idx > -1 {
		return false
	}
	*s = append(*s, val)
	s.Sort()
	return true
}

func (s *Float32Slice) Remove(val float32) bool {
	idx := s.Index(val, 0)
	if idx < 0 {
		return false
	}
	*s = append((*s)[:idx], (*s)[idx+1:]...)
	return true
}

func (s Float32Slice) Contains(val float32) bool {
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

func (s Float32Slice) Index(val float32, last int) int {
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

func (s Float32Slice) MinMax() (float32, float32) {
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
func (s Float32Slice) ContainsRange(from, to float32) bool {
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

func (s Float32Slice) MatchEqual(val float32, bits, mask *BitSet) *BitSet {
	return MatchFloat32Equal(s, val, bits, mask)
}
