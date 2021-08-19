// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package vec

import (
	"sort"
)

func MatchBoolEqual(src []bool, val bool, bits, mask *Bitset) *Bitset {
	bits = ensureBitfieldSize(bits, len(src))
	bits.cnt = int(matchBoolEqualGeneric(src, val, bits.Bytes()))
	return bits
}

func MatchBoolNotEqual(src []bool, val bool, bits, mask *Bitset) *Bitset {
	bits = ensureBitfieldSize(bits, len(src))
	bits.cnt = int(matchBoolNotEqualGeneric(src, val, bits.Bytes()))
	return bits
}

func MatchBoolLessThan(src []bool, val bool, bits, mask *Bitset) *Bitset {
	bits = ensureBitfieldSize(bits, len(src))
	bits.cnt = int(matchBoolLessThanGeneric(src, val, bits.Bytes()))
	return bits
}

func MatchBoolLessThanEqual(src []bool, val bool, bits, mask *Bitset) *Bitset {
	bits = ensureBitfieldSize(bits, len(src))
	bits.cnt = int(matchBoolLessThanEqualGeneric(src, val, bits.Bytes()))
	return bits
}

func MatchBoolGreaterThan(src []bool, val bool, bits, mask *Bitset) *Bitset {
	bits = ensureBitfieldSize(bits, len(src))
	bits.cnt = int(matchBoolGreaterThanGeneric(src, val, bits.Bytes()))
	return bits
}

func MatchBoolGreaterThanEqual(src []bool, val bool, bits, mask *Bitset) *Bitset {
	bits = ensureBitfieldSize(bits, len(src))
	bits.cnt = int(matchBoolGreaterThanEqualGeneric(src, val, bits.Bytes()))
	return bits
}

func MatchBoolBetween(src []bool, a, b bool, bits, mask *Bitset) *Bitset {
	bits = ensureBitfieldSize(bits, len(src))
	bits.cnt = int(matchBoolBetweenGeneric(src, a, b, bits.Bytes()))
	return bits
}

var Booleans = struct {
	Insert        func([]bool, int, ...bool) []bool
	Contains      func([]bool, bool) bool
	Index         func([]bool, bool, int) int
	MinMax        func([]bool) (bool, bool)
	ContainsRange func([]bool, bool, bool) bool
	Intersect     func([]bool, []bool, []bool) []bool
	MatchEqual    func([]bool, bool, *Bitset, *Bitset) *Bitset
}{
	Insert: func(s []bool, k int, v ...bool) []bool {
		return boolInsert(s, k, v...)
	},
	Contains: func(s []bool, v bool) bool {
		return boolContains(s, v)
	},
	Index: func(s []bool, v bool, last int) int {
		return boolIndex(s, v, last)
	},
	MinMax: func(s []bool) (bool, bool) {
		return boolMinMax(s)
	},
	ContainsRange: func(s []bool, from, to bool) bool {
		return boolContainsRange(s, from, to)
	},
	MatchEqual: func(s []bool, val bool, bits, mask *Bitset) *Bitset {
		return MatchBoolEqual(s, val, bits, mask)
	},
}

func boolInsert(s []bool, k int, vs ...bool) []bool {
	if n := len(s) + len(vs); n <= cap(s) {
		s2 := s[:n]
		copy(s2[k+len(vs):], s[k:])
		copy(s2[k:], vs)
		return s2
	}
	s2 := make([]bool, len(s)+len(vs))
	copy(s2, s[:k])
	copy(s2[k:], vs)
	copy(s2[k+len(vs):], s[k:])
	return s2
}

func boolContains(s []bool, val bool) bool {
	// empty s cannot contain values
	if len(s) == 0 {
		return false
	}

	if s[0] == val || s[len(s)-1] == val {
		return true
	}

	return false
}

func boolIndex(s []bool, val bool, last int) int {
	if len(s) <= last {
		return -1
	}

	// search for value in slice starting at last index
	slice := s[last:]
	l := len(slice)
	min, max := slice[0], slice[l-1]
	if min && !val {
		return -1
	}
	if !min && !max && val {
		return -1
	}

	// use binary search (slice is sorted)
	idx := sort.Search(l, func(i int) bool { return s[i] })
	if idx < l && s[idx] == val {
		return idx + last
	}
	return -1
}

func boolMinMax(s []bool) (bool, bool) {
	var min, max bool

	switch l := len(s); l {
	case 0:
		// nothing
	case 1:
		min, max = s[0], s[0]
	default:
		// If there is more than one element, then initialize min and max
		min, max = s[0], s[0]
		for i := 1; i < l; i++ {
			if min != max {
				break
			}
			min = min && s[i]
			max = max || s[i]
		}
	}

	return min, max
}

// ContainsRange returns true when slice s contains any values between
// from and to. Since a values are binary this only checks if either
// from or to are contained.  Slice s is expected to be sorted
// by condition false < true, and from must also be less than or equal
// to under the same condition.
func boolContainsRange(s []bool, from, to bool) bool {
	n := len(s)
	if n == 0 {
		return false
	}
	// Case A: [false,false] !E [true,true,true]
	if !to && s[0] {
		return false
	}
	// Case E: [true,true] !E [false,false,false]
	if from && !s[n-1] {
		return false
	}
	// Case B-D
	// [false,false] E [false,false,false]
	// [false,false] E [false,false,true]
	// [false,true] E [false,false,false]
	// [false,true] E [false,false,true]
	// [false,true] E [true,true,true]
	// [true,true] E [false,false,true]
	// [true,true] E [true,true,true]
	return true
}
