// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package vec

import (
	"sort"
	"strings"
)

func MatchStringsEqual(src []string, val string, bits, mask *BitSet) *BitSet {
	bits = ensureBitfieldSize(bits, len(src))
	bits.cnt = matchStringsEqualGeneric(src, val, bits.Bytes(), mask.Bytes())
	return bits
}

func MatchStringsNotEqual(src []string, val string, bits, mask *BitSet) *BitSet {
	bits = ensureBitfieldSize(bits, len(src))
	bits.cnt = matchStringsNotEqualGeneric(src, val, bits.Bytes(), mask.Bytes())
	return bits
}

func MatchStringsLessThan(src []string, val string, bits, mask *BitSet) *BitSet {
	bits = ensureBitfieldSize(bits, len(src))
	bits.cnt = matchStringsLessThanGeneric(src, val, bits.Bytes(), mask.Bytes())
	return bits
}

func MatchStringsLessThanEqual(src []string, val string, bits, mask *BitSet) *BitSet {
	bits = ensureBitfieldSize(bits, len(src))
	bits.cnt = matchStringsLessThanEqualGeneric(src, val, bits.Bytes(), mask.Bytes())
	return bits
}

func MatchStringsGreaterThan(src []string, val string, bits, mask *BitSet) *BitSet {
	bits = ensureBitfieldSize(bits, len(src))
	bits.cnt = matchStringsGreaterThanGeneric(src, val, bits.Bytes(), mask.Bytes())
	return bits
}

func MatchStringsGreaterThanEqual(src []string, val string, bits, mask *BitSet) *BitSet {
	bits = ensureBitfieldSize(bits, len(src))
	bits.cnt = matchStringsGreaterThanEqualGeneric(src, val, bits.Bytes(), mask.Bytes())
	return bits
}

func MatchStringsBetween(src []string, a, b string, bits, mask *BitSet) *BitSet {
	bits = ensureBitfieldSize(bits, len(src))
	bits.cnt = matchStringsBetweenGeneric(src, a, b, bits.Bytes(), mask.Bytes())
	return bits
}

type StringSlice []string

func (s StringSlice) Sort() StringSlice {
	sort.Slice(s, func(i, j int) bool { return strings.Compare(s[i], s[j]) < 0 })
	return s
}

func (s StringSlice) Less(i, j int) bool { return strings.Compare(s[i], s[j]) < 0 }
func (s StringSlice) Len() int           { return len(s) }
func (s StringSlice) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }

func (s StringSlice) Contains(val string) bool {
	// empty s cannot contain values
	if len(s) == 0 {
		return false
	}

	// s is sorted, check against first (min) and last (max) entries
	if strings.Compare(s[0], val) > 0 {
		return false
	}
	if strings.Compare(s[len(s)-1], val) < 0 {
		return false
	}
	// use binary search to find value in sorted s
	i := sort.Search(len(s), func(i int) bool { return strings.Compare(s[i], val) >= 0 })
	if i < len(s) && strings.Compare(s[i], val) == 0 {
		return true
	}

	return false
}

func (s StringSlice) Index(val string, last int) int {
	if len(s) <= last {
		return -1
	}

	// search for value in slice starting at last index
	slice := s[last:]
	l := len(slice)
	min, max := slice[0], slice[l-1]
	if strings.Compare(min, val) > 0 {
		return -1
	}
	if strings.Compare(max, val) < 0 {
		return -1
	}

	// use binary search (slice is sorted)
	idx := sort.Search(l, func(i int) bool { return strings.Compare(s[i], val) >= 0 })
	if idx < l && strings.Compare(s[idx], val) == 0 {
		return idx + last
	}
	return -1
}

func (s StringSlice) MinMax() (string, string) {
	var min, max string

	switch l := len(s); l {
	case 0:
		// nothing
	case 1:
		min, max = s[0], s[0]
	default:
		// If there is more than one element, then initialize min and max
		if strings.Compare(s[0], s[1]) > 0 {
			max = s[0]
			min = s[1]
		} else {
			max = s[1]
			min = s[0]
		}

		for i := 2; i < l; i++ {
			if strings.Compare(s[i], max) > 0 {
				max = s[i]
			} else if strings.Compare(s[i], min) < 0 {
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
func (s StringSlice) ContainsRange(from, to string) bool {
	n := len(s)
	if n == 0 {
		return false
	}
	// Case A
	if v := strings.Compare(to, s[0]); v < 0 {
		return false
	} else if v == 0 {
		// shortcut for B.1
		return true
	}
	// Case E
	if v := strings.Compare(from, s[n-1]); v > 0 {
		return false
	} else if v == 0 {
		// shortcut for D.3
		return true
	}
	// Case B-D
	// search if lower interval bound is within slice
	min := sort.Search(n, func(i int) bool {
		return strings.Compare(s[i], from) >= 0
	})
	// exit when from was found (no need to check if min < n)
	if strings.Compare(s[min], from) == 0 {
		return true
	}
	// continue search for upper interval bound in the remainder of the slice
	max := sort.Search(n-min, func(i int) bool {
		return strings.Compare(s[i+min], to) >= 0
	})
	max = max + min

	// exit when to was found (also solves case C1a)
	if max < n && strings.Compare(s[max], to) == 0 {
		return true
	}

	// range is contained iff min < max; note that from/to do not necessarily
	// have to be members, but some intermediate values are
	return min < max
}
