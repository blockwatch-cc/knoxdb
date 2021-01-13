// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package vec

import (
	"sort"
	"strings"
)

func MatchStringsEqual(src []string, val string, bits, mask *BitSet) *BitSet {
	bits = ensureBitfieldSize(bits, len(src))
	bits.cnt = int(matchStringsEqualGeneric(src, val, bits.Bytes(), mask.Bytes()))
	return bits
}

func MatchStringsNotEqual(src []string, val string, bits, mask *BitSet) *BitSet {
	bits = ensureBitfieldSize(bits, len(src))
	bits.cnt = int(matchStringsNotEqualGeneric(src, val, bits.Bytes(), mask.Bytes()))
	return bits
}

func MatchStringsLessThan(src []string, val string, bits, mask *BitSet) *BitSet {
	bits = ensureBitfieldSize(bits, len(src))
	bits.cnt = int(matchStringsLessThanGeneric(src, val, bits.Bytes(), mask.Bytes()))
	return bits
}

func MatchStringsLessThanEqual(src []string, val string, bits, mask *BitSet) *BitSet {
	bits = ensureBitfieldSize(bits, len(src))
	bits.cnt = int(matchStringsLessThanEqualGeneric(src, val, bits.Bytes(), mask.Bytes()))
	return bits
}

func MatchStringsGreaterThan(src []string, val string, bits, mask *BitSet) *BitSet {
	bits = ensureBitfieldSize(bits, len(src))
	bits.cnt = int(matchStringsGreaterThanGeneric(src, val, bits.Bytes(), mask.Bytes()))
	return bits
}

func MatchStringsGreaterThanEqual(src []string, val string, bits, mask *BitSet) *BitSet {
	bits = ensureBitfieldSize(bits, len(src))
	bits.cnt = int(matchStringsGreaterThanEqualGeneric(src, val, bits.Bytes(), mask.Bytes()))
	return bits
}

func MatchStringsBetween(src []string, a, b string, bits, mask *BitSet) *BitSet {
	bits = ensureBitfieldSize(bits, len(src))
	bits.cnt = int(matchStringsBetweenGeneric(src, a, b, bits.Bytes(), mask.Bytes()))
	return bits
}

var Strings = struct {
	Sort          func([]string) []string
	Unique        func([]string) []string
	RemoveZeros   func([]string) []string
	AddUnique     func([]string, string) []string
	Remove        func([]string, string) []string
	Contains      func([]string, string) bool
	Index         func([]string, string, int) int
	MinMax        func([]string) (string, string)
	ContainsRange func([]string, string, string) bool
	Intersect     func([]string, []string, []string) []string
	MatchEqual    func([]string, string, *BitSet, *BitSet) *BitSet
}{
	Sort: func(s []string) []string {
		StringsSorter(s).Sort()
		return s
	},
	Unique: func(s []string) []string {
		UniqueStringSlice(s)
		return s
	},
	RemoveZeros: func(s []string) []string {
		s, _ = stringRemoveZeros(s)
		return s
	},
	AddUnique: func(s []string, v string) []string {
		s, _ = stringAddUnique(s, v)
		return s
	},
	Remove: func(s []string, v string) []string {
		s, _ = stringRemove(s, v)
		return s
	},
	Contains: func(s []string, v string) bool {
		return stringContains(s, v)
	},
	Index: func(s []string, v string, last int) int {
		return stringIndex(s, v, last)
	},
	MinMax: func(s []string) (string, string) {
		return stringMinMax(s)
	},
	ContainsRange: func(s []string, from, to string) bool {
		return stringContainsRange(s, from, to)
	},
	Intersect: func(x, y, out []string) []string {
		return IntersectSortedStrings(x, y, out)
	},
	MatchEqual: func(s []string, val string, bits, mask *BitSet) *BitSet {
		return MatchStringsEqual(s, val, bits, mask)
	},
}

func stringAddUnique(s []string, val string) ([]string, bool) {
	idx := stringIndex(s, val, 0)
	if idx > -1 {
		return s, false
	}
	s = append(s, val)
	StringsSorter(s).Sort()
	return s, true
}

func stringRemove(s []string, val string) ([]string, bool) {
	idx := stringIndex(s, val, 0)
	if idx < 0 {
		return s, false
	}
	s = append(s[:idx], s[idx+1:]...)
	return s, true
}

func stringRemoveZeros(s []string) ([]string, int) {
	var n int
	for i, v := range s {
		if len(v) == 0 {
			continue
		}
		s[n] = s[i]
		n++
	}
	s = s[:n]
	return s, n
}

func stringContains(s []string, val string) bool {
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

func stringIndex(s []string, val string, last int) int {
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

func stringMinMax(s []string) (string, string) {
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
func stringContainsRange(s []string, from, to string) bool {
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

type StringsSorter []string

func (s StringsSorter) Sort() {
	if !sort.IsSorted(s) {
		sort.Sort(s)
	}
}

func (s StringsSorter) Len() int           { return len(s) }
func (s StringsSorter) Less(i, j int) bool { return s[i] < s[j] }
func (s StringsSorter) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }

func UniqueStringSlice(a []string) []string {
	if len(a) == 0 {
		return a
	}
	b := make([]string, len(a))
	copy(b, a)
	StringsSorter(b).Sort()
	j := 0
	for i := 1; i < len(b); i++ {
		if b[j] == b[i] {
			continue
		}
		j++
		// preserve the original data
		// in[i], in[j] = in[j], in[i]
		// only set what is required
		b[j] = b[i]
	}
	return b[:j+1]
}

func IntersectSortedStrings(x, y, out []string) []string {
	if out == nil {
		out = make([]string, 0, min(len(x), len(y)))
	}
	count := 0
	for i, j, il, jl := 0, 0, len(x), len(y); i < il && j < jl; {
		if strings.Compare(x[i], y[j]) < 0 {
			i++
			continue
		}
		if strings.Compare(x[i], y[j]) > 0 {
			j++
			continue
		}
		if count > 0 {
			// skip duplicates
			last := out[count-1]
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
			out = append(out, x[i])
			count++
			i++
			j++
		}
	}
	return out
}
