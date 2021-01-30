// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package vec

import (
	"bytes"
	"sort"
)

func MatchBytesEqual(src [][]byte, val []byte, bits, mask *BitSet) *BitSet {
	bits = ensureBitfieldSize(bits, len(src))
	bits.cnt = int(matchBytesEqualGeneric(src, val, bits.Bytes(), mask.Bytes()))
	return bits
}

func MatchBytesNotEqual(src [][]byte, val []byte, bits, mask *BitSet) *BitSet {
	bits = ensureBitfieldSize(bits, len(src))
	bits.cnt = int(matchBytesNotEqualGeneric(src, val, bits.Bytes(), mask.Bytes()))
	return bits
}

func MatchBytesLessThan(src [][]byte, val []byte, bits, mask *BitSet) *BitSet {
	bits = ensureBitfieldSize(bits, len(src))
	bits.cnt = int(matchBytesLessThanGeneric(src, val, bits.Bytes(), mask.Bytes()))
	return bits
}

func MatchBytesLessThanEqual(src [][]byte, val []byte, bits, mask *BitSet) *BitSet {
	bits = ensureBitfieldSize(bits, len(src))
	bits.cnt = int(matchBytesLessThanEqualGeneric(src, val, bits.Bytes(), mask.Bytes()))
	return bits
}

func MatchBytesGreaterThan(src [][]byte, val []byte, bits, mask *BitSet) *BitSet {
	bits = ensureBitfieldSize(bits, len(src))
	bits.cnt = int(matchBytesGreaterThanGeneric(src, val, bits.Bytes(), mask.Bytes()))
	return bits
}

func MatchBytesGreaterThanEqual(src [][]byte, val []byte, bits, mask *BitSet) *BitSet {
	bits = ensureBitfieldSize(bits, len(src))
	bits.cnt = int(matchBytesGreaterThanEqualGeneric(src, val, bits.Bytes(), mask.Bytes()))
	return bits
}

func MatchBytesBetween(src [][]byte, a, b []byte, bits, mask *BitSet) *BitSet {
	bits = ensureBitfieldSize(bits, len(src))
	bits.cnt = int(matchBytesBetweenGeneric(src, a, b, bits.Bytes(), mask.Bytes()))
	return bits
}

type ByteSlice [][]byte

func (s ByteSlice) Sort() ByteSlice {
	sort.Slice(s, func(i, j int) bool { return bytes.Compare(s[i], s[j]) < 0 })
	return s
}

func (s ByteSlice) Less(i, j int) bool { return bytes.Compare(s[i], s[j]) < 0 }
func (s ByteSlice) Len() int           { return len(s) }
func (s ByteSlice) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }

func (s *ByteSlice) AddUnique(val []byte) bool {
	idx := s.Index(val, 0)
	if idx > -1 {
		return false
	}
	*s = append(*s, val)
	s.Sort()
	return true
}

func (s *ByteSlice) Remove(val []byte) bool {
	idx := s.Index(val, 0)
	if idx < 0 {
		return false
	}
	*s = append((*s)[:idx], (*s)[idx+1:]...)
	return true
}

// Contains cgekcs if a sparse sorted slice contains a value val.
func (s ByteSlice) Contains(val []byte) bool {
	// empty s cannot contain values
	if len(s) == 0 {
		return false
	}

	// s is sorted, check against first (min) and last (max) entries
	if bytes.Compare(s[0], val) > 0 {
		return false
	}
	if bytes.Compare(s[len(s)-1], val) < 0 {
		return false
	}
	// use binary search to find value in sorted s
	i := sort.Search(len(s), func(i int) bool { return bytes.Compare(s[i], val) >= 0 })
	if i < len(s) && bytes.Compare(s[i], val) == 0 {
		return true
	}

	return false
}

func (s ByteSlice) Index(val []byte, last int) int {
	if len(s) <= last {
		return -1
	}

	// search for value in slice starting at last index
	slice := s[last:]
	l := len(slice)
	min, max := slice[0], slice[l-1]
	if bytes.Compare(min, val) > 0 {
		return -1
	}
	if bytes.Compare(max, val) < 0 {
		return -1
	}

	// use binary search (slice is sorted)
	idx := sort.Search(l, func(i int) bool { return bytes.Compare(s[i], val) >= 0 })
	if idx < l && bytes.Compare(s[idx], val) == 0 {
		return idx + last
	}
	return -1
}

func (s ByteSlice) MinMax() ([]byte, []byte) {
	var min, max []byte

	switch l := len(s); l {
	case 0:
		// nothing
	case 1:
		min, max = s[0], s[0]
	default:
		// If there is more than one element, then initialize min and max
		if bytes.Compare(s[0], s[1]) > 0 {
			max = s[0]
			min = s[1]
		} else {
			max = s[1]
			min = s[0]
		}

		for i := 2; i < l; i++ {
			if bytes.Compare(s[i], max) > 0 {
				max = s[i]
			} else if bytes.Compare(s[i], min) < 0 {
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
func (s ByteSlice) ContainsRange(from, to []byte) bool {
	n := len(s)
	if n == 0 {
		return false
	}
	// Case A
	if v := bytes.Compare(to, s[0]); v < 0 {
		return false
	} else if v == 0 {
		// shortcut for B.1
		return true
	}
	// Case E
	if v := bytes.Compare(from, s[n-1]); v > 0 {
		return false
	} else if v == 0 {
		// shortcut for D.3
		return true
	}
	// Case B-D
	// search if lower interval bound is within slice
	min := sort.Search(n, func(i int) bool {
		return bytes.Compare(s[i], from) >= 0
	})
	// exit when from was found (no need to check if min < n)
	if bytes.Compare(s[min], from) == 0 {
		return true
	}
	// continue search for upper interval bound in the remainder of the slice
	max := sort.Search(n-min, func(i int) bool {
		return bytes.Compare(s[i+min], to) >= 0
	})
	max = max + min

	// exit when to was found (also solves case C1a)
	if max < n && bytes.Compare(s[max], to) == 0 {
		return true
	}

	// range is contained iff min < max; note that from/to do not necessarily
	// have to be members, but some intermediate values are
	return min < max
}
