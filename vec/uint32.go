// Copyright (c) 2020 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

package vec

import (
	"sort"

	"golang.org/x/exp/slices"
)

func MatchUint32Equal(src []uint32, val uint32, bits, mask *Bitset) *Bitset {
	bits = ensureBitfieldSize(bits, len(src))
	bits.cnt = int(matchUint32Equal(src, val, bits.Bytes()))
	return bits
}

func MatchUint32NotEqual(src []uint32, val uint32, bits, mask *Bitset) *Bitset {
	bits = ensureBitfieldSize(bits, len(src))
	bits.cnt = int(matchUint32NotEqual(src, val, bits.Bytes()))
	return bits
}

func MatchUint32LessThan(src []uint32, val uint32, bits, mask *Bitset) *Bitset {
	bits = ensureBitfieldSize(bits, len(src))
	bits.cnt = int(matchUint32LessThan(src, val, bits.Bytes()))
	return bits
}

func MatchUint32LessThanEqual(src []uint32, val uint32, bits, mask *Bitset) *Bitset {
	bits = ensureBitfieldSize(bits, len(src))
	bits.cnt = int(matchUint32LessThanEqual(src, val, bits.Bytes()))
	return bits
}

func MatchUint32GreaterThan(src []uint32, val uint32, bits, mask *Bitset) *Bitset {
	bits = ensureBitfieldSize(bits, len(src))
	bits.cnt = int(matchUint32GreaterThan(src, val, bits.Bytes()))
	return bits
}

func MatchUint32GreaterThanEqual(src []uint32, val uint32, bits, mask *Bitset) *Bitset {
	bits = ensureBitfieldSize(bits, len(src))
	bits.cnt = int(matchUint32GreaterThanEqual(src, val, bits.Bytes()))
	return bits
}

func MatchUint32Between(src []uint32, a, b uint32, bits, mask *Bitset) *Bitset {
	bits = ensureBitfieldSize(bits, len(src))
	bits.cnt = int(matchUint32Between(src, a, b, bits.Bytes()))
	return bits
}

var Uint32 = struct {
	Sort          func([]uint32) []uint32
	Unique        func([]uint32) []uint32
	RemoveZeros   func([]uint32) []uint32
	AddUnique     func([]uint32, uint32) []uint32
	Insert        func([]uint32, int, ...uint32) []uint32
	Remove        func([]uint32, uint32) []uint32
	Contains      func([]uint32, uint32) bool
	Index         func([]uint32, uint32, int) int
	MinMax        func([]uint32) (uint32, uint32)
	ContainsRange func([]uint32, uint32, uint32) bool
	Intersect     func([]uint32, []uint32, []uint32) []uint32
	MatchEqual    func([]uint32, uint32, *Bitset, *Bitset) *Bitset
}{
	Sort: func(s []uint32) []uint32 {
		return Uint32Sorter(s).Sort()
	},
	Unique: func(s []uint32) []uint32 {
		return UniqueUint32Slice(s)
	},
	RemoveZeros: func(s []uint32) []uint32 {
		s, _ = uint32RemoveZeros(s)
		return s
	},
	AddUnique: func(s []uint32, v uint32) []uint32 {
		s, _ = uint32AddUnique(s, v)
		return s
	},
	Insert: func(s []uint32, k int, v ...uint32) []uint32 {
		return uint32Insert(s, k, v...)
	},
	Remove: func(s []uint32, v uint32) []uint32 {
		s, _ = uint32Remove(s, v)
		return s
	},
	Contains: func(s []uint32, v uint32) bool {
		return uint32Contains(s, v)
	},
	Index: func(s []uint32, v uint32, last int) int {
		return uint32Index(s, v, last)
	},
	MinMax: func(s []uint32) (uint32, uint32) {
		return uint32MinMax(s)
	},
	ContainsRange: func(s []uint32, from, to uint32) bool {
		return uint32ContainsRange(s, from, to)
	},
	Intersect: func(x, y, out []uint32) []uint32 {
		return IntersectSortedUint32(x, y, out)
	},
	MatchEqual: func(s []uint32, val uint32, bits, mask *Bitset) *Bitset {
		return MatchUint32Equal(s, val, bits, mask)
	},
}

func uint32AddUnique(s []uint32, val uint32) ([]uint32, bool) {
	idx, ok := slices.BinarySearch(s, val)
	if ok {
		return s, false
	}
	return uint32Insert(s, idx, val), true
}

func uint32Insert(s []uint32, k int, vs ...uint32) []uint32 {
	if n := len(s) + len(vs); n <= cap(s) {
		s2 := s[:n]
		copy(s2[k+len(vs):], s[k:])
		copy(s2[k:], vs)
		return s2
	}
	s2 := make([]uint32, len(s)+len(vs))
	copy(s2, s[:k])
	copy(s2[k:], vs)
	copy(s2[k+len(vs):], s[k:])
	return s2
}

func uint32Remove(s []uint32, val uint32) ([]uint32, bool) {
	idx, ok := slices.BinarySearch(s, val)
	if !ok {
		return s, false
	}
	s = append(s[:idx], s[idx+1:]...)
	return s, true
}

func uint32RemoveZeros(s []uint32) ([]uint32, int) {
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

func uint32Contains(s []uint32, val uint32) bool {
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

func uint32Index(s []uint32, val uint32, last int) int {
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

func uint32MinMax(s []uint32) (uint32, uint32) {
	var min, max uint32

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
func uint32ContainsRange(s []uint32, from, to uint32) bool {
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
