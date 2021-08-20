// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package vec

import (
	"sort"
)

func MatchUint64Equal(src []uint64, val uint64, bits, mask *Bitset) *Bitset {
	bits = ensureBitfieldSize(bits, len(src))
	bits.cnt = int(matchUint64Equal(src, val, bits.Bytes()))
	return bits
}

func MatchUint64NotEqual(src []uint64, val uint64, bits, mask *Bitset) *Bitset {
	bits = ensureBitfieldSize(bits, len(src))
	bits.cnt = int(matchUint64NotEqual(src, val, bits.Bytes()))
	return bits
}

func MatchUint64LessThan(src []uint64, val uint64, bits, mask *Bitset) *Bitset {
	bits = ensureBitfieldSize(bits, len(src))
	bits.cnt = int(matchUint64LessThan(src, val, bits.Bytes()))
	return bits
}

func MatchUint64LessThanEqual(src []uint64, val uint64, bits, mask *Bitset) *Bitset {
	bits = ensureBitfieldSize(bits, len(src))
	bits.cnt = int(matchUint64LessThanEqual(src, val, bits.Bytes()))
	return bits
}

func MatchUint64GreaterThan(src []uint64, val uint64, bits, mask *Bitset) *Bitset {
	bits = ensureBitfieldSize(bits, len(src))
	bits.cnt = int(matchUint64GreaterThan(src, val, bits.Bytes()))
	return bits
}

func MatchUint64GreaterThanEqual(src []uint64, val uint64, bits, mask *Bitset) *Bitset {
	bits = ensureBitfieldSize(bits, len(src))
	bits.cnt = int(matchUint64GreaterThanEqual(src, val, bits.Bytes()))
	return bits
}

func MatchUint64Between(src []uint64, a, b uint64, bits, mask *Bitset) *Bitset {
	bits = ensureBitfieldSize(bits, len(src))
	bits.cnt = int(matchUint64Between(src, a, b, bits.Bytes()))
	return bits
}

var Uint64 = struct {
	Sort          func([]uint64) []uint64
	Unique        func([]uint64) []uint64
	RemoveZeros   func([]uint64) []uint64
	AddUnique     func([]uint64, uint64) []uint64
	Insert        func([]uint64, int, ...uint64) []uint64
	Remove        func([]uint64, uint64) []uint64
	Contains      func([]uint64, uint64) bool
	Index         func([]uint64, uint64, int) int
	MinMax        func([]uint64) (uint64, uint64)
	ContainsRange func([]uint64, uint64, uint64) bool
	Intersect     func([]uint64, []uint64, []uint64) []uint64
	MatchEqual    func([]uint64, uint64, *Bitset, *Bitset) *Bitset
}{
	Sort: func(s []uint64) []uint64 {
		return Uint64Sorter(s).Sort()
	},
	Unique: func(s []uint64) []uint64 {
		return UniqueUint64Slice(s)
	},
	RemoveZeros: func(s []uint64) []uint64 {
		s, _ = uint64RemoveZeros(s)
		return s
	},
	AddUnique: func(s []uint64, v uint64) []uint64 {
		s, _ = uint64AddUnique(s, v)
		return s
	},
	Insert: func(s []uint64, k int, v ...uint64) []uint64 {
		return uint64Insert(s, k, v...)
	},
	Remove: func(s []uint64, v uint64) []uint64 {
		s, _ = uint64Remove(s, v)
		return s
	},
	Contains: func(s []uint64, v uint64) bool {
		return uint64Contains(s, v)
	},
	Index: func(s []uint64, v uint64, last int) int {
		return uint64Index(s, v, last)
	},
	MinMax: func(s []uint64) (uint64, uint64) {
		return uint64MinMax(s)
	},
	ContainsRange: func(s []uint64, from, to uint64) bool {
		return uint64ContainsRange(s, from, to)
	},
	Intersect: func(x, y, out []uint64) []uint64 {
		if out == nil {
			out = make([]uint64, 0, max(len(x), len(y)))
		}
		return IntersectSortedUint64(x, y, out)
	},
	MatchEqual: func(s []uint64, val uint64, bits, mask *Bitset) *Bitset {
		return MatchUint64Equal(s, val, bits, mask)
	},
}

func uint64AddUnique(s []uint64, val uint64) ([]uint64, bool) {
	idx := uint64Index(s, val, 0)
	if idx > -1 {
		return s, false
	}
	s = append(s, val)
	Uint64Sorter(s).Sort()
	return s, true
}

func uint64Insert(s []uint64, k int, vs ...uint64) []uint64 {
	if n := len(s) + len(vs); n <= cap(s) {
		s2 := s[:n]
		copy(s2[k+len(vs):], s[k:])
		copy(s2[k:], vs)
		return s2
	}
	s2 := make([]uint64, len(s)+len(vs))
	copy(s2, s[:k])
	copy(s2[k:], vs)
	copy(s2[k+len(vs):], s[k:])
	return s2
}

func uint64Remove(s []uint64, val uint64) ([]uint64, bool) {
	idx := uint64Index(s, val, 0)
	if idx < 0 {
		return s, false
	}
	s = append(s[:idx], s[idx+1:]...)
	return s, true
}

func uint64RemoveZeros(s []uint64) ([]uint64, int) {
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

func uint64Contains(s []uint64, val uint64) bool {
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

func uint64Index(s []uint64, val uint64, last int) int {
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

func uint64MinMax(s []uint64) (uint64, uint64) {
	var min, max uint64

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
func uint64ContainsRange(s []uint64, from, to uint64) bool {
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
