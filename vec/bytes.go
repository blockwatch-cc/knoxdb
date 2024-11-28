// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package vec

import (
	"bytes"
	"sort"

	"golang.org/x/exp/slices"
)

func MatchBytesEqual(src [][]byte, val []byte, bits, mask *Bitset) *Bitset {
	bits = ensureBitfieldSize(bits, len(src))
	bits.cnt = int(matchBytesEqualGeneric(src, val, bits.Bytes(), mask.Bytes()))
	return bits
}

func MatchBytesNotEqual(src [][]byte, val []byte, bits, mask *Bitset) *Bitset {
	bits = ensureBitfieldSize(bits, len(src))
	bits.cnt = int(matchBytesNotEqualGeneric(src, val, bits.Bytes(), mask.Bytes()))
	return bits
}

func MatchBytesLessThan(src [][]byte, val []byte, bits, mask *Bitset) *Bitset {
	bits = ensureBitfieldSize(bits, len(src))
	bits.cnt = int(matchBytesLessThanGeneric(src, val, bits.Bytes(), mask.Bytes()))
	return bits
}

func MatchBytesLessThanEqual(src [][]byte, val []byte, bits, mask *Bitset) *Bitset {
	bits = ensureBitfieldSize(bits, len(src))
	bits.cnt = int(matchBytesLessThanEqualGeneric(src, val, bits.Bytes(), mask.Bytes()))
	return bits
}

func MatchBytesGreaterThan(src [][]byte, val []byte, bits, mask *Bitset) *Bitset {
	bits = ensureBitfieldSize(bits, len(src))
	bits.cnt = int(matchBytesGreaterThanGeneric(src, val, bits.Bytes(), mask.Bytes()))
	return bits
}

func MatchBytesGreaterThanEqual(src [][]byte, val []byte, bits, mask *Bitset) *Bitset {
	bits = ensureBitfieldSize(bits, len(src))
	bits.cnt = int(matchBytesGreaterThanEqualGeneric(src, val, bits.Bytes(), mask.Bytes()))
	return bits
}

func MatchBytesBetween(src [][]byte, a, b []byte, bits, mask *Bitset) *Bitset {
	bits = ensureBitfieldSize(bits, len(src))
	bits.cnt = int(matchBytesBetweenGeneric(src, a, b, bits.Bytes(), mask.Bytes()))
	return bits
}

var Bytes = struct {
	Sort          func([][]byte) [][]byte
	Unique        func([][]byte) [][]byte
	RemoveZeros   func([][]byte) [][]byte
	AddUnique     func([][]byte, []byte) [][]byte
	Insert        func([][]byte, int, ...[]byte) [][]byte
	Remove        func([][]byte, []byte) [][]byte
	Contains      func([][]byte, []byte) bool
	Index         func([][]byte, []byte, int) int
	MinMax        func([][]byte) ([]byte, []byte)
	ContainsRange func([][]byte, []byte, []byte) bool
	Intersect     func([][]byte, [][]byte, [][]byte) [][]byte
	MatchEqual    func([][]byte, []byte, *Bitset, *Bitset) *Bitset
}{
	Sort: func(s [][]byte) [][]byte {
		return BytesSorter(s).Sort()
	},
	Unique: func(s [][]byte) [][]byte {
		return UniqueBytesSlice(s)
	},
	RemoveZeros: func(s [][]byte) [][]byte {
		s, _ = bytesRemoveZeros(s)
		return s
	},
	AddUnique: func(s [][]byte, v []byte) [][]byte {
		s, _ = bytesAddUnique(s, v)
		return s
	},
	Insert: func(s [][]byte, k int, v ...[]byte) [][]byte {
		return bytesInsert(s, k, v...)
	},
	Remove: func(s [][]byte, v []byte) [][]byte {
		s, _ = bytesRemove(s, v)
		return s
	},
	Contains: func(s [][]byte, v []byte) bool {
		return bytesContains(s, v)
	},
	Index: func(s [][]byte, v []byte, last int) int {
		return bytesIndex(s, v, last)
	},
	MinMax: func(s [][]byte) ([]byte, []byte) {
		return bytesMinMax(s)
	},
	ContainsRange: func(s [][]byte, from, to []byte) bool {
		return bytesContainsRange(s, from, to)
	},
	Intersect: func(x, y, out [][]byte) [][]byte {
		return IntersectSortedBytes(x, y, out)
	},
	MatchEqual: func(s [][]byte, val []byte, bits, mask *Bitset) *Bitset {
		return MatchBytesEqual(s, val, bits, mask)
	},
}

func bytesAddUnique(s [][]byte, val []byte) ([][]byte, bool) {
	idx, ok := slices.BinarySearchFunc(s, val, bytes.Compare)
	if ok {
		return s, false
	}
	return bytesInsert(s, idx, val), true
}

func bytesInsert(s [][]byte, k int, vs ...[]byte) [][]byte {
	if n := len(s) + len(vs); n <= cap(s) {
		s2 := s[:n]
		copy(s2[k+len(vs):], s[k:])
		for i, v := range vs {
			s2[k+i] = make([]byte, len(v))
			copy(s2[k+i], v)
		}
		return s2
	}
	s2 := make([][]byte, len(s)+len(vs))
	copy(s2, s[:k])
	for i, v := range vs {
		s2[k+i] = make([]byte, len(v))
		copy(s2[k+i], v)
	}
	copy(s2[k+len(vs):], s[k:])
	return s2
}

func bytesRemove(s [][]byte, val []byte) ([][]byte, bool) {
	idx, ok := slices.BinarySearchFunc(s, val, bytes.Compare)
	if !ok {
		return s, false
	}
	s = append(s[:idx], s[idx+1:]...)
	return s, true
}

func bytesRemoveZeros(s [][]byte) ([][]byte, int) {
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

// Contains cgekcs if a sparse sorted slice contains a value val.
func bytesContains(s [][]byte, val []byte) bool {
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
	if i < len(s) && bytes.Equal(s[i], val) {
		return true
	}

	return false
}

func bytesIndex(s [][]byte, val []byte, last int) int {
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
	if idx < l && bytes.Equal(s[idx], val) {
		return idx + last
	}
	return -1
}

func bytesMinMax(s [][]byte) ([]byte, []byte) {
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
func bytesContainsRange(s [][]byte, from, to []byte) bool {
	n := len(s)
	if n == 0 {
		return false
	}
	if len(from) == 0 {
		return true
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
	if max < n && bytes.Equal(s[max], to) {
		return true
	}

	// range is contained iff min < max; note that from/to do not necessarily
	// have to be members, but some intermediate values are
	return min < max
}

type BytesSorter [][]byte

func (s BytesSorter) Sort() [][]byte {
	if !sort.IsSorted(s) {
		sort.Sort(s)
	}
	return s
}

func (s BytesSorter) Len() int           { return len(s) }
func (s BytesSorter) Less(i, j int) bool { return bytes.Compare(s[i], s[j]) < 0 }
func (s BytesSorter) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }

func UniqueBytesSlice(a [][]byte) [][]byte {
	if len(a) == 0 {
		return a
	}
	b := make([][]byte, len(a))
	copy(b, a)
	BytesSorter(b).Sort()
	j := 0
	for i := 1; i < len(b); i++ {
		if bytes.Equal(b[j], b[i]) {
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

func IntersectSortedBytes(x, y, out [][]byte) [][]byte {
	if out == nil {
		out = make([][]byte, 0, min(len(x), len(y)))
	}
	count := 0
	for i, j, il, jl := 0, 0, len(x), len(y); i < il && j < jl; {
		if bytes.Compare(x[i], y[j]) < 0 {
			i++
			continue
		}
		if bytes.Compare(x[i], y[j]) > 0 {
			j++
			continue
		}
		if count > 0 {
			// skip duplicates
			last := out[count-1]
			if bytes.Equal(last, x[i]) {
				i++
				continue
			}
			if bytes.Equal(last, y[j]) {
				j++
				continue
			}
		}
		if i == il || j == jl {
			break
		}
		if bytes.Equal(x[i], y[j]) {
			out = append(out, x[i])
			count++
			i++
			j++
		}
	}
	return out
}
