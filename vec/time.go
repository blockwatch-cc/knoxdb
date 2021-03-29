// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package vec

import (
	"sort"
	"time"
)

func MatchTimeEqual(src []time.Time, val time.Time, bits, mask *Bitset) *Bitset {
	bits = ensureBitfieldSize(bits, len(src))
	bits.cnt = int(matchTimeEqualGeneric(src, val, bits.Bytes(), mask.Bytes()))
	return bits
}

func MatchTimeNotEqual(src []time.Time, val time.Time, bits, mask *Bitset) *Bitset {
	bits = ensureBitfieldSize(bits, len(src))
	bits.cnt = int(matchTimeNotEqualGeneric(src, val, bits.Bytes(), mask.Bytes()))
	return bits
}

func MatchTimeLessThan(src []time.Time, val time.Time, bits, mask *Bitset) *Bitset {
	bits = ensureBitfieldSize(bits, len(src))
	bits.cnt = int(matchTimeLessThanGeneric(src, val, bits.Bytes(), mask.Bytes()))
	return bits
}

func MatchTimeLessThanEqual(src []time.Time, val time.Time, bits, mask *Bitset) *Bitset {
	bits = ensureBitfieldSize(bits, len(src))
	bits.cnt = int(matchTimeLessThanEqualGeneric(src, val, bits.Bytes(), mask.Bytes()))
	return bits
}

func MatchTimeGreaterThan(src []time.Time, val time.Time, bits, mask *Bitset) *Bitset {
	bits = ensureBitfieldSize(bits, len(src))
	bits.cnt = int(matchTimeGreaterThanGeneric(src, val, bits.Bytes(), mask.Bytes()))
	return bits
}

func MatchTimeGreaterThanEqual(src []time.Time, val time.Time, bits, mask *Bitset) *Bitset {
	bits = ensureBitfieldSize(bits, len(src))
	bits.cnt = int(matchTimeGreaterThanEqualGeneric(src, val, bits.Bytes(), mask.Bytes()))
	return bits
}

func MatchTimeBetween(src []time.Time, a, b time.Time, bits, mask *Bitset) *Bitset {
	bits = ensureBitfieldSize(bits, len(src))
	bits.cnt = int(matchTimeBetweenGeneric(src, a, b, bits.Bytes(), mask.Bytes()))
	return bits
}

// Note: time is stored as int64 (unix nanosec) in columns, so we don't need
// special comparison funcs here

var Times = struct {
	Sort          func([]time.Time) []time.Time
	Unique        func([]time.Time) []time.Time
	RemoveZeros   func([]time.Time) []time.Time
	AddUnique     func([]time.Time, time.Time) []time.Time
	Remove        func([]time.Time, time.Time) []time.Time
	Contains      func([]time.Time, time.Time) bool
	Index         func([]time.Time, time.Time, int) int
	MinMax        func([]time.Time) (time.Time, time.Time)
	ContainsRange func([]time.Time, time.Time, time.Time) bool
	Intersect     func([]time.Time, []time.Time, []time.Time) []time.Time
	MatchEqual    func([]time.Time, time.Time, *Bitset, *Bitset) *Bitset
}{
	Sort: func(s []time.Time) []time.Time {
		return TimeSorter(s).Sort()
	},
	Unique: func(s []time.Time) []time.Time {
		return UniqueTimeSlice(s)
	},
	RemoveZeros: func(s []time.Time) []time.Time {
		s, _ = timeRemoveZeros(s)
		return s
	},
	AddUnique: func(s []time.Time, v time.Time) []time.Time {
		s, _ = timeAddUnique(s, v)
		return s
	},
	Remove: func(s []time.Time, v time.Time) []time.Time {
		s, _ = timeRemove(s, v)
		return s
	},
	Contains: func(s []time.Time, v time.Time) bool {
		return timeContains(s, v)
	},
	Index: func(s []time.Time, v time.Time, last int) int {
		return timeIndex(s, v, last)
	},
	MinMax: func(s []time.Time) (time.Time, time.Time) {
		return timeMinMax(s)
	},
	ContainsRange: func(s []time.Time, from, to time.Time) bool {
		return timeContainsRange(s, from, to)
	},
	Intersect: func(x, y, out []time.Time) []time.Time {
		return IntersectSortedTime(x, y, out)
	},
	MatchEqual: func(s []time.Time, val time.Time, bits, mask *Bitset) *Bitset {
		return MatchTimeEqual(s, val, bits, mask)
	},
}

func timeAddUnique(s []time.Time, val time.Time) ([]time.Time, bool) {
	idx := timeIndex(s, val, 0)
	if idx > -1 {
		return s, false
	}
	s = append(s, val)
	TimeSorter(s).Sort()
	return s, true
}

func timeRemove(s []time.Time, val time.Time) ([]time.Time, bool) {
	idx := timeIndex(s, val, 0)
	if idx < 0 {
		return s, false
	}
	s = append(s[:idx], s[idx+1:]...)
	return s, true
}

func timeRemoveZeros(s []time.Time) ([]time.Time, int) {
	var n int
	for i, v := range s {
		if v.IsZero() {
			continue
		}
		s[n] = s[i]
		n++
	}
	s = s[:n]
	return s, n
}

func timeContains(s []time.Time, val time.Time) bool {
	// empty s cannot contain values
	if len(s) == 0 {
		return false
	}

	// s is sorted, check against first (min) and last (max) entries
	if s[0].After(val) {
		return false
	}
	if s[len(s)-1].Before(val) {
		return false
	}
	// use binary search to find value in sorted s
	i := sort.Search(len(s), func(i int) bool { return !s[i].Before(val) })
	if i < len(s) && s[i].Equal(val) {
		return true
	}

	return false
}

func timeIndex(s []time.Time, val time.Time, last int) int {
	if len(s) <= last {
		return -1
	}

	// search for value in slice starting at last index
	slice := s[last:]
	l := len(slice)
	min, max := slice[0], slice[l-1]
	if min.After(val) {
		return -1
	}
	if max.Before(val) {
		return -1
	}

	// use binary search (slice is sorted)
	idx := sort.Search(l, func(i int) bool { return !s[i].Before(val) })
	if idx < l && s[idx].Equal(val) {
		return idx + last
	}
	return -1
}

func timeMinMax(s []time.Time) (time.Time, time.Time) {
	var min, max time.Time

	switch l := len(s); l {
	case 0:
		// nothing
	case 1:
		min, max = s[0], s[0]
	default:
		// If there is more than one element, then initialize min and max
		if s[0].After(s[1]) {
			max = s[0]
			min = s[1]
		} else {
			max = s[1]
			min = s[0]
		}

		for i := 2; i < l; i++ {
			if s[i].After(max) {
				max = s[i]
			} else if s[i].Before(min) {
				min = s[i]
			}
		}
	}

	return min, max
}

func timeContainsRange(s []time.Time, from, to time.Time) bool {
	n := len(s)
	if n == 0 {
		return false
	}
	// Case A
	if to.Before(s[0]) {
		return false
	}
	// shortcut for B.1
	if to.Equal(s[0]) {
		return true
	}
	// Case E
	if from.After(s[n-1]) {
		return false
	}
	// shortcut for D.3
	if from.Equal(s[n-1]) {
		return true
	}
	// Case B-D
	// search if lower interval bound is within slice
	min := sort.Search(n, func(i int) bool {
		return !s[i].Before(from)
	})
	// exit when from was found (no need to check if min < n)
	if from.Equal(s[min]) {
		return true
	}
	// continue search for upper interval bound in the remainder of the slice
	max := sort.Search(n-min, func(i int) bool {
		return !s[i+min].Before(to)
	})
	max = max + min

	// exit when to was found (also solves case C1a)
	if max < n && to.Equal(s[max]) {
		return true
	}

	// otherwise range is contained iff min < max
	return min < max
}

type TimeSorter []time.Time

func (s TimeSorter) Sort() []time.Time {
	if !sort.IsSorted(s) {
		sort.Sort(s)
	}
	return s
}

func (s TimeSorter) Len() int           { return len(s) }
func (s TimeSorter) Less(i, j int) bool { return s[i].Before(s[j]) }
func (s TimeSorter) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }

func UniqueTimeSlice(a []time.Time) []time.Time {
	if len(a) == 0 {
		return a
	}
	b := make([]time.Time, len(a))
	copy(b, a)
	TimeSorter(b).Sort()
	j := 0
	for i := 1; i < len(b); i++ {
		if b[j].Equal(b[i]) {
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

func IntersectSortedTime(x, y, out []time.Time) []time.Time {
	if out == nil {
		out = make([]time.Time, 0, min(len(x), len(y)))
	}
	count := 0
	for i, j, il, jl := 0, 0, len(x), len(y); i < il && j < jl; {
		if x[i].Before(y[j]) {
			i++
			continue
		}
		if x[i].After(y[j]) {
			j++
			continue
		}
		if count > 0 {
			// skip duplicates
			last := out[count-1]
			if last.Equal(x[i]) {
				i++
				continue
			}
			if last.Equal(y[j]) {
				j++
				continue
			}
		}
		if i == il || j == jl {
			break
		}
		if x[i].Equal(y[j]) {
			out = append(out, x[i])
			count++
			i++
			j++
		}
	}
	return out
}
