// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package vec

import (
	"sort"
	"time"
)

// Note: time is stored as int64 (unix nanosec) in columns, so we don't need
// special comparison funcs here

type TimeSlice []time.Time

func (s TimeSlice) Sort() TimeSlice {
	sort.Slice(s, func(i, j int) bool { return s[i].Before(s[j]) })
	return s
}

func (s TimeSlice) Less(i, j int) bool { return s[i].Before(s[j]) }
func (s TimeSlice) Len() int           { return len(s) }
func (s TimeSlice) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }

func (s TimeSlice) Contains(val time.Time) bool {
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

func (s TimeSlice) Index(val time.Time, last int) int {
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

func (s TimeSlice) MinMax() (time.Time, time.Time) {
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

func (s TimeSlice) ContainsRange(from, to time.Time) bool {
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
