// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package num

import (
	"fmt"
	"sort"
)

func Int256Sort(a []Int256) []Int256 {
	sort.Slice(a, func(i, j int) bool { return a[i].Lt(a[j]) })
	return a
}

func Int256Contains(s []Int256, val Int256) bool {
	// empty s cannot contain values
	if len(s) == 0 {
		return false
	}

	// s is sorted, check against first (min) and last (max) entries
	if s[0].Cmp(val) > 0 {
		return false
	}
	if s[len(s)-1].Cmp(val) < 0 {
		return false
	}

	// use binary search to find value in sorted s
	i := sort.Search(len(s), func(i int) bool { return s[i].Cmp(val) >= 0 })
	return i < len(s) && s[i].Eq(val)
}

func Int256ContainsRange(s []Int256, from, to Int256) bool {
	n := len(s)
	if n == 0 {
		return false
	}
	if len(from) == 0 {
		return true
	}
	// Case A
	if v := to.Cmp(s[0]); v < 0 {
		return false
	} else if v == 0 {
		// shortcut for B.1
		return true
	}
	// Case E
	if v := from.Cmp(s[n-1]); v > 0 {
		return false
	} else if v == 0 {
		// shortcut for D.3
		return true
	}
	// Case B-D
	// search if lower interval bound is within slice
	min := sort.Search(n, func(i int) bool {
		return s[i].Cmp(from) >= 0
	})
	// exit when from was found (no need to check if min < n)
	if s[min].Cmp(from) == 0 {
		return true
	}
	// continue search for upper interval bound in the remainder of the slice
	max := sort.Search(n-min, func(i int) bool {
		return s[i+min].Cmp(to) >= 0
	})
	max = max + min

	// exit when to was found (also solves case C1a)
	if max < n && s[max].Eq(to) {
		return true
	}

	// range is contained iff min < max; note that from/to do not necessarily
	// have to be members, but some intermediate values are
	return min < max
}

func Int256Intersect(a, b []Int256) []Int256 {
	if a == nil || b == nil {
		return nil
	}
	out := make([]Int256, 0, min(len(a), len(b)))
	count := 0
	for i, j, la, lb := 0, 0, len(a), len(b); i < la && j < lb; {
		c := a[i].Cmp(b[j])
		if c < 0 {
			i++
			continue
		}
		if c > 0 {
			j++
			continue
		}
		if count > 0 {
			// skip duplicates
			last := out[count-1]
			if last.Eq(a[i]) {
				i++
				continue
			}
			if last.Eq(b[j]) {
				j++
				continue
			}
		}
		if i == la || j == lb {
			break
		}
		if a[i].Eq(b[j]) {
			out = append(out, a[i])
			count++
			i++
			j++
		}
	}
	return out
}

func Int256Union(a, b []Int256) []Int256 {
	if a == nil {
		return b
	}
	if b == nil {
		return a
	}

	// alloc result
	la, lb := len(a), len(b)
	s := make([]Int256, la, la+lb)
	copy(s, a)
	s = s[:la+lb]

	// fast path (append only)
	if la == 0 {
		copy(s, b)
		return s
	}

	// skip duplicate values (note: v does not contain duplicates at this point!)
	in1, in2, out := la-1, lb-1, la+lb-1
	for in2 >= 0 {
		fmt.Printf("in1=%d in2=%d out=%d\n", in1, in2, out)
		// insert new vals as long as they are larger or all old vals have been
		// copied (i.e. every new val is smaller than the first old val)
		for in2 >= 0 && (in1 < 0 || s[in1].Cmp(b[in2]) < 0) {
			s[out] = b[in2]
			in2--
			out--
		}

		// insert old vals as long as they are strictly larger
		for in1 >= 0 && (in2 < 0 || s[in1].Cmp(b[in2]) > 0) {
			s[out] = s[in1]
			in1--
			out--
		}

		// skip duplicates in v
		for in1 >= 0 && in2 >= 0 && s[in1].Eq(b[in2]) {
			in2--
		}
	}

	// when duplicates were dropped, close the gap at slice front
	for in1 >= 0 {
		s[out] = s[in1]
		in1--
		out--
	}
	s = s[out+1:]

	return s
}

func Int256Difference(a, b []Int256) []Int256 {
	if len(b) == 0 {
		return a
	}
	if a == nil {
		return nil
	}

	out := make([]Int256, len(a))
	var j, k int
	for _, v := range a {
		if v.Eq(b[k]) {
			k++
			continue
		}
		out[j] = v
		j++
	}
	return out[:j]
}
