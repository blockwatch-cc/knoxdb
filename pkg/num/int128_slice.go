// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package num

import (
	"slices"
	"sort"
)

func Int128Sort(s []Int128) []Int128 {
	sort.Slice(s, func(i, j int) bool { return s[i].Lt(s[j]) })
	return s
}

func Int128MinMax(s []Int128) (Int128, Int128) {
	switch l := len(s); l {
	case 0:
		return ZeroInt128, ZeroInt128
	case 1:
		return s[0], s[0]
	default:
		return s[0], s[l-1]
	}
}

func Int128Unique(s []Int128) []Int128 {
	if len(s) == 0 {
		return s
	}
	sort.Slice(s, func(i, j int) bool { return s[i].Lt(s[j]) })

	j := 0
	for i := 1; i < len(s); i++ {
		if s[j].Eq(s[i]) {
			continue
		}
		j++
		// preserve the original data
		// in[i], in[j] = in[j], in[i]
		// only set what is required
		s[j] = s[i]
	}
	return s[:j+1]
}

func Int128Contains(s []Int128, val Int128) bool {
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

func Int128ContainsRange(s []Int128, from, to Int128) bool {
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

func Int128Intersect(a, b []Int128) []Int128 {
	if a == nil || b == nil {
		return nil
	}
	out := make([]Int128, 0, min(len(a), len(b)))
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

func Int128Union(a, b []Int128) []Int128 {
	if a == nil {
		return b
	}
	if b == nil {
		return a
	}

	// alloc result
	la, lb := len(a), len(b)
	s := make([]Int128, la, la+lb)
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

func Int128Difference(a, b []Int128) []Int128 {
	if len(b) == 0 {
		return a
	}
	if a == nil {
		return nil
	}

	out := make([]Int128, len(a))
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

func Int128RemoveRange(s []Int128, from, to Int128) []Int128 {
	n := len(s)
	start := sort.Search(n, func(i int) bool {
		return s[i].Cmp(from) >= 0
	})
	if start == n {
		return slices.Clone(s)
	}
	end := sort.Search(n-start, func(i int) bool {
		return s[i+start].Cmp(to) >= 0
	})
	if start+end < n && s[start+end].Eq(to) {
		end++
	}
	out := make([]Int128, n-end)
	copy(out, s[:start])
	copy(out[start:], s[start+end:])
	return out
}

func Int128IntersectRange(s []Int128, from, to Int128) []Int128 {
	if s == nil {
		return nil
	}
	n := len(s)
	start := sort.Search(n, func(i int) bool {
		return s[i].Cmp(from) >= 0
	})
	if start == n {
		return []Int128{}
	}
	end := sort.Search(n-start, func(i int) bool {
		return s[i+start].Cmp(to) >= 0
	})
	if start+end < n && s[start+end].Eq(to) {
		end++
	}
	out := make([]Int128, end)
	copy(out, s[start:start+end])
	return out
}
