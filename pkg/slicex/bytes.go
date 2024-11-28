// Copyright (c) 2023-2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package slicex

import (
	"bytes"
	"sort"
)

// Optimized algorithms for ordered byte slices
type OrderedBytes struct {
	NonZero bool
	Unique  bool
	Values  [][]byte
}

func NewOrderedBytes(s [][]byte) *OrderedBytes {
	if s == nil {
		s = make([][]byte, 0)
	}
	bytesSorter(s).Sort()
	return &OrderedBytes{
		Values: s,
	}
}

func UniqueBytes(s [][]byte) [][]byte {
	bytesSorter(s).Sort()
	return removeDuplicateBytes(s)
}

func (o *OrderedBytes) SetNonZero() *OrderedBytes {
	// remove zero values
	o.Values, _ = removeZeroBytes(o.Values)
	o.NonZero = true
	return o
}

func (o *OrderedBytes) SetUnique() *OrderedBytes {
	// remove duplicates
	o.Values = removeDuplicateBytes(o.Values)
	o.Unique = true
	return o
}

func (o OrderedBytes) Len() int {
	return len(o.Values)
}

func (o OrderedBytes) MinMax() ([]byte, []byte) {
	switch l := len(o.Values); l {
	case 0:
		return nil, nil
	case 1:
		return o.Values[0], o.Values[0]
	default:
		return o.Values[0], o.Values[l-1]
	}
}

func (o *OrderedBytes) Insert(val ...[]byte) *OrderedBytes {
	// remove incoming zeros
	if o.NonZero {
		val, _ = removeZeroBytes(val)
	}
	// sort incoming slice
	bytesSorter(val).Sort()

	// shortcut for empty target
	if len(o.Values) == 0 {
		if o.Unique {
			val = removeDuplicateBytes(val)
		}
		o.Values = append(o.Values, val...)
		return o
	}

	// merge slices
	o.Values = mergeBytes(o.Values, o.Unique, val...)
	return o
}

func (o *OrderedBytes) Remove(val ...[]byte) *OrderedBytes {
	if len(val) == 0 {
		return o
	}
	bytesSorter(val).Sort()
	var j, k int
	for _, v := range o.Values {
		if bytes.Equal(v, val[k]) {
			k++
			continue
		}
		o.Values[j] = v
		j++
	}
	o.Values = o.Values[:j]
	return o
}

func (o *OrderedBytes) Index(val []byte) (int, bool) {
	return indexBytes(o.Values, val, 0)
}

func (o *OrderedBytes) IndexStart(val []byte, start int) (int, bool) {
	return indexBytes(o.Values, val, start)
}

func (o OrderedBytes) Contains(val []byte) bool {
	return containsBytes(o.Values, val)
}

func (o OrderedBytes) ContainsAny(val ...[]byte) bool {
	bytesSorter(val).Sort()
	var (
		last int
		ok   bool
	)
	for _, v := range val {
		last, ok = indexBytes(o.Values, v, last)
		if ok {
			return true
		}
	}
	return false
}

func (o OrderedBytes) ContainsAll(val ...[]byte) bool {
	bytesSorter(val).Sort()
	var (
		last int
		ok   bool
	)
	for _, v := range val {
		last, ok = indexBytes(o.Values, v, last)
		if !ok {
			return false
		}
	}
	return true
}

func (o OrderedBytes) ContainsRange(from, to []byte) bool {
	return containsRangeBytes(o.Values, from, to)
}

func (o OrderedBytes) RemoveRange(from, to []byte) *OrderedBytes {
	return &OrderedBytes{
		NonZero: o.NonZero,
		Unique:  o.Unique,
		Values:  removeRangeBytes(o.Values, from, to, make([][]byte, 0)),
	}
}

func (o OrderedBytes) IntersectRange(from, to []byte) *OrderedBytes {
	return &OrderedBytes{
		NonZero: o.NonZero,
		Unique:  o.Unique,
		Values:  intersectRangeBytes(o.Values, from, to, make([][]byte, 0)),
	}
}

func (o OrderedBytes) Equal(o2 *OrderedBytes) bool {
	if o.Len() != o2.Len() {
		return false
	}
	for i := range o.Values {
		if !bytes.Equal(o.Values[i], o2.Values[i]) {
			return false
		}
	}
	return true
}

func (o OrderedBytes) Intersect(v *OrderedBytes) *OrderedBytes {
	if v == nil {
		return nil
	}
	return &OrderedBytes{
		NonZero: o.NonZero || v.NonZero,
		Unique:  o.Unique || v.Unique,
		Values:  intersectBytes(o.Values, v.Values, make([][]byte, 0)),
	}
}

func (o *OrderedBytes) Union(v *OrderedBytes) *OrderedBytes {
	if v == nil {
		return o
	}
	res := &OrderedBytes{
		NonZero: o.NonZero && v.NonZero,
		Unique:  o.Unique && v.Unique,
		Values:  make([][]byte, len(o.Values), len(o.Values)+len(v.Values)),
	}
	copy(res.Values, o.Values)
	res.Values = mergeBytes(res.Values, v.Unique, v.Values...)
	return res
}

func (o *OrderedBytes) Difference(v *OrderedBytes) *OrderedBytes {
	if v == nil {
		return o
	}
	res := &OrderedBytes{
		NonZero: o.NonZero && v.NonZero,
		Unique:  o.Unique && v.Unique,
		Values:  make([][]byte, len(o.Values)),
	}
	copy(res.Values, o.Values)
	return res.Remove(v.Values...)
}

func containsBytes(s [][]byte, val []byte) bool {
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
	return i < len(s) && bytes.Equal(s[i], val)
}

// returns where val was found or would appear
func indexBytes(s [][]byte, val []byte, last int) (int, bool) {
	if len(s) <= last {
		return len(s), false
	}

	// search for value in slice starting at last index
	slice := s[last:]
	l := len(slice)
	min, max := slice[0], slice[l-1]
	if bytes.Compare(min, val) > 0 {
		return 0, false
	}
	if bytes.Compare(max, val) < 0 {
		return l, false
	}

	// for sparse slices, use binary search (slice is sorted)
	idx := sort.Search(l, func(i int) bool { return bytes.Compare(s[i], val) >= 0 })
	return idx + last, idx < l && bytes.Equal(s[idx], val)
}

func removeZeroBytes(s [][]byte) ([][]byte, int) {
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

// assumes s is already sorted
func removeDuplicateBytes(s [][]byte) [][]byte {
	if len(s) == 0 {
		return s
	}
	j := 0
	for i := 1; i < len(s); i++ {
		if bytes.Equal(s[j], s[i]) {
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

func mergeBytes(s [][]byte, unique bool, v ...[]byte) [][]byte {
	ls, lv := len(s), len(v)
	// extend cap(s) if necessary
	if cap(s) < ls+lv {
		tmp := make([][]byte, ls, ls+lv)
		copy(tmp, s)
		s = tmp
	}
	s = s[:ls+lv]

	// fast path (append only)
	if ls == 0 {
		copy(s, v)
		return s
	}

	// merge backward
	if unique {
		// skip duplicate values (note: v does not contain duplicates at this point!)
		in1, in2, out := ls-1, lv-1, ls+lv-1
		for in2 >= 0 {
			// insert new vals as long as they are larger or all old vals have been
			// copied (i.e. every new val is smaller than the first old val)
			for in2 >= 0 && (in1 < 0 || bytes.Compare(s[in1], v[in2]) < 0) {
				s[out] = v[in2]
				in2--
				out--
			}

			// insert old vals as long as they are strictly larger
			for in1 >= 0 && (in2 < 0 || bytes.Compare(s[in1], v[in2]) > 0) {
				s[out] = s[in1]
				in1--
				out--
			}

			// skip duplicates in v
			for in1 >= 0 && in2 >= 0 && bytes.Equal(s[in1], v[in2]) {
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

	} else {
		// copy all values in order
		for in1, in2, out := ls-1, lv-1, ls+lv-1; in2 >= 0; {
			// insert new vals as long as they are larger or all old vals have been
			// copied (i.e. every new val is smaller than the first old val)
			for in2 >= 0 && (in1 < 0 || bytes.Compare(s[in1], v[in2]) < 0) {
				s[out] = v[in2]
				in2--
				out--
			}

			// insert old vals as long as they are larger (using >= instead of >
			// to copy duplicate vals as well)
			for in1 >= 0 && (in2 < 0 || bytes.Compare(s[in1], v[in2]) >= 0) {
				s[out] = s[in1]
				in1--
				out--
			}
		}
	}

	return s
}

func intersectBytes(x, y, out [][]byte) [][]byte {
	if out == nil {
		out = make([][]byte, 0, min(len(x), len(y)))
	}
	count := 0
	for i, j, il, jl := 0, 0, len(x), len(y); i < il && j < jl; {
		c := bytes.Compare(x[i], y[j])
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

// containsRangeBytes returns true when slice s contains any values between
// from and to. Note that from/to do not necessarily have to be members
// themselves, but some intermediate values are. Slice s is expected
// to be sorted and from must be less than or equal to to.
func containsRangeBytes(s [][]byte, from, to []byte) bool {
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

func removeRangeBytes(s [][]byte, from, to []byte, out [][]byte) [][]byte {
	n := len(s)
	start := sort.Search(n, func(i int) bool {
		return bytes.Compare(s[i], from) >= 0
	})
	if start == n {
		if cap(out) < n {
			out = make([][]byte, n)
		}
		out = out[:n]
		copy(out, s)
		return out
	}
	end := sort.Search(n-start, func(i int) bool {
		return bytes.Compare(s[i+start], to) >= 0
	})
	if start+end < n && bytes.Equal(s[start+end], to) {
		end++
	}
	if out == nil || cap(out) < n-end {
		out = make([][]byte, n-end)
	}
	out = out[:n-end]
	copy(out, s[:start])
	copy(out[start:], s[start+end:])
	return out
}

func intersectRangeBytes(s [][]byte, from, to []byte, out [][]byte) [][]byte {
	n := len(s)
	start := sort.Search(n, func(i int) bool {
		return bytes.Compare(s[i], from) >= 0
	})
	if start == n {
		return out
	}
	end := sort.Search(n-start, func(i int) bool {
		return bytes.Compare(s[i+start], to) >= 0
	})
	if start+end < n && bytes.Equal(s[start+end], to) {
		end++
	}
	if out == nil || cap(out) < end {
		out = make([][]byte, end)
	}
	out = out[:end]
	copy(out, s[start:start+end])
	return out
}

type bytesSorter [][]byte

func (s bytesSorter) Sort() [][]byte {
	if !sort.IsSorted(s) {
		sort.Sort(s)
	}
	return s
}

func (s bytesSorter) Len() int           { return len(s) }
func (s bytesSorter) Less(i, j int) bool { return bytes.Compare(s[i], s[j]) < 0 }
func (s bytesSorter) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }

func BytesMinMax(s [][]byte) ([]byte, []byte) {
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
