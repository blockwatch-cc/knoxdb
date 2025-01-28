// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package pack

var InvalidRange = Range{0, 1<<32 - 1}

// index range within a pack used for scans
type Range [2]uint32

func (r Range) IsValid() bool {
	return r == InvalidRange
}

func (r Range) IsFull(n int) bool {
	return r[0] == 0 && int(r[1]) == n
}

func (r Range) Union(s Range) Range {
	if !r.IsValid() {
		return s
	}
	if !s.IsValid() {
		return r
	}
	return Range{min(r[0], s[0]), max(r[1], s[1])}
}

func (r Range) Intersect(s Range) Range {
	if !r.IsValid() {
		return s
	}
	if !s.IsValid() {
		return r
	}
	start := max(r[0], s[0])
	end := min(r[1], s[1])
	if start > end {
		return InvalidRange
	}
	return Range{start, end}
}
