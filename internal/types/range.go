// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package types

import "blockwatch.cc/knoxdb/internal/arena"

const maxSeq = 1 << 17 // 128k

var (
	InvalidRange = Range{0, 1<<32 - 1}
	constRange   = make([]uint32, maxSeq) // 128k const selection vector
)

func init() {
	for i := range uint32(maxSeq) {
		constRange[i] = i
	}
}

// index range within a pack used for scans as [a,b)
type Range [2]uint32

func NewRange[T Integer | int | uint](a, b T) Range {
	return Range{uint32(a), uint32(b)}
}

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

func (r Range) AsSelection() []uint32 {
	return MakeSelection(r[0], r[1])
}

func MakeSelection[T uint32 | uint64 | int | uint](a, b T) []uint32 {
	n := b - a
	sel := arena.AllocUint32(int(n))[:n]
	if b <= T(maxSeq) {
		copy(sel, constRange[a:])
	} else {
		for i := range uint32(n) {
			sel[i] = uint32(a) + i
		}
	}
	return sel
}

func NegateSelection(s []uint32, sz int) []uint32 {
	if s == nil {
		return []uint32{} // nil = all -> neg = empty
	}
	if len(s) == 0 {
		return MakeSelection(0, sz) // empty = none -> neg = all
	}
	sz -= len(s)
	neg := arena.AllocUint32(sz)[:sz]
	var (
		i uint32
		j int
	)
	for _, v := range s {
		for i < v {
			neg[j] = i
			j++
			i++
		}
		i++
	}
	for j < sz {
		neg[j] = i
		j++
		i++
	}
	return neg
}
