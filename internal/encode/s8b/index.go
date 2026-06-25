// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package s8b

import (
	"sort"
)

type Index interface {
	Len() int
	End() int
	Find(n int) (word int, skip int, ok bool)
}

type IndexImpl[T uint16 | uint32] struct {
	ends []T
}

func (idx IndexImpl[T]) Len() int {
	return len(idx.ends)
}

func (idx IndexImpl[T]) End() int {
	return int(idx.ends[len(idx.ends)-1])
}

func (idx IndexImpl[T]) Find(n int) (int, int, bool) {
	if n > idx.End() {
		return 0, 0, false
	}
	i := sort.Search(len(idx.ends), func(i int) bool {
		return idx.ends[i] >= T(n)
	})
	if int(idx.ends[i]) == n {
		i++
	}
	if i > 0 {
		return i, n - int(idx.ends[i-1]), true
	}
	return i, n, true
}

var maxNPerSelector = [16]byte{128, 128, 60, 30, 20, 15, 12, 10, 8, 7, 6, 5, 4, 3, 2, 1}

func MakeIndex[T uint16 | uint32](src []byte, dst []T) Index {
	return makeIndex(src, dst)
}

func makeIndex[T uint16 | uint32](src []byte, dst []T) *IndexImpl[T] {
	var (
		i  = 7 // code words are LE packed, selector is in last byte
		n  T
		sz = len(src) / 8
	)

	if dst == nil || cap(dst) < sz {
		dst = make([]T, 0, sz)
	}
	dst = dst[:0]

	for range len(src) / 64 {
		n += T(maxNPerSelector[src[i]>>4])
		dst = append(dst, n)
		i += 8
		n += T(maxNPerSelector[src[i]>>4])
		dst = append(dst, n)
		i += 8
		n += T(maxNPerSelector[src[i]>>4])
		dst = append(dst, n)
		i += 8
		n += T(maxNPerSelector[src[i]>>4])
		dst = append(dst, n)
		i += 8
		n += T(maxNPerSelector[src[i]>>4])
		dst = append(dst, n)
		i += 8
		n += T(maxNPerSelector[src[i]>>4])
		dst = append(dst, n)
		i += 8
		n += T(maxNPerSelector[src[i]>>4])
		dst = append(dst, n)
		i += 8
		n += T(maxNPerSelector[src[i]>>4])
		dst = append(dst, n)
		i += 8
	}

	for i < len(src) {
		n += T(maxNPerSelector[src[i]>>4])
		dst = append(dst, n)
		i += 8
	}

	return &IndexImpl[T]{ends: dst}
}
