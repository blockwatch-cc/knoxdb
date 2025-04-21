// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package s8b

import "sort"

type Index interface {
	Len() int
	End() int
	Find(n int) int
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

func (idx IndexImpl[T]) Find(n int) int {
	i := sort.Search(len(idx.ends), func(i int) bool {
		return idx.ends[i] >= T(n)
	})
	return i
}

var maxNPerSelector = [16]int{128, 128, 60, 30, 20, 15, 12, 10, 8, 7, 6, 5, 4, 3, 2, 1}

func MakeIndex[T uint16 | uint32](src []byte, dst []T) Index {
	var (
		i int
		n T
	)

	if dst == nil {
		dst = make([]T, 0)
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
