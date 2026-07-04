// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package sortx

type Integer interface {
	~int | ~uint | ~int8 | ~int16 | ~int32 | ~int64 |
		~uint8 | ~uint16 | ~uint32 | ~uint64 | ~uintptr
}

func SizeFor[T Integer]() int {
	x := uint16(1 << 8)
	y := uint32(2 << 16)
	z := uint64(4 << 32)
	return 1 + int(T(x))>>8 + int(T(y))>>16 + int(T(z))>>32
}

const nbits = 8

// custom radix sort, faster than slices.Sort
func Sort[T Integer](vs []T, shift int) {
	w := SizeFor[T]() * 8
	s := w - nbits - shift

	if len(vs) < 1<<6 {
		// Insertion sort for small inputs
		for i := range vs {
			for j := i; j > 0 && vs[j-1] > vs[j]; j-- {
				vs[j-1], vs[j] = vs[j], vs[j-1]
			}
		}
		return
	}

	// First pass: count each bin size
	var bins [1 << nbits]int
	for _, v := range vs {
		b := uint(v>>s) & 0xFF
		bins[b]++
	}

	// Locate bin ranges in the sorted array
	accum := 0
	var ends [1 << nbits]int
	for b := range len(bins) {
		beg := accum
		accum += bins[b]
		ends[b] = accum
		bins[b] = beg
	}

	// Second pass: move elements into allotted bins
	for b := range len(bins) {
		for i := bins[b]; i < ends[b]; {
			bin := int(vs[i]>>s) & 0xFF
			if bin == b {
				i++
			} else {
				vs[bins[bin]], vs[i] = vs[i], vs[bins[bin]]
				bins[bin]++
			}
		}
	}

	// Recursively sort each bin on the next digit
	if shift < w-nbits {
		beg := 0
		for b := range len(bins) {
			Sort(vs[beg:ends[b]], shift+nbits)
			beg = ends[b]
		}
	}
}
