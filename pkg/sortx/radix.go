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

func Sort[T Integer](vals []T) {
	radixSort(vals, 0)
}

// radixSort is a fast MSD (Most Significant Digit) radix sort with 8-bit
// digits and a small-size insertion sort fallback. It sorts integer slices
// in-place and works recursive.
func radixSort[T Integer](vs []T, shift int) {
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
			radixSort(vs[beg:ends[b]], shift+nbits)
			beg = ends[b]
		}
	}
}

func SortIndices[T, E Integer](idx []T, getKey func(T) E) {
	radixSortIndices(idx, getKey, 0)
}

// radixSortIndices sorts the slice of indices `idx` according to the key
// returned by getKey.
func radixSortIndices[T, E Integer](idx []T, getKey func(T) E, shift int) {
	const nbits = 8

	if len(idx) < 64 {
		// Insertion sort for small inputs (on indices)
		for i := range idx {
			for j := i; j > 0 && getKey(idx[j-1]) > getKey(idx[j]); j-- {
				idx[j-1], idx[j] = idx[j], idx[j-1]
			}
		}
		return
	}

	w := SizeFor[T]() * 8
	s := w - nbits - shift

	// Count elements per bin
	var bins [1 << nbits]int
	for _, i := range idx {
		b := uint(getKey(i)>>s) & 0xFF
		bins[b]++
	}

	// Compute bin ranges
	accum := 0
	var ends [1 << nbits]int
	for b := range bins {
		beg := accum
		accum += bins[b]
		ends[b] = accum
		bins[b] = beg
	}

	// Distribute indices into bins
	for b := range bins {
		for i := bins[b]; i < ends[b]; {
			bin := int(getKey(idx[i])>>s) & 0xFF
			if bin == b {
				i++
			} else {
				idx[bins[bin]], idx[i] = idx[i], idx[bins[bin]]
				bins[bin]++
			}
		}
	}

	// Recurse into each bin for the next digit
	if shift < w-nbits {
		beg := 0
		for b := range bins {
			radixSortIndices(idx[beg:ends[b]], getKey, shift+nbits)
			beg = ends[b]
		}
	}
}
