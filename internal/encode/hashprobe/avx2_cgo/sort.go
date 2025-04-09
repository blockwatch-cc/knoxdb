package main

import "unsafe"

func Sort[T Integer](vs []T, shift int) {
	const (
		bits = 8
		// mask = 0xFF // T(uint64(1<<bits - 1))
	)
	w := int(unsafe.Sizeof(T(0)) * 8)

	if len(vs) < 1<<6 {
		// Insertion sort for small inputs
		for i := 0; i < len(vs); i++ {
			for j := i; j > 0 && vs[j-1] > vs[j]; j-- {
				vs[j-1], vs[j] = vs[j], vs[j-1]
			}
		}
		return
	}

	// First pass: count each bin size
	var bins [1 << bits]int
	for _, v := range vs {
		b := uint(v>>(w-bits-shift)) & 0xFF
		bins[b]++
	}

	// Locate bin ranges in the sorted array
	accum := 0
	var ends [1 << bits]int
	for b := 0; b < len(bins); b++ {
		beg := accum
		accum += bins[b]
		ends[b] = accum
		bins[b] = beg
	}

	// Second pass: move elements into allotted bins
	for b := 0; b < len(bins); b++ {
		for i := bins[b]; i < ends[b]; {
			bin := int((vs[i] >> (w - bits - shift))) & 0xFF
			if bin == b {
				i++
			} else {
				vs[bins[bin]], vs[i] = vs[i], vs[bins[bin]]
				bins[bin]++
			}
		}
	}

	// Recursively sort each bin on the next digit
	if shift < w-bits {
		beg := 0
		for b := 0; b < len(bins); b++ {
			Sort(vs[beg:ends[b]], shift+bits)
			beg = ends[b]
		}
	}
}
