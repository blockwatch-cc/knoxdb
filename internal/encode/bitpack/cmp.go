// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package bitpack

import (
	"unsafe"

	"blockwatch.cc/knoxdb/pkg/util"
)

// We use special compare kernels for bitpacked data. They assume data is MinFOR
// converted hence only unsigned numbers are supported here. A caller may also
// want to perform pre-checks on the value's bit width to exclude obvious cases
// that cannot match like equal 256 on a less than 8 bit packing.
type cmpFunc func(unsafe.Pointer, uint64) uint64
type cmpFunc2 func(unsafe.Pointer, uint64, uint64) uint64
type scalarCmpFunc func(uint64, uint64) bool

func Equal(buf []byte, log2 int, val uint64, n int, bits *Bitset) {
	compare(buf, log2, val, n, bits, cmp_eq, eq, false)
}

func NotEqual(buf []byte, log2 int, val uint64, n int, bits *Bitset) {
	compare(buf, log2, val, n, bits, cmp_eq, eq, true)
}

func Less(buf []byte, log2 int, val uint64, n int, bits *Bitset) {
	compare(buf, log2, val, n, bits, cmp_lt, lt, false)
}

func LessEqual(buf []byte, log2 int, val uint64, n int, bits *Bitset) {
	compare(buf, log2, val, n, bits, cmp_le, le, false)
}

func Greater(buf []byte, log2 int, val uint64, n int, bits *Bitset) {
	compare(buf, log2, val, n, bits, cmp_le, le, true)
}

func GreaterEqual(buf []byte, log2 int, val uint64, n int, bits *Bitset) {
	compare(buf, log2, val, n, bits, cmp_lt, lt, true)
}

func Between(buf []byte, log2 int, a, b uint64, n int, bits *Bitset) {
	compare2(buf, log2, a, b, n, bits, cmp_bw)
}

func compare(src []byte, log2 int, val uint64, n int, bits *Bitset, cmp [65]cmpFunc, scmp scalarCmpFunc, neg bool) {
	var p unsafe.Pointer
	if len(src) > 0 {
		p = unsafe.Pointer(&src[0])
	}
	out := util.FromByteSlice[uint64](bits.Bytes())

	if neg {
		for i := range n / 64 {
			out[i] = ^cmp[log2](p, val)
			p = unsafe.Add(p, log2*8)
		}
	} else {
		for i := range n / 64 {
			out[i] = cmp[log2](p, val)
			p = unsafe.Add(p, log2*8)
		}
	}

	// tail
	if rem := n & 63; rem != 0 {
		var out [64]uint64
		in := util.FromByteSlice[uint64](src)
		decode(out[:rem], in[n/64*log2:], log2, 0)
		k := n &^ 63
		if neg {
			for i := range rem {
				if !scmp(out[i], val) {
					bits.Set(i + k)
				}
			}
		} else {
			for i := range rem {
				if scmp(out[i], val) {
					bits.Set(i + k)
				}
			}
		}
	}

	bits.ResetCount(-1)
}

func compare2(src []byte, log2 int, val1, val2 uint64, n int, bits *Bitset, cmp [65]cmpFunc2) {
	var p unsafe.Pointer
	if len(src) > 0 {
		p = unsafe.Pointer(&src[0])
	}
	out := util.FromByteSlice[uint64](bits.Bytes())

	for i := range n / 64 {
		out[i] = cmp[log2](p, val1, val2)
		p = unsafe.Add(p, log2*8)
	}

	// tail
	if rem := n & 63; rem != 0 {
		var out [64]uint64
		in := util.FromByteSlice[uint64](src)
		decode(out[:rem], in[n/64*log2:], log2, 0)
		k := n &^ 63
		c2 := val2 - val1
		for i := range rem {
			if out[i]-val1 <= c2 {
				bits.Set(i + k)
			}
		}
	}

	bits.ResetCount(-1)
}

func eq(val, cmp uint64) bool {
	return val == cmp
}

func lt(val, cmp uint64) bool {
	return val < cmp
}

func le(val, cmp uint64) bool {
	return val <= cmp
}
