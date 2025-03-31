// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package generic

// We use special compare kernels for bitpacked data. They assume data is MinFOR
// converted hence only unsigned numbers are supported here. A caller may also
// want to perform pre-checks on the value's bit width to exclude obvious cases
// that cannot match like equal 256 on a less than 8 bit packing.

type cmpFunc func(buf []byte, val uint64, n int, bits *Bitset) *Bitset
type cmpFunc2 func(buf []byte, val, val2 uint64, n int, bits *Bitset) *Bitset

func Equal(buf []byte, log2 int, val uint64, n int, bits *Bitset) *Bitset {
	return cmp_eq[log2](buf, val, n, bits)
}

func NotEqual(buf []byte, log2 int, val uint64, n int, bits *Bitset) *Bitset {
	return cmp_ne[log2](buf, val, n, bits)
}

func Less(buf []byte, log2 int, val uint64, n int, bits *Bitset) *Bitset {
	return cmp_lt[log2](buf, val, n, bits)
}

func LessEqual(buf []byte, log2 int, val uint64, n int, bits *Bitset) *Bitset {
	return cmp_le[log2](buf, val, n, bits)
}

func Greater(buf []byte, log2 int, val uint64, n int, bits *Bitset) *Bitset {
	return cmp_gt[log2](buf, val, n, bits)
}

func GreaterEqual(buf []byte, log2 int, val uint64, n int, bits *Bitset) *Bitset {
	return cmp_ge[log2](buf, val, n, bits)
}

func Between(buf []byte, log2 int, a, b uint64, n int, bits *Bitset) *Bitset {
	return cmp_bw[log2](buf, a, b, n, bits)
}
