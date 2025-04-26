// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package bitpack

import (
	"blockwatch.cc/knoxdb/pkg/util"
)

// We use special compare kernels for bitpacked data. They assume data is MinFOR
// converted hence only unsigned numbers are supported here. A caller may also
// want to perform pre-checks on the value's bit width to exclude obvious cases
// that cannot match like equal 256 on a less than 8 bit packing.

func Equal(buf []byte, log2 int, val uint64, n int, bits *Bitset) *Bitset {
	return compare_eq(buf, log2, val, n, bits, false)
}

func NotEqual(buf []byte, log2 int, val uint64, n int, bits *Bitset) *Bitset {
	return compare_eq(buf, log2, val, n, bits, true)
}

func Less(buf []byte, log2 int, val uint64, n int, bits *Bitset) *Bitset {
	return compare_lt(buf, log2, val, n, bits, false)
}

func LessEqual(buf []byte, log2 int, val uint64, n int, bits *Bitset) *Bitset {
	return compare_le(buf, log2, val, n, bits, false)
}

func Greater(buf []byte, log2 int, val uint64, n int, bits *Bitset) *Bitset {
	return compare_le(buf, log2, val, n, bits, true)
}

func GreaterEqual(buf []byte, log2 int, val uint64, n int, bits *Bitset) *Bitset {
	return compare_lt(buf, log2, val, n, bits, true)
}

func Between(buf []byte, log2 int, a, b uint64, n int, bits *Bitset) *Bitset {
	return compare_bw(buf, log2, a, b, n, bits)
}

func compare_eq(buf []byte, log2 int, val uint64, n int, bits *Bitset, neg bool) *Bitset {
	outBuff := util.FromByteSlice[uint64](bits.Bytes())
	inBuff := util.FromByteSlice[uint64](buf)

	if neg {
		for i := range n / 64 {
			outBuff[i] = ^cmp_eq(inBuff[i*log2:], val, log2)
		}
	} else {
		for i := range n / 64 {
			outBuff[i] = cmp_eq(inBuff[i*log2:], val, log2)
		}
	}

	// tail
	if rem := n % 64; rem != 0 {
		var out [64]uint64
		decode(out[:rem], inBuff[n/64:], log2, 0)
		k := n &^ 63
		if neg {
			for i := range rem {
				if out[i] != val {
					bits.Set(i + k)
				}
			}
		} else {
			for i := range rem {
				if out[i] == val {
					bits.Set(i + k)
				}
			}
		}
	}

	bits.ResetCount(-1)
	return bits
}

func compare_lt(buf []byte, log2 int, val uint64, n int, bits *Bitset, neg bool) *Bitset {
	outBuff := util.FromByteSlice[uint64](bits.Bytes())
	inBuff := util.FromByteSlice[uint64](buf)

	if neg {
		for i := range n / 64 {
			outBuff[i] = ^cmp_lt(inBuff[i*log2:], val, log2)
		}
	} else {
		for i := range n / 64 {
			outBuff[i] = cmp_lt(inBuff[i*log2:], val, log2)
		}
	}

	// tail
	if rem := n % 64; rem != 0 {
		var out [64]uint64
		decode(out[:rem], inBuff[n/64:], log2, 0)
		k := n &^ 63
		if neg {
			for i := range rem {
				if out[i] >= val {
					bits.Set(i + k)
				}
			}
		} else {
			for i := range rem {
				if out[i] < val {
					bits.Set(i + k)
				}
			}
		}
	}

	bits.ResetCount(-1)
	return bits
}

func compare_le(buf []byte, log2 int, val uint64, n int, bits *Bitset, neg bool) *Bitset {
	outBuff := util.FromByteSlice[uint64](bits.Bytes())
	inBuff := util.FromByteSlice[uint64](buf)

	if neg {
		for i := range n / 64 {
			outBuff[i] = ^cmp_le(inBuff[i*log2:], val, log2)
		}
	} else {
		for i := range n / 64 {
			outBuff[i] = cmp_le(inBuff[i*log2:], val, log2)
		}
	}

	// tail
	if rem := n % 64; rem != 0 {
		var out [64]uint64
		decode(out[:rem], inBuff[n/64:], log2, 0)
		k := n &^ 63
		if neg {
			for i := range rem {
				if out[i] > val {
					bits.Set(i + k)
				}
			}
		} else {
			for i := range rem {
				if out[i] <= val {
					bits.Set(i + k)
				}
			}
		}
	}

	bits.ResetCount(-1)
	return bits
}

func compare_bw(buf []byte, log2 int, val1, val2 uint64, n int, bits *Bitset) *Bitset {
	outBuff := util.FromByteSlice[uint64](bits.Bytes())
	inBuff := util.FromByteSlice[uint64](buf)

	for i := range n / 64 {
		outBuff[i] = cmp_bw(inBuff[i*log2:], val1, val2, log2)
	}

	// tail
	if rem := n % 64; rem != 0 {
		var out [64]uint64
		decode(out[:rem], inBuff[n/64:], log2, 0)
		k := n &^ 63
		c2 := val2 - val1
		for i := range rem {
			if out[i]-val1 <= c2 {
				bits.Set(i + k)
			}
		}
	}

	bits.ResetCount(-1)
	return bits
}
