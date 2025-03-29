// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package generic

import (
	"blockwatch.cc/knoxdb/internal/bitset"
	"blockwatch.cc/knoxdb/pkg/util"
)

type cmpFunc func(word, val uint64) (int, uint64)
type cmpFunc2 func(word, val, val2 uint64) (int, uint64)

func Equal(src []byte, val uint64, bits *bitset.Bitset) *bitset.Bitset {
	return compare(src, val, bits, cmp_eq, false)
}

func NotEqual(src []byte, val uint64, bits *bitset.Bitset) *bitset.Bitset {
	// re-use equal compare and flip bits
	return compare(src, val, bits, cmp_eq, true)
}

func Less(src []byte, val uint64, bits *bitset.Bitset) *bitset.Bitset {
	// re-use greater equal compare and flip bits
	return compare(src, val, bits, cmp_ge, true)
}

func LessEqual(src []byte, val uint64, bits *bitset.Bitset) *bitset.Bitset {
	// re-use greater than compare and flip bits
	return compare(src, val, bits, cmp_gt, true)
}

func Greater(src []byte, val uint64, bits *bitset.Bitset) *bitset.Bitset {
	return compare(src, val, bits, cmp_gt, false)
}

func GreaterEqual(src []byte, val uint64, bits *bitset.Bitset) *bitset.Bitset {
	return compare(src, val, bits, cmp_ge, false)
}

func Between(src []byte, val, val2 uint64, bits *bitset.Bitset) *bitset.Bitset {
	return compare2(src, val, val2, bits, cmp_bw)
}

func compare(src []byte, val uint64, bits *bitset.Bitset, cmp [16]cmpFunc, neg bool) *bitset.Bitset {
	var k, i int
	buf := bits.Bytes()

	// aggregate output bits and flush in batches
	var (
		out  uint64
		outn int
	)

	for _, word := range util.FromByteSlice[uint64](src) {
		sel := byte(word>>60) & 0xF
		n, mask := cmp[sel](word, val)

		if neg {
			mask = ^mask & (1<<n - 1)
		}

		// skip selectors 0,1 because our encoder does not emit them
		// if sel <= 1 && mask > 0 {
		// 	if outn > 0 {
		// 		aggregate(out, outn, k, buf)
		// 		k += outn
		// 		outn = 0
		// 		out = 0
		// 	}
		// 	bits.SetRange(k, k+n)
		// 	k += n
		// 	i += 8
		// 	continue
		// }

		// aggregate bits and flush sparingly
		if outn+n > 64 {
			if out > 0 {
				writeBits(out, outn, k, buf)
			}
			k += outn
			outn = 0
			out = 0
		}

		out |= mask << outn
		outn += n
		i += 8
	}

	// flush remaining bits
	if outn > 0 {
		writeBits(out, outn, k, buf)
	}

	bits.ResetCount(-1)
	return bits
}

// Writes bitset compatible layout directly into bitset memory
// ensuring limited memory traffic (no |= on mem in hot loop).
//
// Considers edge cases where a previous write did not write
// a full byte and we need to merge-append bits to existing
// bitset bytes. Also corrects for 64 bit shift overflow
// that may happen during adjustment to previous data and
// when output mask contains > 56 bits and we must shift
// for adjustment.
func writeBits(mask uint64, n, k int, buf []byte) {
	// move bits into bitset, adjust for in-byte write offset
	// by shifting result bits by that amount. bytes in mask
	// are in reverse order so we can >>8 as we go
	adj := k & 0x7
	mask2 := mask << adj
	i, j := -adj, k>>3

	// merge first byte
	buf[j] |= byte(mask2)
	mask2 >>= 8
	i += 8
	j++

	// override following bytes
	for i < n {
		buf[j] = byte(mask2)
		mask2 >>= 8
		i += 8
		j++
	}

	// correct for missing mask2 bits on selector 2 (60 bits)
	// which may have been shifted out be a too large adjustment (>4)
	if n >= 60 && adj > 4 {
		buf[j-1] |= byte(mask >> (64 - adj))
	}
}

func compare2(src []byte, val, val2 uint64, bits *bitset.Bitset, cmp [16]cmpFunc2) *bitset.Bitset {
	var k, i int
	buf := bits.Bytes()

	// aggregate output bits and flush in batches
	var (
		out  uint64
		outn int
	)

	for _, word := range util.FromByteSlice[uint64](src) {
		sel := byte(word>>60) & 0xF
		n, mask := cmp[sel](word, val, val2)

		// skip selectors 0,1 because our encoder does not emit them
		// if sel <= 1 && mask > 0 {
		// 	if outn > 0 {
		// 		aggregate(out, outn, k, buf)
		// 		k += outn
		// 		outn = 0
		// 		out = 0
		// 	}
		// 	bits.SetRange(k, k+n)
		// 	k += n
		// 	i += 8
		// 	continue
		// }

		// aggregate bits and flush sparingly
		if outn+n > 64 {
			if out > 0 {
				writeBits(out, outn, k, buf)
			}
			k += outn
			outn = 0
			out = 0
		}

		out |= mask << outn
		outn += n
		i += 8
	}

	// flush remaining bits
	if outn > 0 {
		writeBits(out, outn, k, buf)
	}

	bits.ResetCount(-1)
	return bits
}

// Simple (and slower) compare func left for reference
// func compare2(src []byte, val, val2 uint64, bits *bitset.Bitset, cmp [16]cmpFunc2) *bitset.Bitset {
// 	var k int
// 	buf := bits.Bytes()
// 	for _, word := range util.FromByteSlice[uint64](src) {
// 		sel := word >> 60
// 		n, mask := cmp[sel](word, val, val2)
// 		if mask > 0 {
// 			switch sel {
// 			case 0, 1:
// 				bits.SetRange(k, k+n)
// 			default:
// 				writeBits(mask, n, k, buf)
// 			}
// 		}
// 		k += n
// 	}
// 	bits.ResetCount(-1)
// 	return bits
// }
