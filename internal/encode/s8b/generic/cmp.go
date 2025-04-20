// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package generic

import (
	"blockwatch.cc/knoxdb/internal/bitset"
	"blockwatch.cc/knoxdb/pkg/util"
)

type cmpFunc func(word, val uint64) (int, uint64)
type cmpFunc2 func(word, val, val2 uint64) (int, uint64)

func Equal(src []byte, val uint64, bits *bitset.Bitset) {
	compare(src, val, bits, cmp_eq, false)
}

func NotEqual(src []byte, val uint64, bits *bitset.Bitset) {
	// re-use equal compare and flip bits
	compare(src, val, bits, cmp_eq, true)
}

func Less(src []byte, val uint64, bits *bitset.Bitset) {
	// re-use greater equal compare and flip bits
	compare(src, val, bits, cmp_ge, true)
}

func LessEqual(src []byte, val uint64, bits *bitset.Bitset) {
	// re-use greater than compare and flip bits
	compare(src, val, bits, cmp_gt, true)
}

func Greater(src []byte, val uint64, bits *bitset.Bitset) {
	compare(src, val, bits, cmp_gt, false)
}

func GreaterEqual(src []byte, val uint64, bits *bitset.Bitset) {
	compare(src, val, bits, cmp_ge, false)
}

func Between(src []byte, val, val2 uint64, bits *bitset.Bitset) {
	compare2(src, val, val2, bits, cmp_bw)
}

func compare(src []byte, val uint64, bits *bitset.Bitset, cmp [16]cmpFunc, neg bool) {
	// aggregate output bits and flush in batches
	var (
		k    int
		out  uint64
		outn int
		buf  = bits.Bytes()
	)

	for _, word := range util.FromByteSlice[uint64](src) {
		// choose the comparison kernel for this word
		sel := byte(word>>60) & 0xF
		n, mask := cmp[sel](word, val)

		// fmt.Printf("s8: cmp val=%d sel=%d n=%d k=%d res=0x%x\n", val, sel, n, k+outn, mask)

		// negate match result if requested
		if neg {
			mask = ^mask & (1<<n - 1)
		}

		// selectors 0,1 encode 128 zeros or ones
		if sel <= 1 {
			// flush pending bits
			if outn > 0 {
				if out > 0 {
					writeBits(out, outn, k, buf)
				}
				k += outn
				outn = 0
				out = 0
			}
			if mask > 0 {
				bits.SetRange(k, k+127)
			}
			k += 128
			continue
		}

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
	}

	// flush remaining bits
	if outn > 0 && out > 0 {
		writeBits(out, outn, k, buf)
	}

	bits.ResetCount(-1)
	return
}

func compare2(src []byte, val, val2 uint64, bits *bitset.Bitset, cmp [16]cmpFunc2) {
	// choose the comparison kernel for this word
	var (
		k    int
		out  uint64
		outn int
		buf  = bits.Bytes()
	)

	for _, word := range util.FromByteSlice[uint64](src) {
		// choose the comparison kernel for this word
		sel := byte(word>>60) & 0xF
		n, mask := cmp[sel](word, val, val2)

		// fmt.Printf("s8: cmp val=[%d,%d] sel=%d n=%d k=%d res=0x%x\n", val, val2, sel, n, k+outn, mask)

		// selectors 0,1 encode 128 zeros or ones
		if sel <= 1 {
			// flush pending bits
			if outn > 0 {
				if out > 0 {
					writeBits(out, outn, k, buf)
				}
				k += outn
				outn = 0
				out = 0
			}
			if mask > 0 {
				bits.SetRange(k, k+127)
			}
			k += 128
			continue
		}

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
	}

	// flush remaining bits
	if outn > 0 && out > 0 {
		writeBits(out, outn, k, buf)
	}

	bits.ResetCount(-1)
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
	// fmt.Printf("s8: write mask=%016x n=%d k=%d\n", mask, n, k)

	// merge first byte
	// fmt.Printf("s8: write buf[%d] |= o%08b\n", j, byte(mask2))
	buf[j] |= byte(mask2)
	mask2 >>= 8
	i += 8
	j++

	// override following bytes
	for i < n {
		// fmt.Printf("s8: write buf[%d] = o%08b\n", j, byte(mask2))
		buf[j] = byte(mask2)
		mask2 >>= 8
		i += 8
		j++
	}

	// correct for missing mask2 bits on selector 2 (60 bits)
	// which may have been shifted out
	if adj > 0 {
		// fmt.Printf("s8: adj buf[%d] |= o%08b\n", j-1, byte(mask>>(64-adj)))
		buf[j-1] |= byte(mask >> (64 - adj))
	}
}
