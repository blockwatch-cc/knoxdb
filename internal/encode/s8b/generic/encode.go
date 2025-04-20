// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package generic

// Package s8b implements the 64bit integer encoding algoritm as published
// by Ann and Moffat in "Index compression using 64-bit words", Softw. Pract.
// Exper. 2010; 40:131–147 with modifications outlined below.
//
// It is capable of encoding multiple integers with values betweeen 0 and up
// to 1^60-1, in a single uint64 word using adaptive bit-packing. Each code
// word uses the same bit-width signalled as 4bit selector in the most
// significant bits.
//
// Simple8b always fills up a code word with values, which can lead to
// lower efficiency at the tail end of a vector or when consecutive values
// vary widely in bit-width as in these cases lower width values are flushed
// into the next word(s) at higher width to accomodate the packing factor.
//
// Notable changes to the original simple8b implementation by jwilder:
// - selector 0 is repurposed to represent 128 zeros instead of 240 ones
// - selector 1 is repurposed to represent 128 ones instead of 120 ones
// - encoder performs min-FOR fusion and requires minv/maxv for the input vector
// - changed memory layout to LittleEndian from BigEndian
// - use Go generics to support all integer types as input instead of only uint64
// - new incremental packing algorithm with early stop
//

// Simple8b is 64bit word-sized encoder that packs multiple integers into a
// single word using a 4 bit selector values and up to 60 bits for the remaining
// values.  Integers are encoded using the following table:
//
// ┌──────────────┬─────────────────────────────────────────────────────────────┐
// │   Selector   │       0    1   2   3   4   5   6   7  8  9  0 11 12 13 14 15│
// ├──────────────┼─────────────────────────────────────────────────────────────┤
// │     Bits     │       0    0   1   2   3   4   5   6  7  8 10 12 15 20 30 60│
// ├──────────────┼─────────────────────────────────────────────────────────────┤
// │      N       │     128  128  60  30  20  15  12  10  8  7  6  5  4  3  2  1│
// ├──────────────┼─────────────────────────────────────────────────────────────┤
// │   Wasted Bits│      60   60   0   0   0   0  12   0  4  4  0  0  0  0  0  0│
// └──────────────┴─────────────────────────────────────────────────────────────┘
//
// For example, when the number of values can be encoded using 4 bits, selected 5
// is encoded in the 4 most significant bits followed by 15 values encoded used
// 4 bits each in the remaining 60 bits.
import (
	"errors"
	"math/bits"
	"unsafe"

	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/util"
)

const (
	MaxValue     = (1 << 60) - 1
	S8B_BIT_SIZE = 60
)

var (
	maxNPerSelector = [16]int{128, 128, 60, 30, 20, 15, 12, 10, 8, 7, 6, 5, 4, 3, 2, 1}

	// max values per bit width
	maxNPerBits = [64]byte{
		60,   // 0 -- 60x 0bit values per uint64
		60,   // 1 -- 60x 1bit values per uint64
		30,   // 2 -- 30x 2bit values per uint64
		20,   // 3 -- 20x 3bit values per uint64
		15,   // 4 -- 15x 4bit values per uint64
		12,   // 5 -- 12x 5bit values per uint64
		10,   // 6 -- 10x 6bit values per uint64
		8,    // 7 -- 8x 7bit values per uint64
		7,    // 8 -- 7x 8bit values per uint64
		6, 6, // [9,10] 6x 10bit values per uint64
		5, 5, // [11,12] 5x 12bit values per uint64
		4, 4, 4, // [13..15] 4x 15bit values per uint64
		3, 3, 3, 3, 3, // [16..20] 3x 20 bit values per uint64
		2, 2, 2, 2, 2, 2, 2, 2, 2, 2, // [21..30] 2x 30bit values per uint64
		1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, // [31..60] 1x 60bit value per uint64
		1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
		0, 0, 0, // [61..63] undefined
	}

	// selector code per bit width
	codeByBits = [64]byte{
		2,      // 0 -- 60x 0bit values per uint64
		2,      // 1 -- 60x 1bit values per uint64
		3,      // 2 -- 30x 2bit values per uint64
		4,      // 3 -- 20x 3bit values per uint64
		5,      // 4 -- 15x 4bit values per uint64
		6,      // 5 -- 12x 5bit values per uint64
		7,      // 6 -- 10x 6bit values per uint64
		8,      // 7 -- 8x 7bit values per uint64
		9,      // 8 -- 7x 8bit values per uint64
		10, 10, // [9,10] 6x 10bit values per uint64
		11, 11, // [11,12] 5x 12bit values per uint64
		12, 12, 12, // [13..15] 4x 15bit values per uint64
		13, 13, 13, 13, 13, // [16..20] 3x 20 bit values per uint64
		14, 14, 14, 14, 14, 14, 14, 14, 14, 14, // [21..30] 2x 30bit values per uint64
		15, 15, 15, 15, 15, 15, 15, 15, 15, 15, // [31..60] 1x 60bit value per uint64
		15, 15, 15, 15, 15, 15, 15, 15, 15, 15,
		15, 15, 15, 15, 15, 15, 15, 15, 15, 15,
		255, 255, 255, // [61..63] undefined
	}

	ErrValueOutOfBounds    = errors.New("value out of bounds")
	ErrInvalidBufferLength = errors.New("dst length is not multiple of 8")
)

//go:nocheckptr
func Encode[T types.Integer](dst []byte, src []T, minv, maxv T) ([]byte, error) {
	if len(src) == 0 {
		return nil, nil
	}
	if len(dst)&7 != 0 {
		return nil, ErrInvalidBufferLength
	}

	// pick selector based on input bit width
	selector := packSelector[T](minv)

	// determine the maximum possible bit width for this vector (post min-FOR)
	// we use it to
	// - check the 60 bit limit
	// - stop the incremental packing loop early (benefits small bit widths most)
	maxLog2 := bits.Len64(uint64(maxv) - uint64(minv))
	if maxLog2 > 60 {
		return nil, ErrValueOutOfBounds
	}

	out := util.FromByteSlice[uint64](dst)
	var i, j int
	for i < len(src) {
		remaining := src[i:]

		// try to pack runs of 128x 0s or 1s (pre-check if first value actually
		// translates to 0 or one, skip expensive check otherwise)
		if len(remaining) >= 128 && uint64(remaining[0])-uint64(minv) <= 1 {
			isZero, isOne := zeroOrOne(unsafe.Pointer(&remaining[0]), minv)
			if isZero {
				out[j] = 0
				j++
				i += 128
				continue
			} else if isOne {
				out[j] = uint64(1) << S8B_BIT_SIZE
				j++
				i += 128
				continue
			}
		}

		var (
			n        int
			nleft    int    = len(remaining)
			maxSeen  uint64 = 1
			maxN     int    = 60
			usedBits int    = 1
			isFull   bool
		)

		// Incremental packing
		for n < nleft {
			val := uint64(remaining[n]) - uint64(minv)
			if val > maxSeen {
				maxSeen = val
				usedBits = bits.Len64(val)
				maxN = int(maxNPerBits[usedBits])
				if n > maxN {
					// cannot use this value this round
					break
				}

				// stop search early when maxLog2 is reached
				// bad when sel is already large (low packing rate) because
				// then the check is pure overhead. We have determined experimentally
				// that maxN > 5 is a good compromise for prefixing the maxlog2 check
				if maxN > 5 && usedBits == maxLog2 {
					// stop early and try packing the max number of values at this bit width
					n = min(maxN, nleft)
					isFull = maxN <= nleft
					break
				}
			}
			n++
			if n == maxN {
				isFull = true
				break
			}
		}

		// adjust selector when code word is not full by increasing usedBits
		// and possible adjusting down n (note: code words must always be full)
		sel := codeByBits[usedBits]
		if !isFull {
			for sel < 15 && n < maxNPerSelector[sel] {
				sel++
			}
			n = min(n, maxNPerSelector[sel])
		}

		// pack values
		out[j] = selector[sel](unsafe.Pointer(&remaining[0]), uint64(minv))
		// fmt.Printf("Pack %T sel=%d i=%d n=%d minv=%d(%T) out[%d]=%016x\n", T(0), sel, i, n, minv, minv, j, out[j])

		j++
		i += n
	}

	return dst[:j*8], nil
}

// BenchmarkZeroOrOneV9/Zeros         	31235934	        37.58 ns/op
// BenchmarkZeroOrOneV9/Ones          	32417026	        37.21 ns/op
// BenchmarkZeroOrOneV9/Dups          	1000000000	         0.7041 ns/op
// BenchmarkZeroOrOneV9/Runs          	1000000000	         0.7182 ns/op
// BenchmarkZeroOrOneV9/Seq           	1000000000	         1.208 ns/op
func zeroOrOne[T types.Integer](p unsafe.Pointer, minv T) (bool, bool) {
	src := (*[128]T)(p)
	var zeroAcc, oneAcc T = 0, 0

	// 2x loop unrolling
	for i := 0; i < 128; i += 2 {
		d0 := src[i] - minv
		d1 := src[i+1] - minv

		// early exit
		if d0 > 1 || d1 > 1 {
			return false, false
		}

		// efficient accumulation to optimize runtime for 128 matches
		zeroAcc |= d0 | d1
		oneAcc |= (d0 ^ 1) | (d1 ^ 1)
	}
	return zeroAcc == 0, oneAcc == 0
}
