// Copyright (c) 2023-2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package bitset

import "math/bits"

// Iterate returns multiple next bits set starting at the specified index
// and up to cap(buf). Use this method when low memory is a priority.
// Iterate returns a zero length slice when no more set bits are found.
//
//	buf := make([]int, 256) // alloc once and reuse
//	n := int(0)
//	n, buffer = bitmap.Iterate(n, buf)
//	for ; len(buf) > 0; n, buf = bitmap.Iterate(n, buf) {
//	 for k := range buf {
//	  do something with buf[k]
//	 }
//	 n += 1
//	}
//
// It is possible to retrieve all set bits as follow:
//
//	indices := make([]int, bitmap.Count())
//	bitmap.Iterate(0, indices)
//
// However, a faster method is [Bitset.Indexes] with a pre-allocated result.
func (s *Bitset) Iterate(i int, buf []int) (int, []int) {
	capacity := cap(buf)
	result := buf[:capacity]

	x := i >> 3
	if x >= len(s.buf) || capacity == 0 {
		return 0, result[:0]
	}

	// process first (partial) word
	word := s.buf[x] >> (i & 7)

	size := 0
	for word != 0 {
		result[size] = i + bits.TrailingZeros8(word)

		size++
		if size == capacity {
			return result[size-1], result[:size]
		}

		// clear the rightmost set bit
		word &= word - 1
	}

	// process the following full words
	// x < len(b.set), no out-of-bounds panic in following slice expression
	x++
	for idx, word := range s.buf[x:] {
		for word != 0 {
			result[size] = (x+idx)<<3 + bits.TrailingZeros8(word)

			size++
			if size == capacity {
				return result[size-1], result[:size]
			}

			// clear the rightmost set bit
			word &= word - 1
		}
	}

	if size > 0 {
		return result[size-1], result[:size]
	}
	return 0, result[:0]
}

// Indexes returns a slice positions as uint32 for one bits in the bitset.
func (s *Bitset) Indexes(result []uint32) []uint32 {
	cnt := s.cnt
	switch {
	case cnt == 0:
		return result[:0]
	case cnt < 0:
		cnt = s.size
	}
	// ensure slice dimension is multiple of 8, we need this for our
	// index lookup algo which always writes multiples of 8 entries
	cnt = roundUpPow2(cnt, 8)
	if result == nil || cap(result) < cnt {
		result = make([]uint32, cnt)
	} else {
		result = result[:cnt]
	}
	n := bitsetIndexes(s.buf, s.size, result)
	return result[:n]
}

// Slice returns a boolean slice containing all values
func (s *Bitset) Slice() []bool {
	res := make([]bool, s.size)
	for i, l := 0, s.size-s.size&7; i < l; i += 8 {
		b := s.buf[i>>3]
		res[i] = b&0x01 > 0
		res[i+1] = b&0x02 > 0
		res[i+2] = b&0x04 > 0
		res[i+3] = b&0x08 > 0
		res[i+4] = b&0x10 > 0
		res[i+5] = b&0x20 > 0
		res[i+6] = b&0x40 > 0
		res[i+7] = b&0x80 > 0
	}
	// tail
	for i := s.size & ^0x7; i < s.size; i++ {
		res[i] = s.buf[i>>3]&bitmask[i&7] > 0
	}
	return res
}
