// Copyright (c) 2025 Blockwatch Data Inc.
// Author: abdul@blockwatch.cc

package fsst

import (
	"encoding/binary"
	"math/bits"
)

// we keep two counters count1[pos] and count2[pos1][pos2] of resp 16 and 12-bits. Both are split into two columns for performance reasons
// first reason is to make the column we update the most during symbolTable construction (the low bits) thinner, thus reducing CPU cache pressure.
// second reason is that when scanning the array, after seeing a 64-bits 0 in the high bits column, we can quickly skip over many codes (15 or 7)
type Counter struct {
	// high arrays come before low arrays, because our GetNext() methods may overrun their 64-bits reads a few bytes
	count1High [FSST_CODE_MAX]uint8                    // array to count frequency of symbols as they occur in the sample (16-bits)
	count1Low  [FSST_CODE_MAX]uint8                    // it is split in a low and high byte: cnt = count1High*256 + count1Low
	count2High [FSST_CODE_MAX][FSST_CODE_MAX / 2]uint8 // array to count subsequent combinations of two symbols in the sample (12-bits: 8-bits low, 4-bits high)
	count2Low  [FSST_CODE_MAX][FSST_CODE_MAX]uint8     // its value is (count2High*256+count2Low) -- but high is 4-bits (we put two numbers in one, hence /2)
	// 385KB  -- but hot area likely just 10 + 30*4 = 130 cache lines (=8KB)
}

func NewCounter() *Counter {
	return &Counter{
		count1High: [FSST_CODE_MAX]uint8{},
		count1Low:  [FSST_CODE_MAX]uint8{},
		count2High: [FSST_CODE_MAX][FSST_CODE_MAX / 2]uint8{},
		count2Low:  [FSST_CODE_MAX][FSST_CODE_MAX]uint8{},
	}
}

func (c *Counter) Count1Set(pos1 uint32, val uint16) {
	c.count1Low[pos1] = uint8(val & 255)
	c.count1High[pos1] = uint8(val >> 8)
}

func (c *Counter) Count1Inc(pos1 uint32) {
	if c.count1Low[pos1] <= 0 { // increment high early (when low==0, not when low==255). This means (high > 0) <=> (cnt > 0)
		c.count1High[pos1]++ // (0,0)->(1,1)->..->(255,1)->(0,1)->(1,2)->(2,2)->(3,2)..(255,2)->(0,2)->(1,3)->(2,3)...
	}
	c.count1Low[pos1]++
}

func (c *Counter) Count2Inc(pos1, pos2 uint32) {
	if c.count2Low[pos1][pos2] <= 0 { // increment high early (when low==0, not when low==255). This means (high > 0) <=> (cnt > 0)
		// inc 4-bits high counter with 1<<0 (1) or 1<<4 (16) -- depending on whether pos2 is even or odd, repectively
		c.count2High[pos1][(pos2)>>1] += 1 << (((pos2) & 1) << 2) // we take our chances with overflow.. (4K maxval, on a 8K sample)
	}
	c.count2Low[pos1][pos2]++
}

func (c *Counter) Count1GetNext(pos1 uint32) (uint32, uint32) { // note: we will advance pos1 to the next nonzero counter in register range
	// read 16-bits single symbol counter, split into two 8-bits numbers (count1Low, count1High), while skipping over zeros
	var high uint64 = binary.LittleEndian.Uint64(c.count1High[pos1:])

	var zero = uint32(7) // number of zero bytes
	if high > 0 {
		zero = uint32(bits.TrailingZeros64(high) >> 3)
	}

	high = (high >> (zero << 3)) & 255 // advance to nonzero counter
	pos1 += zero
	if (pos1 >= FSST_CODE_MAX) || high == 0 {
		return 0, pos1
	}

	low := uint32(c.count1Low[pos1])
	if low > 0 {
		high--
	}

	return (uint32(high<<8) + low), pos1
}

func (c *Counter) Count2GetNext(pos1 uint32, pos2 uint32) (uint32, uint32) { // note: we will advance pos2 to the next nonzero counter in register range
	// read 12-bits pairwise symbol counter, split into low 8-bits and high 4-bits number while skipping over zeros
	var high uint64 = binary.LittleEndian.Uint64(c.count2High[pos1][pos2>>1:])

	high >>= ((pos2 & 1) << 2) // odd pos2: ignore the lowest 4 bits & we see only 15 counters

	var zero uint32 = uint32(15 - (pos2 & 1)) // number of zero 4-bits counters
	if high > 0 {
		zero = uint32(bits.TrailingZeros64(high)) >> 2
	}

	high = (high >> (zero << 2)) & 15 // advance to nonzero counter
	pos2 += zero
	if pos2 >= FSST_CODE_MAX || high <= 0 { // SKIP! advance pos2
		return 0, pos2 // all zero
	}

	low := uint32(c.count2Low[pos1][pos2])
	if low > 0 {
		high-- // high is incremented early and low late, so decrement high (unless low==0)
	}
	return (uint32(high<<8) + low), pos2
}

func (c *Counter) Backup(bc *Counter) {
	// copy(buf, c.count1High[:FSST_CODE_MAX])
	// copy(buf[FSST_CODE_MAX:], c.count1Low[:FSST_CODE_MAX])

	copy(bc.count1High[:], c.count1High[:])
	copy(bc.count1Low[:], c.count1Low[:])
	copy(bc.count2High[:], c.count2High[:])
	copy(bc.count2Low[:], c.count2Low[:])
}

func (c *Counter) Restore(bc *Counter) {
	// copy(c.count1High[:FSST_CODE_MAX], buf[:FSST_CODE_MAX])
	// copy(c.count1Low[:FSST_CODE_MAX], buf[FSST_CODE_MAX:])

	copy(c.count1High[:], bc.count1High[:])
	copy(c.count1Low[:], bc.count1Low[:])
	copy(c.count2High[:], bc.count2High[:])
	copy(c.count2Low[:], bc.count2Low[:])
}

func (c *Counter) Clear() {
	c.count1High = [FSST_CODE_MAX]uint8{}
	c.count1Low = [FSST_CODE_MAX]uint8{}
	c.count2High = [FSST_CODE_MAX][FSST_CODE_MAX / 2]uint8{}
	c.count2Low = [FSST_CODE_MAX][FSST_CODE_MAX]uint8{}
}
