// Copyright (c) 2023-2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package bitset

import (
	"iter"
	"math/bits"
	"sync"

	"blockwatch.cc/knoxdb/pkg/util"
)

// Benchmark M1
//
// Data           Go Iterator       Iterate         Indexes      ChunkIterator
// -----------------------------------------------------------------------------
// 1K/D100        3000 ns/op       820 ns/op       759 ns/op        251 ns/op *
// 16K/D100      44946 ns/op     12915 ns/op     12075 ns/op       3736 ns/op *
// 64K/D100     182124 ns/op     51691 ns/op     51163 ns/op      15040 ns/op *
//
// 1K/D50         1437 ns/op       384 ns/op       495 ns/op        373 ns/op *
// 16K/D50       21918 ns/op      6872 ns/op *    8089 ns/op       7135 ns/op
// 64K/D50       97846 ns/op     30519 ns/op *   68326 ns/op      31289 ns/op
//
// 1K/D1            75 ns/op        49 ns/op *      52 ns/op         53 ns/op
// 16K/D1          504 ns/op       382 ns/op *     715 ns/op        392 ns/op
// 64K/D1         1885 ns/op      1616 ns/op *    3044 ns/op       1595 ns/op
//
//
// Benchmark i9-12900K (AVX2)
//
// Data          Go Iterator      Iterate         Indexes         ChunkIterator
// -----------------------------------------------------------------------------
// 1K/D100        1436 ns/op       513 ns/op       128 ns/op *      196 ns/op
// 16K/D100      22668 ns/op      8068 ns/op      1980 ns/op *     3055 ns/op
// 64K/D100      90683 ns/op     32101 ns/op      7837 ns/op *    12249 ns/op
//
// 1K/D50          751 ns/op       221 ns/op       128 ns/op *      238 ns/op
// 16K/D50       11626 ns/op      3543 ns/op      1965 ns/op *     3618 ns/op
// 64K/D50       46540 ns/op     16318 ns/op      7901 ns/op *    16423 ns/op
//
// 1K/D1           137 ns/op        30 ns/op        27 ns/op *       36 ns/op
// 16K/D1          316 ns/op       246 ns/op *     610 ns/op        257 ns/op
// 64K/D1         1097 ns/op       963 ns/op *    2430 ns/op        977 ns/op

// An empirical study on synthentic bitsets shows AVX2 indexes
// outperform iterate above 25% fill level by up to 8x (at 100%)
// and underperform for sparser bitsets by 2-4x.
//
// [vals/ns]      Indexes     Iterate    Diff
// -----------------------------------------------
// 1K/D100        7.991       2.006      0.25x
// 1K/D80         8.025       2.913      0.36x
// 1K/D66         8.045       3.512      0.44x
// 1K/D50         8.057       4.595      0.57x
// 1K/D25         8.264       8.497      1.03x *
// 1K/D12         7.908       15.31      1.94x *
// 1K/D6          9.416       22.82      2.42x *
// 1K/D3          14.37       29.14      2.03x *
// 1K/D1          35.03       34.86      1.00x
// -----------------------------------------------
// 16K/D100       8.690       2.040      0.23x
// 16K/D80        8.606       2.858      0.33x
// 16K/D66        8.708       3.437      0.39x
// 16K/D50        8.345       4.547      0.54x
// 16K/D25        7.601       8.850      1.16x *
// 16K/D12        6.055       16.66      2.75x *
// 16K/D6         6.405       29.67      4.63x *
// 16K/D3         8.793       41.71      4.74x *
// 16K/D1         27.63       63.78      2.31x *
// -----------------------------------------------
// 64K/D100       8.439       2.046      0.24x
// 64K/D80        8.498       2.441      0.29x
// 64K/D66        8.598       3.052      0.35x
// 64K/D50        8.205       4.026      0.49x
// 64K/D25        7.380       7.992      1.08x *
// 64K/D12        5.841       16.72      2.86x *
// 64K/D6         5.974       29.85      5.00x *
// 64K/D3         8.465       41.50      4.90x *
// 64K/D1         27.00       67.45      2.50x *

// Iterator returns a Go range loop compatible function that ranges over
// all indexes of bits set in the bitset. Its convenient but slow due
// to the function call overhead for each returned element. Its ok for
// tests, but users should prefer Iterate() or ChunkIterator below.
func (s *Bitset) Iterator() iter.Seq[int] {
	return func(fn func(int) bool) {
		var i int

		// process 64 bit words
		for _, word := range util.FromByteSlice[uint64](s.buf) {
			for word != 0 {
				if !fn(i + bits.TrailingZeros64(word)) {
					return
				}
				// clear the rightmost set bit
				word &= word - 1
			}
			i += 64
		}

		// process tail as 8 bit words
		for _, word := range s.buf[i>>3:] {
			for word != 0 {
				if !fn(i + bits.TrailingZeros8(word)) {
					return
				}
				// clear the rightmost set bit
				word &= word - 1
			}
			i += 8
		}
	}
}

var chunkFactory = sync.Pool{
	New: func() any { return new(ChunkIterator) },
}

// ChunkIterator is a convenience helper to iterate through bitset indexes
type ChunkIterator struct {
	idx  [128]int
	set  *Bitset
	last int
}

func (s *Bitset) Chunks() *ChunkIterator {
	it := chunkFactory.Get().(*ChunkIterator)
	it.set = s
	it.last = -1
	return it
}

func (c *ChunkIterator) Next() ([]int, bool) {
	if c.last+1 >= c.set.size {
		return nil, false
	}
	if c.set.All() {
		// special case for all bits set
		var (
			i int
			v = c.last + 1
		)
		for range 16 {
			c.idx[i] = v
			c.idx[i+1] = v + 1
			c.idx[i+2] = v + 2
			c.idx[i+3] = v + 3
			c.idx[i+4] = v + 4
			c.idx[i+5] = v + 5
			c.idx[i+6] = v + 6
			c.idx[i+7] = v + 7
			i += 8
			v += 8
		}
		n := min(128, c.set.size-c.last-1)
		c.last += n
		return c.idx[:n], true
	}

	// regular case
	res, ok := c.set.Iterate(c.last, c.idx[:])
	if ok {
		c.last = res[len(res)-1]
	} else {
		c.last = c.set.size
	}
	return res, ok
}

func (c *ChunkIterator) Close() {
	c.set = nil
	chunkFactory.Put(c)
}

// Iterate returns multiple indexes of set bits starting at index i.
// Res must be allocated and may be filled up to capacity. When no more
// bits are found, a zero length slice and false is returned.
//
// Prefer this method over Iterator and Indexes using a buffer size
// of 128 or more for good performance
//
//	var (
//		buf  [128]int // alloc once and reuse
//		last int = -1
//	)
//	for {
//		vals, ok := bitmap.Iterate(last, buf[:])
//		if !ok {
//			break
//		}
//		for _, idx := range vals {
//			// do something
//		}
//		last = vals[len(vals)-1]
//	}
//
// It is possible to retrieve all indexes at once.
//
//	indices := make([]int, bitmap.Count())
//	indices, haveAny := bitmap.Iterate(0, indices)
func (s *Bitset) Iterate(last int, res []int) ([]int, bool) {
	// use full capacity
	nmax := cap(res)
	res = res[:nmax]

	// start scan at first or next bit
	var n int
	i := last + 1

	// sanity check
	if i >= s.size || len(res) == 0 {
		return nil, false
	}

	// process leading partial byte if any
	if i&7 > 0 {
		word := s.buf[i>>3] >> (i & 7)
		for word != 0 && n < nmax {
			res[n] = i + bits.TrailingZeros8(word)
			n++

			// clear the rightmost set bit
			word &= word - 1
		}
		i += 8 - i&7
		if n == nmax {
			goto DONE
		}
	}

	// process full 64 bit words
	for _, word := range util.FromByteSlice[uint64](s.buf[i>>3:]) {
		for word != 0 && n < nmax {
			res[n] = i + bits.TrailingZeros64(word)
			n++

			// clear the rightmost set bit
			word &= word - 1
		}
		if n == nmax {
			goto DONE
		}
		i += 64
	}

	// process tail as 8 bit words
	for _, word := range s.buf[i>>3:] {
		for word != 0 && n < nmax {
			res[n] = i + bits.TrailingZeros8(word)
			n++

			// clear the rightmost set bit
			word &= word - 1
		}
		if n == nmax {
			goto DONE
		}
		i += 8
	}

DONE:
	return res[:n], n > 0
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
	// ensure slice is padded with 8 extra values, we need this for our
	// index lookup algo which always writes multiples of 8 entries
	// cnt = roundUpPow2(cnt, 8)
	cnt = min(cnt+8, s.size)
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
	var i int
	for range s.size / 8 {
		b := s.buf[i>>3]
		res[i] = b&0x01 > 0
		res[i+1] = b&0x02 > 0
		res[i+2] = b&0x04 > 0
		res[i+3] = b&0x08 > 0
		res[i+4] = b&0x10 > 0
		res[i+5] = b&0x20 > 0
		res[i+6] = b&0x40 > 0
		res[i+7] = b&0x80 > 0
		i += 8
	}
	// tail
	for range s.size & 0x7 {
		res[i] = s.buf[i>>3]&bitmask[i&7] > 0
		i++
	}
	return res
}
