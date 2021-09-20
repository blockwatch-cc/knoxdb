// Copyright (c) 2018-2021 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package dedup

import (
	"blockwatch.cc/knoxdb/hash/xxhash"
)

// Base sizes
//
// n: length, c: cardinality, len(n): length of elem n
//
// Algo     head      meta  dict      raw data       32k example
// --------------------------------------------------------------------
// Native   24        24n             ∑_1_n(len(n))  768k + n*len(n)
// Compact  3*24      8n              ∑_1_c(len(n))  256k + c*len(n)
// Fixed    8+24                      ∑_1_n(len(n))    32 + n*len(n)
// Dict     4*24+16   8c   n*log2(c)  ∑_1_c(len(n))   112 + 8c + n*log2(c) + c*len(n)
//
//
// Examples
//          max unique   dict cutoff  op_hash     op_store (avg)     <- Use Case
// Algo     32k/32       16k/32       22k/32      2500/66            <- c/len(n)
// ---------------------------------------------------------------
// Native   1.75M       1.75M        1.75M        2.88M
// Compact  1.25M -28%   768k -56%    943k -46%    417k -86%
// Fixed       1M -42%     1M -42%    1M   -42%    2.1M -27%
// Dict     1.34M -23%   696k -60%    906k -48%    229k -92%
//
// Algo selection
//
// Dataset                   Best Algo
// ----------------------------------------
// all zeros              -> fixed (e.g. all-empty params)
// fixed len + no zeros   -> fixed
// dyn len + card < n/2   -> dict
// dyn len + card >= n/2  -> compact (e.g. block/op hashes)

var emptyHash = xxhash.Sum64([]byte{})

type analysis struct {
	nEmpty    int
	isFixed   bool
	fixedSize int
}

func analyze(slice [][]byte) analysis {
	res := analysis{
		fixedSize: len(slice[0]),
		isFixed:   true,
	}
	for _, v := range slice {
		l := len(v)
		if l == 0 {
			res.nEmpty++
			res.isFixed = false
			res.fixedSize = -1
			continue
		}
		if res.isFixed && l != res.fixedSize {
			res.isFixed = false
			res.fixedSize = -1
		}
	}
	return res
}

// FIXME: check for collisions between nil, 0x0, 0x0 0x0, ...
func dedup(slice [][]byte) (dupmap []int, card int, sz int) {
	m := make(map[uint64]int, len(slice))
	dupmap = make([]int, len(slice))
	for i, v := range slice {
		h := emptyHash
		if len(v) > 0 {
			h = xxhash.Sum64(v)
		}
		if j, ok := m[h]; ok {
			dupmap[i] = j
		} else {
			sz += len(v)
			m[h] = card
			dupmap[i] = -1
			card++
		}
	}
	return
}

func optimize(slice [][]byte) ByteArray {
	l := len(slice)
	if l == 0 {
		// fmt.Printf("dedup: empty\n")
		return newFixedByteArray(0, 0)
	}
	an := analyze(slice)
	switch true {
	case an.nEmpty == l:
		// all zeros
		// fmt.Printf("dedup: zeros len=%d empty=%d\n", l, an.nEmpty)
		return newFixedByteArray(0, l)
	default:
		// analyze content for duplicates
		dm, card, sz := dedup(slice)
		if an.isFixed && an.nEmpty == 0 && card == l {
			// all fixed and unique
			// fmt.Printf("dedup: fixed len=%d empty=%d esz=%d\n", l, an.nEmpty, an.fixedSize)
			return makeFixedByteArray(an.fixedSize, slice)
		} else if card < l/2 {
			// many duplicates
			// fmt.Printf("dedup: dict len=%d size=%d empty=%d card=%d\n", l, sz, an.nEmpty, card)
			return makeDictByteArray(sz, card, slice, dm)
		} else {
			// some duplicates
			// fmt.Printf("dedup: compact len=%d size=%d empty=%d card=%d\n", l, sz, an.nEmpty, card)
			return makeCompactByteArray(sz, card, slice, dm)
		}
	}
}
