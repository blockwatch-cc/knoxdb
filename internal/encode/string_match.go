// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package encode

import (
	"bytes"

	"blockwatch.cc/knoxdb/internal/bitset"
	"blockwatch.cc/knoxdb/internal/types"
)

func matchStringEqual(a types.StringAccessor, val []byte, bits, mask *bitset.Bitset) {
	set := bits.Bytes()
	var cnt int
	if mask != nil {
		msk := mask.Bytes()
		for i := range a.Len() {
			bit := bitmask(i)
			if (msk[i>>3] & bit) == 0 {
				continue
			}
			if !bytes.Equal(a.Get(i), val) {
				continue
			}
			set[i>>3] |= bit
			cnt++
		}
	} else {
		for i := range a.Len() {
			if !bytes.Equal(a.Get(i), val) {
				continue
			}
			set[i>>3] |= bitmask(i)
			cnt++
		}
	}
	bits.ResetCount(cnt)
}

func matchStringNotEqual(a types.StringAccessor, val []byte, bits, mask *bitset.Bitset) {
	set := bits.Bytes()
	var cnt int
	if mask != nil {
		msk := mask.Bytes()
		for i := range a.Len() {
			bit := bitmask(i)
			if (msk[i>>3] & bit) == 0 {
				continue
			}
			if bytes.Equal(a.Get(i), val) {
				continue
			}
			set[i>>3] |= bit
			cnt++
		}
	} else {
		for i := range a.Len() {
			if bytes.Equal(a.Get(i), val) {
				continue
			}
			set[i>>3] |= bitmask(i)
			cnt++
		}
	}
	bits.ResetCount(cnt)
}

func matchStringLess(a types.StringAccessor, val []byte, bits, mask *bitset.Bitset) {
	set := bits.Bytes()
	var cnt int
	if mask != nil {
		msk := mask.Bytes()
		for i := range a.Len() {
			bit := bitmask(i)
			if (msk[i>>3] & bit) == 0 {
				continue
			}
			if bytes.Compare(a.Get(i), val) >= 0 {
				continue
			}
			set[i>>3] |= bit
			cnt++
		}
	} else {
		for i := range a.Len() {
			if bytes.Compare(a.Get(i), val) >= 0 {
				continue
			}
			set[i>>3] |= bitmask(i)
			cnt++
		}
	}
	bits.ResetCount(cnt)
}

func matchStringLessEqual(a types.StringAccessor, val []byte, bits, mask *bitset.Bitset) {
	set := bits.Bytes()
	var cnt int
	if mask != nil {
		msk := mask.Bytes()
		for i := range a.Len() {
			bit := bitmask(i)
			if (msk[i>>3] & bit) == 0 {
				continue
			}
			if bytes.Compare(a.Get(i), val) > 0 {
				continue
			}
			set[i>>3] |= bit
			cnt++
		}
	} else {
		for i := range a.Len() {
			if bytes.Compare(a.Get(i), val) > 0 {
				continue
			}
			set[i>>3] |= bitmask(i)
			cnt++
		}
	}
	bits.ResetCount(cnt)
}

func matchStringGreater(a types.StringAccessor, val []byte, bits, mask *bitset.Bitset) {
	set := bits.Bytes()
	var cnt int
	if mask != nil {
		msk := mask.Bytes()
		for i := range a.Len() {
			bit := bitmask(i)
			if (msk[i>>3] & bit) == 0 {
				continue
			}
			if bytes.Compare(a.Get(i), val) <= 0 {
				continue
			}
			set[i>>3] |= bit
			cnt++
		}
	} else {
		for i := range a.Len() {
			if bytes.Compare(a.Get(i), val) <= 0 {
				continue
			}
			set[i>>3] |= bitmask(i)
			cnt++
		}
	}
	bits.ResetCount(cnt)
}

func matchStringGreaterEqual(a types.StringAccessor, val []byte, bits, mask *bitset.Bitset) {
	set := bits.Bytes()
	var cnt int
	if mask != nil {
		msk := mask.Bytes()
		for i := range a.Len() {
			bit := bitmask(i)
			if (msk[i>>3] & bit) == 0 {
				continue
			}
			if bytes.Compare(a.Get(i), val) < 0 {
				continue
			}
			set[i>>3] |= bit
			cnt++
		}
	} else {
		for i := range a.Len() {
			if bytes.Compare(a.Get(i), val) < 0 {
				continue
			}
			set[i>>3] |= bitmask(i)
			cnt++
		}
	}
	bits.ResetCount(cnt)
}

func matchStringBetween(a types.StringAccessor, from, to []byte, bits, mask *bitset.Bitset) {
	if a.Len() == 0 {
		return
	}

	if bytes.Equal(from, to) {
		matchStringEqual(a, from, bits, mask)
		return
	}

	set := bits.Bytes()
	var cnt int
	if mask != nil {
		msk := mask.Bytes()
		for i := range a.Len() {
			bit := bitmask(i)
			if (msk[i>>3] & bit) == 0 {
				continue
			}
			v := a.Get(i)
			if bytes.Compare(v, from) < 0 {
				continue
			}
			if bytes.Compare(v, to) > 0 {
				continue
			}
			set[i>>3] |= bit
			cnt++
		}
	} else {
		for i := range a.Len() {
			v := a.Get(i)
			if bytes.Compare(v, from) < 0 {
				continue
			}
			if bytes.Compare(v, to) > 0 {
				continue
			}
			set[i>>3] |= bitmask(i)
			cnt++
		}
	}
	bits.ResetCount(cnt)
}

func bitmask(i int) byte {
	return byte(1 << (i & 7))
}

func bytemask(size int) byte {
	return byte(0xff >> (7 - uint(size-1)&0x7) & 0xff)
}
