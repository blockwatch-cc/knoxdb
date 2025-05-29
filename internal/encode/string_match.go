// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package encode

import (
	"bytes"

	"blockwatch.cc/knoxdb/internal/bitset"
	"blockwatch.cc/knoxdb/internal/types"
)

func matchStringEqual(a types.StringReader, val []byte, bits, mask *bitset.Bitset) {
	set := bits.Bytes()
	var cnt int
	if mask != nil {
		for i := range mask.Iterator() {
			if !bytes.Equal(a.Get(i), val) {
				continue
			}
			set[i>>3] |= bitmask(i)
			cnt++
		}
	} else {
		for i, v := range a.Iterator() {
			if !bytes.Equal(v, val) {
				continue
			}
			set[i>>3] |= bitmask(i)
			cnt++
		}
	}
	bits.ResetCount(cnt)
}

func matchStringNotEqual(a types.StringReader, val []byte, bits, mask *bitset.Bitset) {
	set := bits.Bytes()
	var cnt int
	if mask != nil {
		for i := range mask.Iterator() {
			if bytes.Equal(a.Get(i), val) {
				continue
			}
			set[i>>3] |= bitmask(i)
			cnt++
		}
	} else {
		for i, v := range a.Iterator() {
			if bytes.Equal(v, val) {
				continue
			}
			set[i>>3] |= bitmask(i)
			cnt++
		}
	}
	bits.ResetCount(cnt)
}

func matchStringLess(a types.StringReader, val []byte, bits, mask *bitset.Bitset) {
	set := bits.Bytes()
	var cnt int
	if mask != nil {
		for i := range mask.Iterator() {
			if bytes.Compare(a.Get(i), val) >= 0 {
				continue
			}
			set[i>>3] |= bitmask(i)
			cnt++
		}
	} else {
		for i, v := range a.Iterator() {
			if bytes.Compare(v, val) >= 0 {
				continue
			}
			set[i>>3] |= bitmask(i)
			cnt++
		}
	}
	bits.ResetCount(cnt)
}

func matchStringLessEqual(a types.StringReader, val []byte, bits, mask *bitset.Bitset) {
	set := bits.Bytes()
	var cnt int
	if mask != nil {
		for i := range mask.Iterator() {
			if bytes.Compare(a.Get(i), val) > 0 {
				continue
			}
			set[i>>3] |= bitmask(i)
			cnt++
		}
	} else {
		for i, v := range a.Iterator() {
			if bytes.Compare(v, val) > 0 {
				continue
			}
			set[i>>3] |= bitmask(i)
			cnt++
		}
	}
	bits.ResetCount(cnt)
}

func matchStringGreater(a types.StringReader, val []byte, bits, mask *bitset.Bitset) {
	set := bits.Bytes()
	var cnt int
	if mask != nil {
		for i := range mask.Iterator() {
			if bytes.Compare(a.Get(i), val) <= 0 {
				continue
			}
			set[i>>3] |= bitmask(i)
			cnt++
		}
	} else {
		for i, v := range a.Iterator() {
			if bytes.Compare(v, val) <= 0 {
				continue
			}
			set[i>>3] |= bitmask(i)
			cnt++
		}
	}
	bits.ResetCount(cnt)
}

func matchStringGreaterEqual(a types.StringReader, val []byte, bits, mask *bitset.Bitset) {
	set := bits.Bytes()
	var cnt int
	if mask != nil {
		for i := range mask.Iterator() {
			if bytes.Compare(a.Get(i), val) < 0 {
				continue
			}
			set[i>>3] |= bitmask(i)
			cnt++
		}
	} else {
		for i, v := range a.Iterator() {
			if bytes.Compare(v, val) < 0 {
				continue
			}
			set[i>>3] |= bitmask(i)
			cnt++
		}
	}
	bits.ResetCount(cnt)
}

func matchStringBetween(a types.StringReader, from, to []byte, bits, mask *bitset.Bitset) {
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
		for i := range mask.Iterator() {
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
	} else {
		for i, v := range a.Iterator() {
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
