// Copyright (c) 2013 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package dedup

import (
	"bytes"

	"blockwatch.cc/knoxdb/internal/bitset"
	"golang.org/x/exp/slices"
)

func bitmask(i int) byte {
	return byte(1 << uint(i&0x7))
}

func bytemask(size int) byte {
	return byte(0xff >> (7 - uint(size-1)&0x7) & 0xff)
}

func matchEqual(a ByteArray, val []byte, bits, mask *bitset.Bitset) *bitset.Bitset {
	bits = bits.Resize(a.Len())
	bbuf := bits.Bytes()
	var cnt int
	if mask != nil {
		mbuf := mask.Bytes()
		for i, k := 0, a.Len(); i < k; i++ {
			bit := bitmask(i)
			if (mbuf[i>>3] & bit) == 0 {
				continue
			}
			if !bytes.Equal(a.Elem(i), val) {
				continue
			}
			bbuf[i>>3] |= bit
			cnt++
		}
	} else {
		for i, k := 0, a.Len(); i < k; i++ {
			if !bytes.Equal(a.Elem(i), val) {
				continue
			}
			bbuf[i>>3] |= bitmask(i)
			cnt++
		}
	}
	bits.ResetCount(cnt)
	return bits
}

func matchNotEqual(a ByteArray, val []byte, bits, mask *bitset.Bitset) *bitset.Bitset {
	bits = bits.Resize(a.Len())
	bbuf := bits.Bytes()
	var cnt int
	if mask != nil {
		mbuf := mask.Bytes()
		for i, k := 0, a.Len(); i < k; i++ {
			bit := bitmask(i)
			if (mbuf[i>>3] & bit) == 0 {
				continue
			}
			if bytes.Equal(a.Elem(i), val) {
				continue
			}
			bbuf[i>>3] |= bit
			cnt++
		}
	} else {
		for i, k := 0, a.Len(); i < k; i++ {
			if bytes.Equal(a.Elem(i), val) {
				continue
			}
			bbuf[i>>3] |= bitmask(i)
			cnt++
		}
	}
	bits.ResetCount(cnt)
	return bits
}

func matchLess(a ByteArray, val []byte, bits, mask *bitset.Bitset) *bitset.Bitset {
	bits = bits.Resize(a.Len())
	bbuf := bits.Bytes()
	var cnt int
	if mask != nil {
		mbuf := mask.Bytes()
		for i, k := 0, a.Len(); i < k; i++ {
			bit := bitmask(i)
			if (mbuf[i>>3] & bit) == 0 {
				continue
			}
			if bytes.Compare(a.Elem(i), val) >= 0 {
				continue
			}
			bbuf[i>>3] |= bit
			cnt++
		}
	} else {
		for i, k := 0, a.Len(); i < k; i++ {
			if bytes.Compare(a.Elem(i), val) >= 0 {
				continue
			}
			bbuf[i>>3] |= bitmask(i)
			cnt++
		}
	}
	bits.ResetCount(cnt)
	return bits
}

func matchLessEqual(a ByteArray, val []byte, bits, mask *bitset.Bitset) *bitset.Bitset {
	bits = bits.Resize(a.Len())
	bbuf := bits.Bytes()
	var cnt int
	if mask != nil {
		mbuf := mask.Bytes()
		for i, k := 0, a.Len(); i < k; i++ {
			bit := bitmask(i)
			if (mbuf[i>>3] & bit) == 0 {
				continue
			}
			if bytes.Compare(a.Elem(i), val) > 0 {
				continue
			}
			bbuf[i>>3] |= bit
			cnt++
		}
	} else {
		for i, k := 0, a.Len(); i < k; i++ {
			if bytes.Compare(a.Elem(i), val) > 0 {
				continue
			}
			bbuf[i>>3] |= bitmask(i)
			cnt++
		}
	}
	bits.ResetCount(cnt)
	return bits
}

func matchGreater(a ByteArray, val []byte, bits, mask *bitset.Bitset) *bitset.Bitset {
	bits = bits.Resize(a.Len())
	bbuf := bits.Bytes()
	var cnt int
	if mask != nil {
		mbuf := mask.Bytes()
		for i, k := 0, a.Len(); i < k; i++ {
			bit := bitmask(i)
			if (mbuf[i>>3] & bit) == 0 {
				continue
			}
			if bytes.Compare(a.Elem(i), val) <= 0 {
				continue
			}
			bbuf[i>>3] |= bit
			cnt++
		}
	} else {
		for i, k := 0, a.Len(); i < k; i++ {
			if bytes.Compare(a.Elem(i), val) <= 0 {
				continue
			}
			bbuf[i>>3] |= bitmask(i)
			cnt++
		}
	}
	bits.ResetCount(cnt)
	return bits
}

func matchGreaterEqual(a ByteArray, val []byte, bits, mask *bitset.Bitset) *bitset.Bitset {
	bits = bits.Resize(a.Len())
	bbuf := bits.Bytes()
	var cnt int
	if mask != nil {
		mbuf := mask.Bytes()
		for i, k := 0, a.Len(); i < k; i++ {
			bit := bitmask(i)
			if (mbuf[i>>3] & bit) == 0 {
				continue
			}
			if bytes.Compare(a.Elem(i), val) < 0 {
				continue
			}
			bbuf[i>>3] |= bit
			cnt++
		}
	} else {
		for i, k := 0, a.Len(); i < k; i++ {
			if bytes.Compare(a.Elem(i), val) < 0 {
				continue
			}
			bbuf[i>>3] |= bitmask(i)
			cnt++
		}
	}
	bits.ResetCount(cnt)
	return bits
}

func matchBetween(a ByteArray, from, to []byte, bits, mask *bitset.Bitset) *bitset.Bitset {
	bits = bits.Resize(a.Len())
	bbuf := bits.Bytes()
	// short-cut for empty min
	if a.Len() == 0 {
		if mask != nil {
			copy(bbuf, mask.Bytes())
			bits.ResetCount(-1)
		} else {
			bbuf[0] = 0xff
			for bp := 1; bp < len(bbuf); bp *= 2 {
				copy(bbuf[bp:], bbuf[:bp])
			}
			bbuf[len(bbuf)-1] &= bytemask(a.Len())
			bits.ResetCount(a.Len())
		}
		return bits
	}

	if bytes.Equal(from, to) {
		return matchEqual(a, from, bits, mask)
	}

	var cnt int
	if mask != nil {
		mbuf := mask.Bytes()
		for i, k := 0, a.Len(); i < k; i++ {
			bit := bitmask(i)
			if (mbuf[i>>3] & bit) == 0 {
				continue
			}
			v := a.Elem(i)
			if bytes.Compare(v, from) < 0 {
				continue
			}
			if bytes.Compare(v, to) > 0 {
				continue
			}
			bbuf[i>>3] |= bit
			cnt++
		}
	} else {
		for i, k := 0, a.Len(); i < k; i++ {
			v := a.Elem(i)
			if bytes.Compare(v, from) < 0 {
				continue
			}
			if bytes.Compare(v, to) > 0 {
				continue
			}
			bbuf[i>>3] |= bitmask(i)
			cnt++
		}
	}
	bits.ResetCount(cnt)
	return bits

}

func minMaxArr(a ByteArray) ([]byte, []byte) {
	var min, max []byte
	switch l := a.Len(); l {
	case 0:
		// nothing
	case 1:
		min = a.Elem(0)
		max = min
	default:
		// If there is more than one element, then initialize min and max
		if x, y := a.Elem(0), a.Elem(1); bytes.Compare(x, y) > 0 {
			max = x
			min = y
		} else {
			max = y
			min = x
		}

		for i := 2; i < l; i++ {
			if x := a.Elem(i); bytes.Compare(x, max) > 0 {
				max = x
			} else if bytes.Compare(x, min) < 0 {
				min = x
			}
		}
	}
	// copy to avoid reference
	return slices.Clone(min), slices.Clone(max)
}

func minArr(a ByteArray) []byte {
	l := a.Len()
	if l == 0 {
		return nil
	}
	val := a.Elem(0)
	for i := 1; i < l; i++ {
		e := a.Elem(i)
		if bytes.Compare(e, val) < 0 {
			val = e
		}
	}
	return slices.Clone(val)
}

func maxArr(a ByteArray) []byte {
	l := a.Len()
	if l == 0 {
		return nil
	}
	val := a.Elem(0)
	for i := 1; i < l; i++ {
		e := a.Elem(i)
		if bytes.Compare(e, val) > 0 {
			val = e
		}
	}
	return slices.Clone(val)
}
