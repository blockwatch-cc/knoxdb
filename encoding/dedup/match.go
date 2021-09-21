// Copyright (c) 2018-2021 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package dedup

import (
	"blockwatch.cc/knoxdb/vec"
	"bytes"
)

func bitmask(i int) byte {
	return byte(1 << uint(i&0x7))
}

func bytemask(size int) byte {
	return byte(0xff >> (7 - uint(size-1)&0x7) & 0xff)
}

func ensureBitfieldSize(bits *vec.Bitset, sz int) *vec.Bitset {
	if bits == nil {
		bits = vec.NewBitset(sz)
	} else {
		bits.Grow(sz)
	}
	return bits
}

func matchEqual(a ByteArray, val []byte, bitvec, maskvec *vec.Bitset) *vec.Bitset {
	bitvec = ensureBitfieldSize(bitvec, a.Len())
	bits := bitvec.Bytes()
	mask := maskvec.Bytes()
	var cnt int
	if mask != nil {
		for i := 0; i < a.Len(); i++ {
			v := a.Elem(i)
			bit := bitmask(i)
			if (mask[i>>3] & bit) == 0 {
				continue
			}
			if bytes.Compare(v, val) != 0 {
				continue
			}
			bits[i>>3] |= bit
			cnt++
		}
	} else {
		for i := 0; i < a.Len(); i++ {
			v := a.Elem(i)
			if bytes.Compare(v, val) != 0 {
				continue
			}
			bits[i>>3] |= bitmask(i)
			cnt++
		}
	}
	bitvec.ResetCount(cnt)
	return bitvec
}

func matchNotEqual(a ByteArray, val []byte, bitvec, maskvec *vec.Bitset) *vec.Bitset {
	bitvec = ensureBitfieldSize(bitvec, a.Len())
	bits := bitvec.Bytes()
	mask := maskvec.Bytes()
	var cnt int
	if mask != nil {
		for i := 0; i < a.Len(); i++ {
			v := a.Elem(i)
			bit := bitmask(i)
			if mask != nil && (mask[i>>3]&bit) == 0 {
				continue
			}
			if bytes.Compare(v, val) == 0 {
				continue
			}
			bits[i>>3] |= bit
			cnt++
		}
	} else {
		for i := 0; i < a.Len(); i++ {
			v := a.Elem(i)
			if bytes.Compare(v, val) == 0 {
				continue
			}
			bits[i>>3] |= bitmask(i)
			cnt++
		}
	}
	bitvec.ResetCount(cnt)
	return bitvec
}

func matchLessThan(a ByteArray, val []byte, bitvec, maskvec *vec.Bitset) *vec.Bitset {
	bitvec = ensureBitfieldSize(bitvec, a.Len())
	bits := bitvec.Bytes()
	mask := maskvec.Bytes()
	var cnt int
	if mask != nil {
		for i := 0; i < a.Len(); i++ {
			v := a.Elem(i)
			bit := bitmask(i)
			if mask != nil && (mask[i>>3]&bit) == 0 {
				continue
			}
			if bytes.Compare(v, val) >= 0 {
				continue
			}
			bits[i>>3] |= bit
			cnt++
		}
	} else {
		for i := 0; i < a.Len(); i++ {
			v := a.Elem(i)
			if bytes.Compare(v, val) >= 0 {
				continue
			}
			bits[i>>3] |= bitmask(i)
			cnt++
		}
	}
	bitvec.ResetCount(cnt)
	return bitvec
}

func matchLessThanEqual(a ByteArray, val []byte, bitvec, maskvec *vec.Bitset) *vec.Bitset {
	bitvec = ensureBitfieldSize(bitvec, a.Len())
	bits := bitvec.Bytes()
	mask := maskvec.Bytes()
	var cnt int
	if mask != nil {
		for i := 0; i < a.Len(); i++ {
			v := a.Elem(i)
			bit := bitmask(i)
			if mask != nil && (mask[i>>3]&bit) == 0 {
				continue
			}
			if bytes.Compare(v, val) > 0 {
				continue
			}
			bits[i>>3] |= bit
			cnt++
		}
	} else {
		for i := 0; i < a.Len(); i++ {
			v := a.Elem(i)
			if bytes.Compare(v, val) > 0 {
				continue
			}
			bits[i>>3] |= bitmask(i)
			cnt++
		}
	}
	bitvec.ResetCount(cnt)
	return bitvec
}

func matchGreaterThan(a ByteArray, val []byte, bitvec, maskvec *vec.Bitset) *vec.Bitset {
	bitvec = ensureBitfieldSize(bitvec, a.Len())
	bits := bitvec.Bytes()
	mask := maskvec.Bytes()
	var cnt int
	if mask != nil {
		for i := 0; i < a.Len(); i++ {
			v := a.Elem(i)
			bit := bitmask(i)
			if mask != nil && (mask[i>>3]&bit) == 0 {
				continue
			}
			if bytes.Compare(v, val) <= 0 {
				continue
			}
			bits[i>>3] |= bit
			cnt++
		}
	} else {
		for i := 0; i < a.Len(); i++ {
			v := a.Elem(i)
			if bytes.Compare(v, val) <= 0 {
				continue
			}
			bits[i>>3] |= bitmask(i)
			cnt++
		}
	}
	bitvec.ResetCount(cnt)
	return bitvec
}

func matchGreaterThanEqual(a ByteArray, val []byte, bitvec, maskvec *vec.Bitset) *vec.Bitset {
	bitvec = ensureBitfieldSize(bitvec, a.Len())
	bits := bitvec.Bytes()
	mask := maskvec.Bytes()
	var cnt int
	if mask != nil {
		for i := 0; i < a.Len(); i++ {
			v := a.Elem(i)
			bit := bitmask(i)
			if mask != nil && (mask[i>>3]&bit) == 0 {
				continue
			}
			if bytes.Compare(v, val) < 0 {
				continue
			}
			bits[i>>3] |= bit
			cnt++
		}
	} else {
		for i := 0; i < a.Len(); i++ {
			v := a.Elem(i)
			if bytes.Compare(v, val) < 0 {
				continue
			}
			bits[i>>3] |= bitmask(i)
			cnt++
		}
	}
	bitvec.ResetCount(cnt)
	return bitvec
}

func matchBetween(a ByteArray, from, to []byte, bitvec, maskvec *vec.Bitset) *vec.Bitset {
	bitvec = ensureBitfieldSize(bitvec, a.Len())
	bits := bitvec.Bytes()
	mask := maskvec.Bytes()
	// short-cut for empty min
	if a.Len() == 0 {
		if mask != nil {
			copy(bits, mask)
			bitvec.ResetCount()
		} else {
			bits[0] = 0xff
			for bp := 1; bp < len(bits); bp *= 2 {
				copy(bits[bp:], bits[:bp])
			}
			bits[len(bits)-1] &= bytemask(a.Len())
			bitvec.ResetCount(a.Len())
		}
		return bitvec
	}

	// make sure min/max are in correct order
	if d := bytes.Compare(from, to); d < 0 {
		to, from = from, to
	} else if d == 0 {
		return matchEqual(a, from, bitvec, maskvec)
	}

	var cnt int
	if mask != nil {
		for i := 0; i < a.Len(); i++ {
			v := a.Elem(i)
			bit := bitmask(i)
			if mask != nil && (mask[i>>3]&bit) == 0 {
				continue
			}
			if bytes.Compare(v, from) < 0 {
				continue
			}
			if bytes.Compare(v, to) > 0 {
				continue
			}
			bits[i>>3] |= bit
			cnt++
		}
	} else {
		for i := 0; i < a.Len(); i++ {
			v := a.Elem(i)
			if bytes.Compare(v, from) < 0 {
				continue
			}
			if bytes.Compare(v, to) > 0 {
				continue
			}
			bits[i>>3] |= bitmask(i)
			cnt++
		}
	}
	bitvec.ResetCount(cnt)
	return bitvec

}

func minMax(a ByteArray) ([]byte, []byte) {
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
	cmin := make([]byte, len(min))
	copy(cmin, min)
	cmax := make([]byte, len(max))
	copy(cmax, max)
	return cmin, cmax
}
