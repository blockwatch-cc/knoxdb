// Copyright (c) 2023 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package cmp

import (
	"math/bits"

	"blockwatch.cc/knoxdb/internal/bitset"
	"blockwatch.cc/knoxdb/pkg/num"
	"blockwatch.cc/knoxdb/pkg/util"
)

func cmp_i256_eq(src *num.Int256Stride, val num.Int256, res, mask []byte) int64 {
	var cnt int64
	n := src.Len() / 8
	var idx int
	if mask != nil {
		for i := range n {
			m := mask[i]
			if m == 0 {
				idx += 8
				continue
			}
			a1 := src.X0[idx] == int64(val[0]) && src.X1[idx] == val[1] && src.X2[idx] == val[2] && src.X3[idx] == val[3]
			a2 := src.X0[idx+1] == int64(val[0]) && src.X1[idx+1] == val[1] && src.X2[idx+1] == val[2] && src.X3[idx+1] == val[3]
			a3 := src.X0[idx+2] == int64(val[0]) && src.X1[idx+2] == val[1] && src.X2[idx+2] == val[2] && src.X3[idx+2] == val[3]
			a4 := src.X0[idx+3] == int64(val[0]) && src.X1[idx+3] == val[1] && src.X2[idx+3] == val[2] && src.X3[idx+3] == val[3]
			// note: bitset bytes store bits inverted for efficient index algo
			b := util.Bool2byte(a1) + util.Bool2byte(a2)<<1 + util.Bool2byte(a3)<<2 + util.Bool2byte(a4)<<3
			a1 = src.X0[idx+4] == int64(val[0]) && src.X1[idx+4] == val[1] && src.X2[idx+4] == val[2] && src.X3[idx+4] == val[3]
			a2 = src.X0[idx+5] == int64(val[0]) && src.X1[idx+5] == val[1] && src.X2[idx+5] == val[2] && src.X3[idx+5] == val[3]
			a3 = src.X0[idx+6] == int64(val[0]) && src.X1[idx+6] == val[1] && src.X2[idx+6] == val[2] && src.X3[idx+6] == val[3]
			a4 = src.X0[idx+7] == int64(val[0]) && src.X1[idx+7] == val[1] && src.X2[idx+7] == val[2] && src.X3[idx+7] == val[3]
			b += util.Bool2byte(a1)<<4 + util.Bool2byte(a2)<<5 + util.Bool2byte(a3)<<6 + util.Bool2byte(a4)<<7
			res[i] = b & m
			cnt += int64(bits.OnesCount8(b))
			idx += 8
		}

		// tail
		if src.Len()%8 > 0 {
			for i, l := n*8, src.Len(); i < l; i++ {
				bit := byte(1) << (i & 7)
				if (mask[n] & bit) == 0 {
					continue
				}
				if src.X0[i] != int64(val[0]) || src.X1[i] != val[1] || src.X2[i] != val[2] || src.X3[i] != val[3] {
					continue
				}
				res[n] |= bit
				cnt++
			}
		}

	} else {
		for i := range n {
			a1 := src.X0[idx] == int64(val[0]) && src.X1[idx] == val[1] && src.X2[idx] == val[2] && src.X3[idx] == val[3]
			a2 := src.X0[idx+1] == int64(val[0]) && src.X1[idx+1] == val[1] && src.X2[idx+1] == val[2] && src.X3[idx+1] == val[3]
			a3 := src.X0[idx+2] == int64(val[0]) && src.X1[idx+2] == val[1] && src.X2[idx+2] == val[2] && src.X3[idx+2] == val[3]
			a4 := src.X0[idx+3] == int64(val[0]) && src.X1[idx+3] == val[1] && src.X2[idx+3] == val[2] && src.X3[idx+3] == val[3]
			// note: bitset bytes store bits inverted for efficient index algo
			b := util.Bool2byte(a1) + util.Bool2byte(a2)<<1 + util.Bool2byte(a3)<<2 + util.Bool2byte(a4)<<3
			a1 = src.X0[idx+4] == int64(val[0]) && src.X1[idx+4] == val[1] && src.X2[idx+4] == val[2] && src.X3[idx+4] == val[3]
			a2 = src.X0[idx+5] == int64(val[0]) && src.X1[idx+5] == val[1] && src.X2[idx+5] == val[2] && src.X3[idx+5] == val[3]
			a3 = src.X0[idx+6] == int64(val[0]) && src.X1[idx+6] == val[1] && src.X2[idx+6] == val[2] && src.X3[idx+6] == val[3]
			a4 = src.X0[idx+7] == int64(val[0]) && src.X1[idx+7] == val[1] && src.X2[idx+7] == val[2] && src.X3[idx+7] == val[3]
			b += util.Bool2byte(a1)<<4 + util.Bool2byte(a2)<<5 + util.Bool2byte(a3)<<6 + util.Bool2byte(a4)<<7
			res[i] = b
			cnt += int64(bits.OnesCount8(b))
			idx += 8
		}

		// tail
		if src.Len()%8 > 0 {
			for i, l := n*8, src.Len(); i < l; i++ {
				if src.X0[i] == int64(val[0]) && src.X1[i] == val[1] && src.X2[i] == val[2] && src.X3[i] == val[3] {
					res[n] |= 1 << (i & 7)
					cnt++
				}
			}
		}
	}
	return cnt
}

func cmp_i256_ne(src *num.Int256Stride, val num.Int256, res, mask []byte) int64 {
	var cnt int64
	n := src.Len() / 8
	var idx int
	if mask != nil {
		for i := range n {
			m := mask[i]
			if m == 0 {
				idx += 8
				continue
			}
			a1 := src.X0[idx] != int64(val[0]) || src.X1[idx] != val[1] || src.X2[idx] != val[2] || src.X3[idx] != val[3]
			a2 := src.X0[idx+1] != int64(val[0]) || src.X1[idx+1] != val[1] || src.X2[idx+1] != val[2] || src.X3[idx+1] != val[3]
			a3 := src.X0[idx+2] != int64(val[0]) || src.X1[idx+2] != val[1] || src.X2[idx+2] != val[2] || src.X3[idx+2] != val[3]
			a4 := src.X0[idx+3] != int64(val[0]) || src.X1[idx+3] != val[1] || src.X2[idx+3] != val[2] || src.X3[idx+3] != val[3]
			// note: bitset bytes store bits inverted for efficient index algo
			b := util.Bool2byte(a1) + util.Bool2byte(a2)<<1 + util.Bool2byte(a3)<<2 + util.Bool2byte(a4)<<3
			a1 = src.X0[idx+4] != int64(val[0]) || src.X1[idx+4] != val[1] || src.X2[idx+4] != val[2] || src.X3[idx+4] != val[3]
			a2 = src.X0[idx+5] != int64(val[0]) || src.X1[idx+5] != val[1] || src.X2[idx+5] != val[2] || src.X3[idx+5] != val[3]
			a3 = src.X0[idx+6] != int64(val[0]) || src.X1[idx+6] != val[1] || src.X2[idx+6] != val[2] || src.X3[idx+6] != val[3]
			a4 = src.X0[idx+7] != int64(val[0]) || src.X1[idx+7] != val[1] || src.X2[idx+7] != val[2] || src.X3[idx+7] != val[3]
			b += util.Bool2byte(a1)<<4 + util.Bool2byte(a2)<<5 + util.Bool2byte(a3)<<6 + util.Bool2byte(a4)<<7
			res[i] = b & m
			cnt += int64(bits.OnesCount8(b))
			idx += 8
		}

		// tail
		if src.Len()%8 > 0 {
			for i, l := n*8, src.Len(); i < l; i++ {
				bit := byte(1) << (i & 7)
				if (mask[n] & bit) == 0 {
					continue
				}
				if src.X0[i] == int64(val[0]) && src.X1[i] == val[1] && src.X2[i] == val[2] && src.X3[i] == val[3] {
					continue
				}
				res[n] |= bit
				cnt++
			}
		}

	} else {
		for i := range n {
			a1 := src.X0[idx] != int64(val[0]) || src.X1[idx] != val[1] || src.X2[idx] != val[2] || src.X3[idx] != val[3]
			a2 := src.X0[idx+1] != int64(val[0]) || src.X1[idx+1] != val[1] || src.X2[idx+1] != val[2] || src.X3[idx+1] != val[3]
			a3 := src.X0[idx+2] != int64(val[0]) || src.X1[idx+2] != val[1] || src.X2[idx+2] != val[2] || src.X3[idx+2] != val[3]
			a4 := src.X0[idx+3] != int64(val[0]) || src.X1[idx+3] != val[1] || src.X2[idx+3] != val[2] || src.X3[idx+3] != val[3]
			// note: bitset bytes store bits inverted for efficient index algo
			b := util.Bool2byte(a1) + util.Bool2byte(a2)<<1 + util.Bool2byte(a3)<<2 + util.Bool2byte(a4)<<3
			a1 = src.X0[idx+4] != int64(val[0]) || src.X1[idx+4] != val[1] || src.X2[idx+4] != val[2] || src.X3[idx+4] != val[3]
			a2 = src.X0[idx+5] != int64(val[0]) || src.X1[idx+5] != val[1] || src.X2[idx+5] != val[2] || src.X3[idx+5] != val[3]
			a3 = src.X0[idx+6] != int64(val[0]) || src.X1[idx+6] != val[1] || src.X2[idx+6] != val[2] || src.X3[idx+6] != val[3]
			a4 = src.X0[idx+7] != int64(val[0]) || src.X1[idx+7] != val[1] || src.X2[idx+7] != val[2] || src.X3[idx+7] != val[3]
			b += util.Bool2byte(a1)<<4 + util.Bool2byte(a2)<<5 + util.Bool2byte(a3)<<6 + util.Bool2byte(a4)<<7
			res[i] = b
			cnt += int64(bits.OnesCount8(b))
			idx += 8
		}

		// tail
		if src.Len()%8 > 0 {
			for i, l := n*8, src.Len(); i < l; i++ {
				if src.X0[i] != int64(val[0]) || src.X1[i] != val[1] || src.X2[i] != val[2] || src.X3[i] != val[3] {
					res[n] |= 1 << (i & 7)
					cnt++
				}
			}
		}
	}
	return cnt
}

func cmp_i256_lt(src *num.Int256Stride, val num.Int256, bits, mask []byte) int64 {
	var cnt int64
	if mask != nil {
		for i := range bitset.NewFromBytes(mask, src.Len()).Iterator() {
			if src.Get(i).Ge(val) {
				continue
			}
			bits[i>>3] |= byte(1) << (i & 7)
			cnt++
		}
	} else {
		for i, v := range src.Iterator() {
			if v.Ge(val) {
				continue
			}
			bits[i>>3] |= byte(1) << (i & 7)
			cnt++
		}
	}
	return cnt
}

func cmp_i256_le(src *num.Int256Stride, val num.Int256, bits, mask []byte) int64 {
	var cnt int64
	if mask != nil {
		for i := range bitset.NewFromBytes(mask, src.Len()).Iterator() {
			if src.Get(i).Gt(val) {
				continue
			}
			bits[i>>3] |= byte(1) << (i & 7)
			cnt++
		}
	} else {
		for i, v := range src.Iterator() {
			if v.Gt(val) {
				continue
			}
			bits[i>>3] |= byte(1) << (i & 7)
			cnt++
		}
	}
	return cnt
}

func cmp_i256_gt(src *num.Int256Stride, val num.Int256, bits, mask []byte) int64 {
	var cnt int64
	if mask != nil {
		for i := range bitset.NewFromBytes(mask, src.Len()).Iterator() {
			if src.Get(i).Le(val) {
				continue
			}
			bits[i>>3] |= byte(1) << (i & 7)
			cnt++
		}
	} else {
		for i, v := range src.Iterator() {
			if v.Le(val) {
				continue
			}
			bits[i>>3] |= byte(1) << (i & 7)
			cnt++
		}
	}
	return cnt
}

func cmp_i256_ge(src *num.Int256Stride, val num.Int256, bits, mask []byte) int64 {
	var cnt int64
	if mask != nil {
		for i := range bitset.NewFromBytes(mask, src.Len()).Iterator() {
			if src.Get(i).Lt(val) {
				continue
			}
			bits[i>>3] |= byte(1) << (i & 7)
			cnt++
		}
	} else {
		for i, v := range src.Iterator() {
			if v.Lt(val) {
				continue
			}
			bits[i>>3] |= byte(1) << (i & 7)
			cnt++
		}
	}
	return cnt
}

func cmp_i256_bw(src *num.Int256Stride, a, b num.Int256, bits, mask []byte) int64 {
	diff := b.Sub(a).Add64(1).Uint256()
	var cnt int64
	if mask != nil {
		for i := range bitset.NewFromBytes(mask, src.Len()).Iterator() {
			if src.Get(i).Sub(a).Uint256().Ge(diff) {
				continue
			}
			bits[i>>3] |= byte(1) << (i & 7)
			cnt++
		}
	} else {
		for i, v := range src.Iterator() {
			if v.Sub(a).Uint256().Ge(diff) {
				continue
			}
			bits[i>>3] |= byte(1) << (i & 7)
			cnt++
		}
	}
	return cnt
}
