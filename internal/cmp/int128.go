// Copyright (c) 2023 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package cmp

import (
	"math/bits"

	"blockwatch.cc/knoxdb/pkg/num"
	"blockwatch.cc/knoxdb/pkg/util"
)

func cmp_i128_eq(src num.Int128Stride, val num.Int128, res, mask []byte) int64 {
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
			a1 := src.X0[idx] == int64(val[0]) && src.X1[idx] == val[1]
			a2 := src.X0[idx+1] == int64(val[0]) && src.X1[idx+1] == val[1]
			a3 := src.X0[idx+2] == int64(val[0]) && src.X1[idx+2] == val[1]
			a4 := src.X0[idx+3] == int64(val[0]) && src.X1[idx+3] == val[1]
			// note: bitset bytes store bits inverted for efficient index algo
			b := util.Bool2byte(a1) + util.Bool2byte(a2)<<1 + util.Bool2byte(a3)<<2 + util.Bool2byte(a4)<<3
			a1 = src.X0[idx+4] == int64(val[0]) && src.X1[idx+4] == val[1]
			a2 = src.X0[idx+5] == int64(val[0]) && src.X1[idx+5] == val[1]
			a3 = src.X0[idx+6] == int64(val[0]) && src.X1[idx+6] == val[1]
			a4 = src.X0[idx+7] == int64(val[0]) && src.X1[idx+7] == val[1]
			b += util.Bool2byte(a1)<<4 + util.Bool2byte(a2)<<5 + util.Bool2byte(a3)<<6 + util.Bool2byte(a4)<<7
			res[i] = b & m
			cnt += int64(bits.OnesCount8(b))
			idx += 8
		}

		// tail
		if src.Len()&7 > 0 {
			for i, l := n*8, src.Len(); i < l; i++ {
				bit := byte(1) << (i & 7)
				if (mask[n] & bit) == 0 {
					continue
				}
				if src.X0[i] != int64(val[0]) || src.X1[i] != val[1] {
					continue
				}
				res[n] |= bit
				cnt++
			}
		}

	} else {
		for i := range n {
			a1 := src.X0[idx] == int64(val[0]) && src.X1[idx] == val[1]
			a2 := src.X0[idx+1] == int64(val[0]) && src.X1[idx+1] == val[1]
			a3 := src.X0[idx+2] == int64(val[0]) && src.X1[idx+2] == val[1]
			a4 := src.X0[idx+3] == int64(val[0]) && src.X1[idx+3] == val[1]
			// note: bitset bytes store bits inverted for efficient index algo
			b := util.Bool2byte(a1) + util.Bool2byte(a2)<<1 + util.Bool2byte(a3)<<2 + util.Bool2byte(a4)<<3
			a1 = src.X0[idx+4] == int64(val[0]) && src.X1[idx+4] == val[1]
			a2 = src.X0[idx+5] == int64(val[0]) && src.X1[idx+5] == val[1]
			a3 = src.X0[idx+6] == int64(val[0]) && src.X1[idx+6] == val[1]
			a4 = src.X0[idx+7] == int64(val[0]) && src.X1[idx+7] == val[1]
			b += util.Bool2byte(a1)<<4 + util.Bool2byte(a2)<<5 + util.Bool2byte(a3)<<6 + util.Bool2byte(a4)<<7
			res[i] = b
			cnt += int64(bits.OnesCount8(b))
			idx += 8
		}

		// tail
		if src.Len()&7 > 0 {
			for i, l := n*8, src.Len(); i < l; i++ {
				if src.X0[i] == int64(val[0]) && src.X1[i] == val[1] {
					res[n] |= 1 << (i & 7)
					cnt++
				}
			}
		}
	}
	return cnt
}

func cmp_i128_ne(src num.Int128Stride, val num.Int128, res, mask []byte) int64 {
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
			a1 := src.X0[idx] != int64(val[0]) || src.X1[idx] != val[1]
			a2 := src.X0[idx+1] != int64(val[0]) || src.X1[idx+1] != val[1]
			a3 := src.X0[idx+2] != int64(val[0]) || src.X1[idx+2] != val[1]
			a4 := src.X0[idx+3] != int64(val[0]) || src.X1[idx+3] != val[1]
			// note: bitset bytes store bits inverted for efficient index algo
			b := util.Bool2byte(a1) + util.Bool2byte(a2)<<1 + util.Bool2byte(a3)<<2 + util.Bool2byte(a4)<<3
			a1 = src.X0[idx+4] != int64(val[0]) || src.X1[idx+4] != val[1]
			a2 = src.X0[idx+5] != int64(val[0]) || src.X1[idx+5] != val[1]
			a3 = src.X0[idx+6] != int64(val[0]) || src.X1[idx+6] != val[1]
			a4 = src.X0[idx+7] != int64(val[0]) || src.X1[idx+7] != val[1]
			b += util.Bool2byte(a1)<<4 + util.Bool2byte(a2)<<5 + util.Bool2byte(a3)<<6 + util.Bool2byte(a4)<<7
			res[i] = b & m
			cnt += int64(bits.OnesCount8(b))
			idx += 8
		}

		// tail
		if src.Len()&7 > 0 {
			for i, l := n*8, src.Len(); i < l; i++ {
				bit := byte(1) << (i & 7)
				if (mask[n] & bit) == 0 {
					continue
				}
				if src.X0[i] == int64(val[0]) && src.X1[i] == val[1] {
					continue
				}
				res[n] |= bit
				cnt++
			}
		}

	} else {
		for i := range n {
			a1 := src.X0[idx] != int64(val[0]) || src.X1[idx] != val[1]
			a2 := src.X0[idx+1] != int64(val[0]) || src.X1[idx+1] != val[1]
			a3 := src.X0[idx+2] != int64(val[0]) || src.X1[idx+2] != val[1]
			a4 := src.X0[idx+3] != int64(val[0]) || src.X1[idx+3] != val[1]
			// note: bitset bytes store bits inverted for efficient index algo
			b := util.Bool2byte(a1) + util.Bool2byte(a2)<<1 + util.Bool2byte(a3)<<2 + util.Bool2byte(a4)<<3
			a1 = src.X0[idx+4] != int64(val[0]) || src.X1[idx+4] != val[1]
			a2 = src.X0[idx+5] != int64(val[0]) || src.X1[idx+5] != val[1]
			a3 = src.X0[idx+6] != int64(val[0]) || src.X1[idx+6] != val[1]
			a4 = src.X0[idx+7] != int64(val[0]) || src.X1[idx+7] != val[1]
			b += util.Bool2byte(a1)<<4 + util.Bool2byte(a2)<<5 + util.Bool2byte(a3)<<6 + util.Bool2byte(a4)<<7
			res[i] = b
			cnt += int64(bits.OnesCount8(b))
			idx += 8
		}

		// tail
		if src.Len()&7 > 0 {
			for i, l := n*8, src.Len(); i < l; i++ {
				if src.X0[i] != int64(val[0]) || src.X1[i] != val[1] {
					res[n] |= 1 << (i & 7)
					cnt++
				}
			}
		}
	}
	return cnt
}

func cmp_i128_lt(src num.Int128Stride, val num.Int128, res, mask []byte) int64 {
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
			a1 := src.X0[idx] < int64(val[0]) || (src.X0[idx] == int64(val[0]) && src.X1[idx] < val[1])
			a2 := src.X0[idx+1] < int64(val[0]) || (src.X0[idx+1] == int64(val[0]) && src.X1[idx+1] < val[1])
			a3 := src.X0[idx+2] < int64(val[0]) || (src.X0[idx+2] == int64(val[0]) && src.X1[idx+2] < val[1])
			a4 := src.X0[idx+3] < int64(val[0]) || (src.X0[idx+3] == int64(val[0]) && src.X1[idx+3] < val[1])
			// note: bitset bytes store bits inverted for efficient index algo
			b := util.Bool2byte(a1) + util.Bool2byte(a2)<<1 + util.Bool2byte(a3)<<2 + util.Bool2byte(a4)<<3
			a1 = src.X0[idx+4] < int64(val[0]) || (src.X0[idx+4] == int64(val[0]) && src.X1[idx+4] < val[1])
			a2 = src.X0[idx+5] < int64(val[0]) || (src.X0[idx+5] == int64(val[0]) && src.X1[idx+5] < val[1])
			a3 = src.X0[idx+6] < int64(val[0]) || (src.X0[idx+6] == int64(val[0]) && src.X1[idx+6] < val[1])
			a4 = src.X0[idx+7] < int64(val[0]) || (src.X0[idx+7] == int64(val[0]) && src.X1[idx+7] < val[1])
			b += util.Bool2byte(a1)<<4 + util.Bool2byte(a2)<<5 + util.Bool2byte(a3)<<6 + util.Bool2byte(a4)<<7
			res[i] = b & m
			cnt += int64(bits.OnesCount8(b))
			idx += 8
		}

		// tail
		if src.Len()&7 > 0 {
			for i, l := n*8, src.Len(); i < l; i++ {
				bit := byte(1) << (i & 7)
				if (mask[n] & bit) == 0 {
					continue
				}
				if src.X0[i] > int64(val[0]) || (src.X0[i] == int64(val[0]) && src.X1[i] >= val[1]) {
					continue
				}
				res[n] |= bit
				cnt++
			}
		}

	} else {
		for i := range n {
			a1 := src.X0[idx] < int64(val[0]) || (src.X0[idx] == int64(val[0]) && src.X1[idx] < val[1])
			a2 := src.X0[idx+1] < int64(val[0]) || (src.X0[idx+1] == int64(val[0]) && src.X1[idx+1] < val[1])
			a3 := src.X0[idx+2] < int64(val[0]) || (src.X0[idx+2] == int64(val[0]) && src.X1[idx+2] < val[1])
			a4 := src.X0[idx+3] < int64(val[0]) || (src.X0[idx+3] == int64(val[0]) && src.X1[idx+3] < val[1])
			// note: bitset bytes store bits inverted for efficient index algo
			b := util.Bool2byte(a1) + util.Bool2byte(a2)<<1 + util.Bool2byte(a3)<<2 + util.Bool2byte(a4)<<3
			a1 = src.X0[idx+4] < int64(val[0]) || (src.X0[idx+4] == int64(val[0]) && src.X1[idx+4] < val[1])
			a2 = src.X0[idx+5] < int64(val[0]) || (src.X0[idx+5] == int64(val[0]) && src.X1[idx+5] < val[1])
			a3 = src.X0[idx+6] < int64(val[0]) || (src.X0[idx+6] == int64(val[0]) && src.X1[idx+6] < val[1])
			a4 = src.X0[idx+7] < int64(val[0]) || (src.X0[idx+7] == int64(val[0]) && src.X1[idx+7] < val[1])
			b += util.Bool2byte(a1)<<4 + util.Bool2byte(a2)<<5 + util.Bool2byte(a3)<<6 + util.Bool2byte(a4)<<7
			res[i] = b
			cnt += int64(bits.OnesCount8(b))
			idx += 8
		}

		// tail
		if src.Len()&7 > 0 {
			for i, l := n*8, src.Len(); i < l; i++ {
				if src.X0[i] < int64(val[0]) || (src.X0[i] == int64(val[0]) && src.X1[i] < val[1]) {
					res[n] |= 1 << (i & 7)
					cnt++
				}
			}
		}
	}
	return cnt
}

func cmp_i128_le(src num.Int128Stride, val num.Int128, res, mask []byte) int64 {
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
			a1 := src.X0[idx] < int64(val[0]) || (src.X0[idx] == int64(val[0]) && src.X1[idx] <= val[1])
			a2 := src.X0[idx+1] < int64(val[0]) || (src.X0[idx+1] == int64(val[0]) && src.X1[idx+1] <= val[1])
			a3 := src.X0[idx+2] < int64(val[0]) || (src.X0[idx+2] == int64(val[0]) && src.X1[idx+2] <= val[1])
			a4 := src.X0[idx+3] < int64(val[0]) || (src.X0[idx+3] == int64(val[0]) && src.X1[idx+3] <= val[1])
			// note: bitset bytes store bits inverted for efficient index algo
			b := util.Bool2byte(a1) + util.Bool2byte(a2)<<1 + util.Bool2byte(a3)<<2 + util.Bool2byte(a4)<<3
			a1 = src.X0[idx+4] < int64(val[0]) || (src.X0[idx+4] == int64(val[0]) && src.X1[idx+4] <= val[1])
			a2 = src.X0[idx+5] < int64(val[0]) || (src.X0[idx+5] == int64(val[0]) && src.X1[idx+5] <= val[1])
			a3 = src.X0[idx+6] < int64(val[0]) || (src.X0[idx+6] == int64(val[0]) && src.X1[idx+6] <= val[1])
			a4 = src.X0[idx+7] < int64(val[0]) || (src.X0[idx+7] == int64(val[0]) && src.X1[idx+7] <= val[1])
			b += util.Bool2byte(a1)<<4 + util.Bool2byte(a2)<<5 + util.Bool2byte(a3)<<6 + util.Bool2byte(a4)<<7
			res[i] = b & m
			cnt += int64(bits.OnesCount8(b))
			idx += 8
		}

		// tail
		if src.Len()&7 > 0 {
			for i, l := n*8, src.Len(); i < l; i++ {
				bit := byte(1) << (i & 7)
				if (mask[n] & bit) == 0 {
					continue
				}
				if src.X0[i] > int64(val[0]) || (src.X0[i] == int64(val[0]) && src.X1[i] > val[1]) {
					continue
				}
				res[n] |= bit
				cnt++
			}
		}

	} else {
		for i := range n {
			a1 := src.X0[idx] < int64(val[0]) || (src.X0[idx] == int64(val[0]) && src.X1[idx] <= val[1])
			a2 := src.X0[idx+1] < int64(val[0]) || (src.X0[idx+1] == int64(val[0]) && src.X1[idx+1] <= val[1])
			a3 := src.X0[idx+2] < int64(val[0]) || (src.X0[idx+2] == int64(val[0]) && src.X1[idx+2] <= val[1])
			a4 := src.X0[idx+3] < int64(val[0]) || (src.X0[idx+3] == int64(val[0]) && src.X1[idx+3] <= val[1])
			// note: bitset bytes store bits inverted for efficient index algo
			b := util.Bool2byte(a1) + util.Bool2byte(a2)<<1 + util.Bool2byte(a3)<<2 + util.Bool2byte(a4)<<3
			a1 = src.X0[idx+4] < int64(val[0]) || (src.X0[idx+4] == int64(val[0]) && src.X1[idx+4] <= val[1])
			a2 = src.X0[idx+5] < int64(val[0]) || (src.X0[idx+5] == int64(val[0]) && src.X1[idx+5] <= val[1])
			a3 = src.X0[idx+6] < int64(val[0]) || (src.X0[idx+6] == int64(val[0]) && src.X1[idx+6] <= val[1])
			a4 = src.X0[idx+7] < int64(val[0]) || (src.X0[idx+7] == int64(val[0]) && src.X1[idx+7] <= val[1])
			b += util.Bool2byte(a1)<<4 + util.Bool2byte(a2)<<5 + util.Bool2byte(a3)<<6 + util.Bool2byte(a4)<<7
			res[i] = b
			cnt += int64(bits.OnesCount8(b))
			idx += 8
		}

		// tail
		if src.Len()&7 > 0 {
			for i, l := n*8, src.Len(); i < l; i++ {
				if src.X0[i] < int64(val[0]) || (src.X0[i] == int64(val[0]) && src.X1[i] <= val[1]) {
					res[n] |= 1 << (i & 7)
					cnt++
				}
			}
		}
	}
	return cnt
}

func cmp_i128_gt(src num.Int128Stride, val num.Int128, res, mask []byte) int64 {
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
			a1 := src.X0[idx] > int64(val[0]) || (src.X0[idx] == int64(val[0]) && src.X1[idx] > val[1])
			a2 := src.X0[idx+1] > int64(val[0]) || (src.X0[idx+1] == int64(val[0]) && src.X1[idx+1] > val[1])
			a3 := src.X0[idx+2] > int64(val[0]) || (src.X0[idx+2] == int64(val[0]) && src.X1[idx+2] > val[1])
			a4 := src.X0[idx+3] > int64(val[0]) || (src.X0[idx+3] == int64(val[0]) && src.X1[idx+3] > val[1])
			// note: bitset bytes store bits inverted for efficient index algo
			b := util.Bool2byte(a1) + util.Bool2byte(a2)<<1 + util.Bool2byte(a3)<<2 + util.Bool2byte(a4)<<3
			a1 = src.X0[idx+4] > int64(val[0]) || (src.X0[idx+4] == int64(val[0]) && src.X1[idx+4] > val[1])
			a2 = src.X0[idx+5] > int64(val[0]) || (src.X0[idx+5] == int64(val[0]) && src.X1[idx+5] > val[1])
			a3 = src.X0[idx+6] > int64(val[0]) || (src.X0[idx+6] == int64(val[0]) && src.X1[idx+6] > val[1])
			a4 = src.X0[idx+7] > int64(val[0]) || (src.X0[idx+7] == int64(val[0]) && src.X1[idx+7] > val[1])
			b += util.Bool2byte(a1)<<4 + util.Bool2byte(a2)<<5 + util.Bool2byte(a3)<<6 + util.Bool2byte(a4)<<7
			res[i] = b & m
			cnt += int64(bits.OnesCount8(b))
			idx += 8
		}

		// tail
		if src.Len()&7 > 0 {
			for i, l := n*8, src.Len(); i < l; i++ {
				bit := byte(1) << (i & 7)
				if (mask[n] & bit) == 0 {
					continue
				}
				if src.X0[i] < int64(val[0]) || (src.X0[i] == int64(val[0]) && src.X1[i] <= val[1]) {
					continue
				}
				res[n] |= bit
				cnt++
			}
		}

	} else {
		for i := range n {
			a1 := src.X0[idx] > int64(val[0]) || (src.X0[idx] == int64(val[0]) && src.X1[idx] > val[1])
			a2 := src.X0[idx+1] > int64(val[0]) || (src.X0[idx+1] == int64(val[0]) && src.X1[idx+1] > val[1])
			a3 := src.X0[idx+2] > int64(val[0]) || (src.X0[idx+2] == int64(val[0]) && src.X1[idx+2] > val[1])
			a4 := src.X0[idx+3] > int64(val[0]) || (src.X0[idx+3] == int64(val[0]) && src.X1[idx+3] > val[1])
			// note: bitset bytes store bits inverted for efficient index algo
			b := util.Bool2byte(a1) + util.Bool2byte(a2)<<1 + util.Bool2byte(a3)<<2 + util.Bool2byte(a4)<<3
			a1 = src.X0[idx+4] > int64(val[0]) || (src.X0[idx+4] == int64(val[0]) && src.X1[idx+4] > val[1])
			a2 = src.X0[idx+5] > int64(val[0]) || (src.X0[idx+5] == int64(val[0]) && src.X1[idx+5] > val[1])
			a3 = src.X0[idx+6] > int64(val[0]) || (src.X0[idx+6] == int64(val[0]) && src.X1[idx+6] > val[1])
			a4 = src.X0[idx+7] > int64(val[0]) || (src.X0[idx+7] == int64(val[0]) && src.X1[idx+7] > val[1])
			b += util.Bool2byte(a1)<<4 + util.Bool2byte(a2)<<5 + util.Bool2byte(a3)<<6 + util.Bool2byte(a4)<<7
			res[i] = b
			cnt += int64(bits.OnesCount8(b))
			idx += 8
		}

		// tail
		if src.Len()&7 > 0 {
			for i, l := n*8, src.Len(); i < l; i++ {
				if src.X0[i] > int64(val[0]) || (src.X0[i] == int64(val[0]) && src.X1[i] > val[1]) {
					res[n] |= 1 << (i & 7)
					cnt++
				}
			}
		}
	}
	return cnt
}

func cmp_i128_ge(src num.Int128Stride, val num.Int128, res, mask []byte) int64 {
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
			a1 := src.X0[idx] > int64(val[0]) || (src.X0[idx] == int64(val[0]) && src.X1[idx] >= val[1])
			a2 := src.X0[idx+1] > int64(val[0]) || (src.X0[idx+1] == int64(val[0]) && src.X1[idx+1] >= val[1])
			a3 := src.X0[idx+2] > int64(val[0]) || (src.X0[idx+2] == int64(val[0]) && src.X1[idx+2] >= val[1])
			a4 := src.X0[idx+3] > int64(val[0]) || (src.X0[idx+3] == int64(val[0]) && src.X1[idx+3] >= val[1])
			// note: bitset bytes store bits inverted for efficient index algo
			b := util.Bool2byte(a1) + util.Bool2byte(a2)<<1 + util.Bool2byte(a3)<<2 + util.Bool2byte(a4)<<3
			a1 = src.X0[idx+4] > int64(val[0]) || (src.X0[idx+4] == int64(val[0]) && src.X1[idx+4] >= val[1])
			a2 = src.X0[idx+5] > int64(val[0]) || (src.X0[idx+5] == int64(val[0]) && src.X1[idx+5] >= val[1])
			a3 = src.X0[idx+6] > int64(val[0]) || (src.X0[idx+6] == int64(val[0]) && src.X1[idx+6] >= val[1])
			a4 = src.X0[idx+7] > int64(val[0]) || (src.X0[idx+7] == int64(val[0]) && src.X1[idx+7] >= val[1])
			b += util.Bool2byte(a1)<<4 + util.Bool2byte(a2)<<5 + util.Bool2byte(a3)<<6 + util.Bool2byte(a4)<<7
			res[i] = b & m
			cnt += int64(bits.OnesCount8(b))
			idx += 8
		}

		// tail
		if src.Len()&7 > 0 {
			for i, l := n*8, src.Len(); i < l; i++ {
				bit := byte(1) << (i & 7)
				if (mask[n] & bit) == 0 {
					continue
				}
				if src.X0[i] < int64(val[0]) || (src.X0[i] == int64(val[0]) && src.X1[i] < val[1]) {
					continue
				}
				res[n] |= bit
				cnt++
			}
		}

	} else {
		for i := range n {
			a1 := src.X0[idx] > int64(val[0]) || (src.X0[idx] == int64(val[0]) && src.X1[idx] >= val[1])
			a2 := src.X0[idx+1] > int64(val[0]) || (src.X0[idx+1] == int64(val[0]) && src.X1[idx+1] >= val[1])
			a3 := src.X0[idx+2] > int64(val[0]) || (src.X0[idx+2] == int64(val[0]) && src.X1[idx+2] >= val[1])
			a4 := src.X0[idx+3] > int64(val[0]) || (src.X0[idx+3] == int64(val[0]) && src.X1[idx+3] >= val[1])
			// note: bitset bytes store bits inverted for efficient index algo
			b := util.Bool2byte(a1) + util.Bool2byte(a2)<<1 + util.Bool2byte(a3)<<2 + util.Bool2byte(a4)<<3
			a1 = src.X0[idx+4] > int64(val[0]) || (src.X0[idx+4] == int64(val[0]) && src.X1[idx+4] >= val[1])
			a2 = src.X0[idx+5] > int64(val[0]) || (src.X0[idx+5] == int64(val[0]) && src.X1[idx+5] >= val[1])
			a3 = src.X0[idx+6] > int64(val[0]) || (src.X0[idx+6] == int64(val[0]) && src.X1[idx+6] >= val[1])
			a4 = src.X0[idx+7] > int64(val[0]) || (src.X0[idx+7] == int64(val[0]) && src.X1[idx+7] >= val[1])
			b += util.Bool2byte(a1)<<4 + util.Bool2byte(a2)<<5 + util.Bool2byte(a3)<<6 + util.Bool2byte(a4)<<7
			res[i] = b
			cnt += int64(bits.OnesCount8(b))
			idx += 8
		}

		// tail
		if src.Len()&7 > 0 {
			for i, l := n*8, src.Len(); i < l; i++ {
				if src.X0[i] > int64(val[0]) || (src.X0[i] == int64(val[0]) && src.X1[i] >= val[1]) {
					res[n] |= 1 << (i & 7)
					cnt++
				}
			}
		}
	}
	return cnt
}

func cmp_i128_bw(src num.Int128Stride, a, b num.Int128, bits, mask []byte) int64 {
	var cnt int64
	if mask != nil {
		for i := range src.X0 {
			bit := byte(1) << (i & 7)
			if (mask[i>>3] & bit) == 0 {
				continue
			}
			v := num.Int128{uint64(src.X0[i]), src.X1[i]}
			if a.Le(v) && b.Ge(v) {
				bits[i>>3] |= bit
				cnt++
			}
		}
	} else {
		for i := range src.X0 {
			v := num.Int128{uint64(src.X0[i]), src.X1[i]}
			if a.Le(v) && b.Ge(v) {
				bits[i>>3] |= byte(1) << (i & 7)
				cnt++
			}
		}
	}
	return cnt
}
