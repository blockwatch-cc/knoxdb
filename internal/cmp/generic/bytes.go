// Copyright (c) 2023 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package generic

import (
	"bytes"
	"math/bits"

	"blockwatch.cc/knoxdb/pkg/util"
)

func MatchBytesEqual(src [][]byte, val []byte, res, mask []byte) int64 {
	var cnt int64
	n := len(src) / 8
	var idx int
	if mask != nil {
		for i := range n {
			m := mask[i]
			if m == 0 {
				idx += 8
				continue
			}
			a1 := bytes.Equal(src[idx], val)
			a2 := bytes.Equal(src[idx+1], val)
			a3 := bytes.Equal(src[idx+2], val)
			a4 := bytes.Equal(src[idx+3], val)
			// note: bitset bytes store bits inverted for efficient index algo
			b := util.Bool2byte(a1) + util.Bool2byte(a2)<<1 + util.Bool2byte(a3)<<2 + util.Bool2byte(a4)<<3
			a1 = bytes.Equal(src[idx+4], val)
			a2 = bytes.Equal(src[idx+5], val)
			a3 = bytes.Equal(src[idx+6], val)
			a4 = bytes.Equal(src[idx+7], val)
			b += util.Bool2byte(a1)<<4 + util.Bool2byte(a2)<<5 + util.Bool2byte(a3)<<6 + util.Bool2byte(a4)<<7
			res[i] = b & m
			cnt += int64(bits.OnesCount8(b))
			idx += 8
		}

		// tail
		if len(src)%8 > 0 {
			for i, v := range src[idx:] {
				bit := byte(0x1) << uint(i&0x7)
				if (mask[n] & bit) == 0 {
					continue
				}
				if !bytes.Equal(v, val) {
					continue
				}
				res[n] |= bit
				cnt++
			}
		}

	} else {
		for i := range n {
			a1 := bytes.Equal(src[idx], val)
			a2 := bytes.Equal(src[idx+1], val)
			a3 := bytes.Equal(src[idx+2], val)
			a4 := bytes.Equal(src[idx+3], val)
			// note: bitset bytes store bits inverted for efficient index algo
			b := util.Bool2byte(a1) + util.Bool2byte(a2)<<1 + util.Bool2byte(a3)<<2 + util.Bool2byte(a4)<<3
			a1 = bytes.Equal(src[idx+4], val)
			a2 = bytes.Equal(src[idx+5], val)
			a3 = bytes.Equal(src[idx+6], val)
			a4 = bytes.Equal(src[idx+7], val)
			b += util.Bool2byte(a1)<<4 + util.Bool2byte(a2)<<5 + util.Bool2byte(a3)<<6 + util.Bool2byte(a4)<<7
			res[i] = b
			cnt += int64(bits.OnesCount8(b))
			idx += 8
		}

		// tail
		if len(src)%8 > 0 {
			for i, v := range src[idx:] {
				if bytes.Equal(v, val) {
					res[n] |= 0x1 << i
					cnt++
				}
			}
		}
	}
	return cnt
}

func MatchBytesNotEqual(src [][]byte, val []byte, res, mask []byte) int64 {
	var cnt int64
	n := len(src) / 8
	var idx int
	if mask != nil {
		for i := range n {
			m := mask[i]
			if m == 0 {
				idx += 8
				continue
			}
			a1 := !bytes.Equal(src[idx], val)
			a2 := !bytes.Equal(src[idx+1], val)
			a3 := !bytes.Equal(src[idx+2], val)
			a4 := !bytes.Equal(src[idx+3], val)
			// note: bitset bytes store bits inverted for efficient index algo
			b := util.Bool2byte(a1) + util.Bool2byte(a2)<<1 + util.Bool2byte(a3)<<2 + util.Bool2byte(a4)<<3
			a1 = !bytes.Equal(src[idx+4], val)
			a2 = !bytes.Equal(src[idx+5], val)
			a3 = !bytes.Equal(src[idx+6], val)
			a4 = !bytes.Equal(src[idx+7], val)
			b += util.Bool2byte(a1)<<4 + util.Bool2byte(a2)<<5 + util.Bool2byte(a3)<<6 + util.Bool2byte(a4)<<7
			res[i] = b & m
			cnt += int64(bits.OnesCount8(b))
			idx += 8
		}

		// tail
		if len(src)%8 > 0 {
			for i, v := range src[idx:] {
				bit := byte(0x1) << uint(i&0x7)
				if (mask[n] & bit) == 0 {
					continue
				}
				if bytes.Equal(v, val) {
					continue
				}
				res[n] |= bit
				cnt++
			}
		}

	} else {
		for i := range n {
			a1 := !bytes.Equal(src[idx], val)
			a2 := !bytes.Equal(src[idx+1], val)
			a3 := !bytes.Equal(src[idx+2], val)
			a4 := !bytes.Equal(src[idx+3], val)
			// note: bitset bytes store bits inverted for efficient index algo
			b := util.Bool2byte(a1) + util.Bool2byte(a2)<<1 + util.Bool2byte(a3)<<2 + util.Bool2byte(a4)<<3
			a1 = !bytes.Equal(src[idx+4], val)
			a2 = !bytes.Equal(src[idx+5], val)
			a3 = !bytes.Equal(src[idx+6], val)
			a4 = !bytes.Equal(src[idx+7], val)
			b += util.Bool2byte(a1)<<4 + util.Bool2byte(a2)<<5 + util.Bool2byte(a3)<<6 + util.Bool2byte(a4)<<7
			res[i] = b
			cnt += int64(bits.OnesCount8(b))
			idx += 8
		}

		// tail
		if len(src)%8 > 0 {
			for i, v := range src[idx:] {
				if !bytes.Equal(v, val) {
					res[n] |= 0x1 << i
					cnt++
				}
			}
		}
	}
	return cnt
}

func MatchBytesLess(src [][]byte, val []byte, res, mask []byte) int64 {
	var cnt int64
	n := len(src) / 8
	var idx int
	if mask != nil {
		for i := range n {
			m := mask[i]
			if m == 0 {
				idx += 8
				continue
			}
			a1 := bytes.Compare(src[idx], val) < 0
			a2 := bytes.Compare(src[idx+1], val) < 0
			a3 := bytes.Compare(src[idx+2], val) < 0
			a4 := bytes.Compare(src[idx+3], val) < 0
			// note: bitset bytes store bits inverted for efficient index algo
			b := util.Bool2byte(a1) + util.Bool2byte(a2)<<1 + util.Bool2byte(a3)<<2 + util.Bool2byte(a4)<<3
			a1 = bytes.Compare(src[idx+4], val) < 0
			a2 = bytes.Compare(src[idx+5], val) < 0
			a3 = bytes.Compare(src[idx+6], val) < 0
			a4 = bytes.Compare(src[idx+7], val) < 0
			b += util.Bool2byte(a1)<<4 + util.Bool2byte(a2)<<5 + util.Bool2byte(a3)<<6 + util.Bool2byte(a4)<<7
			res[i] = b & m
			cnt += int64(bits.OnesCount8(b))
			idx += 8
		}

		// tail
		if len(src)%8 > 0 {
			for i, v := range src[idx:] {
				bit := byte(0x1) << uint(i&0x7)
				if (mask[n] & bit) == 0 {
					continue
				}
				if bytes.Compare(v, val) >= 0 {
					continue
				}
				res[n] |= bit
				cnt++
			}
		}

	} else {
		for i := range n {
			a1 := bytes.Compare(src[idx], val) < 0
			a2 := bytes.Compare(src[idx+1], val) < 0
			a3 := bytes.Compare(src[idx+2], val) < 0
			a4 := bytes.Compare(src[idx+3], val) < 0
			// note: bitset bytes store bits inverted for efficient index algo
			b := util.Bool2byte(a1) + util.Bool2byte(a2)<<1 + util.Bool2byte(a3)<<2 + util.Bool2byte(a4)<<3
			a1 = bytes.Compare(src[idx+4], val) < 0
			a2 = bytes.Compare(src[idx+5], val) < 0
			a3 = bytes.Compare(src[idx+6], val) < 0
			a4 = bytes.Compare(src[idx+7], val) < 0
			b += util.Bool2byte(a1)<<4 + util.Bool2byte(a2)<<5 + util.Bool2byte(a3)<<6 + util.Bool2byte(a4)<<7
			res[i] = b
			cnt += int64(bits.OnesCount8(b))
			idx += 8
		}

		// tail
		if len(src)%8 > 0 {
			for i, v := range src[idx:] {
				if bytes.Compare(v, val) < 0 {
					res[n] |= 0x1 << i
					cnt++
				}
			}
		}
	}
	return cnt
}

func MatchBytesLessEqual(src [][]byte, val []byte, res, mask []byte) int64 {
	var cnt int64
	n := len(src) / 8
	var idx int
	if mask != nil {
		for i := range n {
			m := mask[i]
			if m == 0 {
				idx += 8
				continue
			}
			a1 := bytes.Compare(src[idx], val) <= 0
			a2 := bytes.Compare(src[idx+1], val) <= 0
			a3 := bytes.Compare(src[idx+2], val) <= 0
			a4 := bytes.Compare(src[idx+3], val) <= 0
			// note: bitset bytes store bits inverted for efficient index algo
			b := util.Bool2byte(a1) + util.Bool2byte(a2)<<1 + util.Bool2byte(a3)<<2 + util.Bool2byte(a4)<<3
			a1 = bytes.Compare(src[idx+4], val) <= 0
			a2 = bytes.Compare(src[idx+5], val) <= 0
			a3 = bytes.Compare(src[idx+6], val) <= 0
			a4 = bytes.Compare(src[idx+7], val) <= 0
			b += util.Bool2byte(a1)<<4 + util.Bool2byte(a2)<<5 + util.Bool2byte(a3)<<6 + util.Bool2byte(a4)<<7
			res[i] = b & m
			cnt += int64(bits.OnesCount8(b))
			idx += 8
		}

		// tail
		if len(src)%8 > 0 {
			for i, v := range src[idx:] {
				bit := byte(0x1) << uint(i&0x7)
				if (mask[n] & bit) == 0 {
					continue
				}
				if bytes.Compare(v, val) > 0 {
					continue
				}
				res[n] |= bit
				cnt++
			}
		}

	} else {
		for i := range n {
			a1 := bytes.Compare(src[idx], val) <= 0
			a2 := bytes.Compare(src[idx+1], val) <= 0
			a3 := bytes.Compare(src[idx+2], val) <= 0
			a4 := bytes.Compare(src[idx+3], val) <= 0
			// note: bitset bytes store bits inverted for efficient index algo
			b := util.Bool2byte(a1) + util.Bool2byte(a2)<<1 + util.Bool2byte(a3)<<2 + util.Bool2byte(a4)<<3
			a1 = bytes.Compare(src[idx+4], val) <= 0
			a2 = bytes.Compare(src[idx+5], val) <= 0
			a3 = bytes.Compare(src[idx+6], val) <= 0
			a4 = bytes.Compare(src[idx+7], val) <= 0
			b += util.Bool2byte(a1)<<4 + util.Bool2byte(a2)<<5 + util.Bool2byte(a3)<<6 + util.Bool2byte(a4)<<7
			res[i] = b
			cnt += int64(bits.OnesCount8(b))
			idx += 8
		}

		// tail
		if len(src)%8 > 0 {
			for i, v := range src[idx:] {
				if bytes.Compare(v, val) <= 0 {
					res[n] |= 0x1 << i
					cnt++
				}
			}
		}
	}
	return cnt
}

func MatchBytesGreater(src [][]byte, val []byte, res, mask []byte) int64 {
	var cnt int64
	n := len(src) / 8
	var idx int
	if mask != nil {
		for i := range n {
			m := mask[i]
			if m == 0 {
				idx += 8
				continue
			}
			a1 := bytes.Compare(src[idx], val) > 0
			a2 := bytes.Compare(src[idx+1], val) > 0
			a3 := bytes.Compare(src[idx+2], val) > 0
			a4 := bytes.Compare(src[idx+3], val) > 0
			// note: bitset bytes store bits inverted for efficient index algo
			b := util.Bool2byte(a1) + util.Bool2byte(a2)<<1 + util.Bool2byte(a3)<<2 + util.Bool2byte(a4)<<3
			a1 = bytes.Compare(src[idx+4], val) > 0
			a2 = bytes.Compare(src[idx+5], val) > 0
			a3 = bytes.Compare(src[idx+6], val) > 0
			a4 = bytes.Compare(src[idx+7], val) > 0
			b += util.Bool2byte(a1)<<4 + util.Bool2byte(a2)<<5 + util.Bool2byte(a3)<<6 + util.Bool2byte(a4)<<7
			res[i] = b & m
			cnt += int64(bits.OnesCount8(b))
			idx += 8
		}

		// tail
		if len(src)%8 > 0 {
			for i, v := range src[idx:] {
				bit := byte(0x1) << uint(i&0x7)
				if (mask[n] & bit) == 0 {
					continue
				}
				if bytes.Compare(v, val) <= 0 {
					continue
				}
				res[n] |= bit
				cnt++
			}
		}

	} else {
		for i := range n {
			a1 := bytes.Compare(src[idx], val) > 0
			a2 := bytes.Compare(src[idx+1], val) > 0
			a3 := bytes.Compare(src[idx+2], val) > 0
			a4 := bytes.Compare(src[idx+3], val) > 0
			// note: bitset bytes store bits inverted for efficient index algo
			b := util.Bool2byte(a1) + util.Bool2byte(a2)<<1 + util.Bool2byte(a3)<<2 + util.Bool2byte(a4)<<3
			a1 = bytes.Compare(src[idx+4], val) > 0
			a2 = bytes.Compare(src[idx+5], val) > 0
			a3 = bytes.Compare(src[idx+6], val) > 0
			a4 = bytes.Compare(src[idx+7], val) > 0
			b += util.Bool2byte(a1)<<4 + util.Bool2byte(a2)<<5 + util.Bool2byte(a3)<<6 + util.Bool2byte(a4)<<7
			res[i] = b
			cnt += int64(bits.OnesCount8(b))
			idx += 8
		}

		// tail
		if len(src)%8 > 0 {
			for i, v := range src[idx:] {
				if bytes.Compare(v, val) > 0 {
					res[n] |= 0x1 << i
					cnt++
				}
			}
		}
	}
	return cnt
}

func MatchBytesGreaterEqual(src [][]byte, val []byte, res, mask []byte) int64 {
	var cnt int64
	n := len(src) / 8
	var idx int
	if mask != nil {
		for i := range n {
			m := mask[i]
			if m == 0 {
				idx += 8
				continue
			}
			a1 := bytes.Compare(src[idx], val) >= 0
			a2 := bytes.Compare(src[idx+1], val) >= 0
			a3 := bytes.Compare(src[idx+2], val) >= 0
			a4 := bytes.Compare(src[idx+3], val) >= 0
			// note: bitset bytes store bits inverted for efficient index algo
			b := util.Bool2byte(a1) + util.Bool2byte(a2)<<1 + util.Bool2byte(a3)<<2 + util.Bool2byte(a4)<<3
			a1 = bytes.Compare(src[idx+4], val) >= 0
			a2 = bytes.Compare(src[idx+5], val) >= 0
			a3 = bytes.Compare(src[idx+6], val) >= 0
			a4 = bytes.Compare(src[idx+7], val) >= 0
			b += util.Bool2byte(a1)<<4 + util.Bool2byte(a2)<<5 + util.Bool2byte(a3)<<6 + util.Bool2byte(a4)<<7
			res[i] = b & m
			cnt += int64(bits.OnesCount8(b))
			idx += 8
		}

		// tail
		if len(src)%8 > 0 {
			for i, v := range src[idx:] {
				bit := byte(0x1) << uint(i&0x7)
				if (mask[n] & bit) == 0 {
					continue
				}
				if bytes.Compare(v, val) < 0 {
					continue
				}
				res[n] |= bit
				cnt++
			}
		}

	} else {
		for i := range n {
			a1 := bytes.Compare(src[idx], val) >= 0
			a2 := bytes.Compare(src[idx+1], val) >= 0
			a3 := bytes.Compare(src[idx+2], val) >= 0
			a4 := bytes.Compare(src[idx+3], val) >= 0
			// note: bitset bytes store bits inverted for efficient index algo
			b := util.Bool2byte(a1) + util.Bool2byte(a2)<<1 + util.Bool2byte(a3)<<2 + util.Bool2byte(a4)<<3
			a1 = bytes.Compare(src[idx+4], val) >= 0
			a2 = bytes.Compare(src[idx+5], val) >= 0
			a3 = bytes.Compare(src[idx+6], val) >= 0
			a4 = bytes.Compare(src[idx+7], val) >= 0
			b += util.Bool2byte(a1)<<4 + util.Bool2byte(a2)<<5 + util.Bool2byte(a3)<<6 + util.Bool2byte(a4)<<7
			res[i] = b
			cnt += int64(bits.OnesCount8(b))
			idx += 8
		}

		// tail
		if len(src)%8 > 0 {
			for i, v := range src[idx:] {
				if bytes.Compare(v, val) >= 0 {
					res[n] |= 0x1 << i
					cnt++
				}
			}
		}
	}
	return cnt
}

func MatchBytesBetween(src [][]byte, a, b, res, mask []byte) int64 {
	// shortcut for equal range
	if bytes.Equal(a, b) {
		return MatchBytesEqual(src, a, res, mask)
	}

	var cnt int64
	n := len(src) / 8
	var idx int
	if mask != nil {
		for i := range n {
			m := mask[i]
			if m == 0 {
				idx += 8
				continue
			}
			a1 := bytes.Compare(src[idx], a) >= 0 && bytes.Compare(src[idx], b) <= 0
			a2 := bytes.Compare(src[idx+1], a) >= 0 && bytes.Compare(src[idx+1], b) <= 0
			a3 := bytes.Compare(src[idx+2], a) >= 0 && bytes.Compare(src[idx+2], b) <= 0
			a4 := bytes.Compare(src[idx+3], a) >= 0 && bytes.Compare(src[idx+3], b) <= 0
			// note: bitset bytes store bits inverted for efficient index algo
			x := util.Bool2byte(a1) + util.Bool2byte(a2)<<1 + util.Bool2byte(a3)<<2 + util.Bool2byte(a4)<<3
			a1 = bytes.Compare(src[idx+4], a) >= 0 && bytes.Compare(src[idx+4], b) <= 0
			a2 = bytes.Compare(src[idx+5], a) >= 0 && bytes.Compare(src[idx+5], b) <= 0
			a3 = bytes.Compare(src[idx+6], a) >= 0 && bytes.Compare(src[idx+6], b) <= 0
			a4 = bytes.Compare(src[idx+7], a) >= 0 && bytes.Compare(src[idx+7], b) <= 0
			x += util.Bool2byte(a1)<<4 + util.Bool2byte(a2)<<5 + util.Bool2byte(a3)<<6 + util.Bool2byte(a4)<<7
			res[i] = x & m
			cnt += int64(bits.OnesCount8(x))
			idx += 8
		}

		// tail
		if len(src)%8 > 0 {
			for i, v := range src[idx:] {
				bit := byte(0x1) << uint(i&0x7)
				if (mask[n] & bit) == 0 {
					continue
				}
				if bytes.Compare(v, a) < 0 {
					continue
				}
				if bytes.Compare(v, b) > 0 {
					continue
				}
				res[n] |= bit
				cnt++
			}
		}

	} else {
		for i := range n {
			a1 := bytes.Compare(src[idx], a) >= 0 && bytes.Compare(src[idx], b) <= 0
			a2 := bytes.Compare(src[idx+1], a) >= 0 && bytes.Compare(src[idx+1], b) <= 0
			a3 := bytes.Compare(src[idx+2], a) >= 0 && bytes.Compare(src[idx+2], b) <= 0
			a4 := bytes.Compare(src[idx+3], a) >= 0 && bytes.Compare(src[idx+3], b) <= 0
			// note: bitset bytes store bits inverted for efficient index algo
			x := util.Bool2byte(a1) + util.Bool2byte(a2)<<1 + util.Bool2byte(a3)<<2 + util.Bool2byte(a4)<<3
			a1 = bytes.Compare(src[idx+4], a) >= 0 && bytes.Compare(src[idx+4], b) <= 0
			a2 = bytes.Compare(src[idx+5], a) >= 0 && bytes.Compare(src[idx+5], b) <= 0
			a3 = bytes.Compare(src[idx+6], a) >= 0 && bytes.Compare(src[idx+6], b) <= 0
			a4 = bytes.Compare(src[idx+7], a) >= 0 && bytes.Compare(src[idx+7], b) <= 0
			x += util.Bool2byte(a1)<<4 + util.Bool2byte(a2)<<5 + util.Bool2byte(a3)<<6 + util.Bool2byte(a4)<<7
			res[i] = x
			cnt += int64(bits.OnesCount8(x))
			idx += 8
		}

		// tail
		if len(src)%8 > 0 {
			for i, v := range src[idx:] {
				bit := byte(0x1) << uint(i&0x7)
				if bytes.Compare(v, a) < 0 {
					continue
				}
				if bytes.Compare(v, b) > 0 {
					continue
				}
				res[n] |= bit
				cnt++
			}
		}
	}
	return cnt
}
