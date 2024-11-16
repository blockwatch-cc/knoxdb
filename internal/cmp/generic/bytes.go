// Copyright (c) 2023 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package generic

import (
	"bytes"
)

func MatchBytesEqual(src [][]byte, val []byte, bits, mask []byte) int64 {
	var cnt int64
	if mask != nil {
		for i, v := range src {
			bit := byte(0x1) << uint(i&0x7)
			if (mask[i>>3] & bit) == 0 {
				continue
			}
			if !bytes.Equal(v, val) {
				continue
			}
			bits[i>>3] |= bit
			cnt++
		}
	} else {
		for i, v := range src {
			if !bytes.Equal(v, val) {
				continue
			}
			bits[i>>3] |= byte(0x1) << uint(i&0x7)
			cnt++
		}
	}
	return cnt
}

func MatchBytesNotEqual(src [][]byte, val []byte, bits, mask []byte) int64 {
	var cnt int64
	if mask != nil {
		for i, v := range src {
			bit := byte(0x1) << uint(i&0x7)
			if (mask[i>>3] & bit) == 0 {
				continue
			}
			if bytes.Equal(v, val) {
				continue
			}
			bits[i>>3] |= bit
			cnt++
		}
	} else {
		for i, v := range src {
			if bytes.Equal(v, val) {
				continue
			}
			bits[i>>3] |= byte(0x1) << uint(i&0x7)
			cnt++
		}
	}
	return cnt
}

func MatchBytesLess(src [][]byte, val []byte, bits, mask []byte) int64 {
	var cnt int64
	if mask != nil {
		for i, v := range src {
			bit := byte(0x1) << uint(i&0x7)
			if (mask[i>>3] & bit) == 0 {
				continue
			}
			if bytes.Compare(v, val) >= 0 {
				continue
			}
			bits[i>>3] |= bit
			cnt++
		}
	} else {
		for i, v := range src {
			if bytes.Compare(v, val) >= 0 {
				continue
			}
			bits[i>>3] |= byte(0x1) << uint(i&0x7)
			cnt++
		}
	}
	return cnt
}

func MatchBytesLessEqual(src [][]byte, val []byte, bits, mask []byte) int64 {
	var cnt int64
	if mask != nil {
		for i, v := range src {
			bit := byte(0x1) << uint(i&0x7)
			if (mask[i>>3] & bit) == 0 {
				continue
			}
			if bytes.Compare(v, val) > 0 {
				continue
			}
			bits[i>>3] |= bit
			cnt++
		}
	} else {
		for i, v := range src {
			if bytes.Compare(v, val) > 0 {
				continue
			}
			bits[i>>3] |= byte(0x1) << uint(i&0x7)
			cnt++
		}
	}
	return cnt
}

func MatchBytesGreater(src [][]byte, val []byte, bits, mask []byte) int64 {
	var cnt int64
	if mask != nil {
		for i, v := range src {
			bit := byte(0x1) << uint(i&0x7)
			if (mask[i>>3] & bit) == 0 {
				continue
			}
			if bytes.Compare(v, val) <= 0 {
				continue
			}
			bits[i>>3] |= bit
			cnt++
		}
	} else {
		for i, v := range src {
			if bytes.Compare(v, val) <= 0 {
				continue
			}
			bits[i>>3] |= byte(0x1) << uint(i&0x7)
			cnt++
		}
	}
	return cnt
}

func MatchBytesGreaterEqual(src [][]byte, val []byte, bits, mask []byte) int64 {
	var cnt int64
	if mask != nil {
		for i, v := range src {
			bit := byte(0x1) << uint(i&0x7)
			if (mask[i>>3] & bit) == 0 {
				continue
			}
			if bytes.Compare(v, val) < 0 {
				continue
			}
			bits[i>>3] |= bit
			cnt++
		}
	} else {
		for i, v := range src {
			if bytes.Compare(v, val) < 0 {
				continue
			}
			bits[i>>3] |= byte(0x1) << uint(i&0x7)
			cnt++
		}
	}
	return cnt
}

func MatchBytesBetween(src [][]byte, a, b, bits, mask []byte) int64 {
	// short-cut for empty min
	if len(a) == 0 {
		if mask != nil {
			copy(bits, mask)
			return -1
		} else {
			bits[0] = 0xff
			for bp := 1; bp < len(bits); bp *= 2 {
				copy(bits[bp:], bits[:bp])
			}
			bits[len(bits)-1] &= bytemask(len(src))
			return int64(len(src))
		}
	}

	// make sure min/max are in correct order
	if d := bytes.Compare(a, b); d < 0 {
		b, a = a, b
	} else if d == 0 {
		return MatchBytesEqual(src, a, bits, mask)
	}

	var cnt int64
	if mask != nil {
		for i, v := range src {
			bit := byte(0x1) << uint(i&0x7)
			if (mask[i>>3] & bit) == 0 {
				continue
			}
			if bytes.Compare(v, a) < 0 {
				continue
			}
			if bytes.Compare(v, b) > 0 {
				continue
			}
			bits[i>>3] |= bit
			cnt++
		}
	} else {
		for i, v := range src {
			if bytes.Compare(v, a) < 0 {
				continue
			}
			if bytes.Compare(v, b) > 0 {
				continue
			}
			bits[i>>3] |= byte(0x1) << uint(i&0x7)
			cnt++
		}
	}
	return cnt
}
