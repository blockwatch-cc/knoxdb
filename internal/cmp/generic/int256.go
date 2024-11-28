// Copyright (c) 2023 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package generic

import "blockwatch.cc/knoxdb/pkg/num"

func MatchInt256Equal(src num.Int256Stride, val num.Int256, bits, mask []byte) int64 {
	var cnt int64
	if mask != nil {
		for i := range src.X0 {
			bit := byte(0x1) << uint(i&0x7)
			if (mask[i>>3] & bit) == 0 {
				continue
			}
			if src.Elem(i) != val {
				continue
			}
			bits[i>>3] |= bit
			cnt++
		}
	} else {
		for i := range src.X0 {
			if src.Elem(i) != val {
				continue
			}
			bits[i>>3] |= byte(0x1) << uint(i&0x7)
			cnt++
		}
	}
	return cnt
}

func MatchInt256NotEqual(src num.Int256Stride, val num.Int256, bits, mask []byte) int64 {
	var cnt int64
	if mask != nil {
		for i := range src.X0 {
			bit := byte(0x1) << uint(i&0x7)
			if (mask[i>>3] & bit) == 0 {
				continue
			}
			if src.Elem(i) == val {
				continue
			}
			bits[i>>3] |= bit
			cnt++
		}
	} else {
		for i := range src.X0 {
			if src.Elem(i) == val {
				continue
			}
			bits[i>>3] |= byte(0x1) << uint(i&0x7)
			cnt++
		}
	}
	return cnt
}

func MatchInt256Less(src num.Int256Stride, val num.Int256, bits, mask []byte) int64 {
	var cnt int64
	if mask != nil {
		for i := range src.X0 {
			bit := byte(0x1) << uint(i&0x7)
			if (mask[i>>3] & bit) == 0 {
				continue
			}
			if src.Elem(i).Gte(val) {
				continue
			}
			bits[i>>3] |= bit
			cnt++
		}
	} else {
		for i := range src.X0 {
			if src.Elem(i).Gte(val) {
				continue
			}
			bits[i>>3] |= byte(0x1) << uint(i&0x7)
			cnt++
		}
	}
	return cnt
}

func MatchInt256LessEqual(src num.Int256Stride, val num.Int256, bits, mask []byte) int64 {
	var cnt int64
	if mask != nil {
		for i := range src.X0 {
			bit := byte(0x1) << uint(i&0x7)
			if (mask[i>>3] & bit) == 0 {
				continue
			}
			if src.Elem(i).Gt(val) {
				continue
			}
			bits[i>>3] |= bit
			cnt++
		}
	} else {
		for i := range src.X0 {
			if src.Elem(i).Gt(val) {
				continue
			}
			bits[i>>3] |= byte(0x1) << uint(i&0x7)
			cnt++
		}
	}
	return cnt
}

func MatchInt256Greater(src num.Int256Stride, val num.Int256, bits, mask []byte) int64 {
	var cnt int64
	if mask != nil {
		for i := range src.X0 {
			bit := byte(0x1) << uint(i&0x7)
			if (mask[i>>3] & bit) == 0 {
				continue
			}
			if src.Elem(i).Lte(val) {
				continue
			}
			bits[i>>3] |= bit
			cnt++
		}
	} else {
		for i := range src.X0 {
			if src.Elem(i).Lte(val) {
				continue
			}
			bits[i>>3] |= byte(0x1) << uint(i&0x7)
			cnt++
		}
	}
	return cnt
}

func MatchInt256GreaterEqual(src num.Int256Stride, val num.Int256, bits, mask []byte) int64 {
	var cnt int64
	if mask != nil {
		for i := range src.X0 {
			bit := byte(0x1) << uint(i&0x7)
			if (mask[i>>3] & bit) == 0 {
				continue
			}
			if src.Elem(i).Lt(val) {
				continue
			}
			bits[i>>3] |= bit
			cnt++
		}
	} else {
		for i := range src.X0 {
			if src.Elem(i).Lt(val) {
				continue
			}
			bits[i>>3] |= byte(0x1) << uint(i&0x7)
			cnt++
		}
	}
	return cnt
}

func MatchInt256Between(src num.Int256Stride, a, b num.Int256, bits, mask []byte) int64 {
	diff := b.Sub(a).Add64(1).Uint256()
	var cnt int64
	if mask != nil {
		for i := range src.X0 {
			bit := byte(0x1) << uint(i&0x7)
			if (mask[i>>3] & bit) == 0 {
				continue
			}
			if src.Elem(i).Sub(a).Uint256().Gte(diff) {
				continue
			}
			bits[i>>3] |= bit
			cnt++
		}
	} else {
		for i := range src.X0 {
			if src.Elem(i).Sub(a).Uint256().Gte(diff) {
				continue
			}
			bits[i>>3] |= byte(0x1) << uint(i&0x7)
			cnt++
		}
	}
	return cnt
}
