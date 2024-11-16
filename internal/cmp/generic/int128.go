// Copyright (c) 2023 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package generic

import "blockwatch.cc/knoxdb/pkg/num"

func MatchInt128Equal(src num.Int128Stride, val num.Int128, bits, mask []byte) int64 {
	var cnt int64
	if mask != nil {
		for i := range src.X0 {
			bit := byte(0x1) << uint(i&0x7)
			if (mask[i>>3] & bit) == 0 {
				continue
			}
			if uint64(src.X0[i]) != val[0] || src.X1[i] != val[1] {
				continue
			}
			bits[i>>3] |= bit
			cnt++
		}
	} else {
		for i := range src.X0 {
			if uint64(src.X0[i]) != val[0] || src.X1[i] != val[1] {
				continue
			}
			bits[i>>3] |= byte(0x1) << uint(i&0x7)
			cnt++
		}
	}
	return cnt
}

func MatchInt128NotEqual(src num.Int128Stride, val num.Int128, bits, mask []byte) int64 {
	var cnt int64
	if mask != nil {
		for i := range src.X0 {
			bit := byte(0x1) << uint(i&0x7)
			if (mask[i>>3] & bit) == 0 {
				continue
			}
			if uint64(src.X0[i]) != val[0] || src.X1[i] != val[1] {
				bits[i>>3] |= bit
				cnt++
			}
		}
	} else {
		for i := range src.X0 {
			if uint64(src.X0[i]) != val[0] || src.X1[i] != val[1] {
				bits[i>>3] |= byte(0x1) << uint(i&0x7)
				cnt++
			}
		}
	}
	return cnt
}

func MatchInt128Less(src num.Int128Stride, val num.Int128, bits, mask []byte) int64 {
	var cnt int64
	if mask != nil {
		for i := range src.X0 {
			bit := byte(0x1) << uint(i&0x7)
			if (mask[i>>3] & bit) == 0 {
				continue
			}
			if val.Gt(num.Int128{uint64(src.X0[i]), src.X1[i]}) {
				bits[i>>3] |= bit
				cnt++
			}
		}
	} else {
		for i := range src.X0 {
			if val.Gt(num.Int128{uint64(src.X0[i]), src.X1[i]}) {
				bits[i>>3] |= byte(0x1) << uint(i&0x7)
				cnt++
			}
		}
	}
	return cnt
}

func MatchInt128LessEqual(src num.Int128Stride, val num.Int128, bits, mask []byte) int64 {
	var cnt int64
	if mask != nil {
		for i := range src.X0 {
			bit := byte(0x1) << uint(i&0x7)
			if (mask[i>>3] & bit) == 0 {
				continue
			}
			if val.Gte(num.Int128{uint64(src.X0[i]), src.X1[i]}) {
				bits[i>>3] |= bit
				cnt++
			}
		}
	} else {
		for i := range src.X0 {
			if val.Gte(num.Int128{uint64(src.X0[i]), src.X1[i]}) {
				bits[i>>3] |= byte(0x1) << uint(i&0x7)
				cnt++
			}
		}
	}
	return cnt
}

func MatchInt128Greater(src num.Int128Stride, val num.Int128, bits, mask []byte) int64 {
	var cnt int64
	if mask != nil {
		for i := range src.X0 {
			bit := byte(0x1) << uint(i&0x7)
			if (mask[i>>3] & bit) == 0 {
				continue
			}
			if val.Lt(num.Int128{uint64(src.X0[i]), src.X1[i]}) {
				bits[i>>3] |= bit
				cnt++
			}
		}
	} else {
		for i := range src.X0 {
			if val.Lt(num.Int128{uint64(src.X0[i]), src.X1[i]}) {
				bits[i>>3] |= byte(0x1) << uint(i&0x7)
				cnt++
			}
		}
	}
	return cnt
}

func MatchInt128GreaterEqual(src num.Int128Stride, val num.Int128, bits, mask []byte) int64 {
	var cnt int64
	if mask != nil {
		for i := range src.X0 {
			bit := byte(0x1) << uint(i&0x7)
			if (mask[i>>3] & bit) == 0 {
				continue
			}
			if val.Lte(num.Int128{uint64(src.X0[i]), src.X1[i]}) {
				bits[i>>3] |= bit
				cnt++
			}
		}
	} else {
		for i := range src.X0 {
			if val.Lte(num.Int128{uint64(src.X0[i]), src.X1[i]}) {
				bits[i>>3] |= byte(0x1) << uint(i&0x7)
				cnt++
			}
		}
	}
	return cnt
}

func MatchInt128Between(src num.Int128Stride, a, b num.Int128, bits, mask []byte) int64 {
	var cnt int64
	if mask != nil {
		for i := range src.X0 {
			bit := byte(0x1) << uint(i&0x7)
			if (mask[i>>3] & bit) == 0 {
				continue
			}
			v := num.Int128{uint64(src.X0[i]), src.X1[i]}
			if a.Lte(v) && b.Gte(v) {
				bits[i>>3] |= bit
				cnt++
			}
		}
	} else {
		for i := range src.X0 {
			v := num.Int128{uint64(src.X0[i]), src.X1[i]}
			if a.Lte(v) && b.Gte(v) {
				bits[i>>3] |= byte(0x1) << uint(i&0x7)
				cnt++
			}
		}
	}
	return cnt
}
