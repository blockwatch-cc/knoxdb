// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package vec

func matchInt128EqualGeneric(src []Int128, val Int128, bits, mask []byte) int64 {
	var cnt int64
	if mask != nil {
		for i, v := range src {
			bit := bitmask(i)
			if (mask[i>>3] & bit) == 0 {
				continue
			}
			if v != val {
				continue
			}
			bits[i>>3] |= bit
			cnt++
		}
	} else {
		for i, v := range src {
			if v != val {
				continue
			}
			bits[i>>3] |= bitmask(i)
			cnt++
		}
	}
	return cnt
}

func matchInt128NotEqualGeneric(src []Int128, val Int128, bits, mask []byte) int64 {
	var cnt int64
	if mask != nil {
		for i, v := range src {
			bit := bitmask(i)
			if (mask[i>>3] & bit) == 0 {
				continue
			}
			if v == val {
				continue
			}
			bits[i>>3] |= bit
			cnt++
		}
	} else {
		for i, v := range src {
			if v == val {
				continue
			}
			bits[i>>3] |= bitmask(i)
			cnt++
		}
	}
	return cnt
}

func matchInt128LessThanGeneric(src []Int128, val Int128, bits, mask []byte) int64 {
	var cnt int64
	if mask != nil {
		for i, v := range src {
			bit := bitmask(i)
			if (mask[i>>3] & bit) == 0 {
				continue
			}
			if v.Gte(val) {
				continue
			}
			bits[i>>3] |= bit
			cnt++
		}
	} else {
		for i, v := range src {
			if v.Gte(val) {
				continue
			}
			bits[i>>3] |= bitmask(i)
			cnt++
		}
	}
	return cnt
}

func matchInt128LessThanEqualGeneric(src []Int128, val Int128, bits, mask []byte) int64 {
	var cnt int64
	if mask != nil {
		for i, v := range src {
			bit := bitmask(i)
			if (mask[i>>3] & bit) == 0 {
				continue
			}
			if v.Gt(val) {
				continue
			}
			bits[i>>3] |= bit
			cnt++
		}
	} else {
		for i, v := range src {
			if v.Gt(val) {
				continue
			}
			bits[i>>3] |= bitmask(i)
			cnt++
		}
	}
	return cnt
}

func matchInt128GreaterThanGeneric(src []Int128, val Int128, bits, mask []byte) int64 {
	var cnt int64
	if mask != nil {
		for i, v := range src {
			bit := bitmask(i)
			if (mask[i>>3] & bit) == 0 {
				continue
			}
			if v.Lte(val) {
				continue
			}
			bits[i>>3] |= bit
			cnt++
		}
	} else {
		for i, v := range src {
			if v.Lte(val) {
				continue
			}
			bits[i>>3] |= bitmask(i)
			cnt++
		}
	}
	return cnt
}

func matchInt128GreaterThanEqualGeneric(src []Int128, val Int128, bits, mask []byte) int64 {
	var cnt int64
	if mask != nil {
		for i, v := range src {
			bit := bitmask(i)
			if (mask[i>>3] & bit) == 0 {
				continue
			}
			if v.Lt(val) {
				continue
			}
			bits[i>>3] |= bit
			cnt++
		}
	} else {
		for i, v := range src {
			if v.Lt(val) {
				continue
			}
			bits[i>>3] |= bitmask(i)
			cnt++
		}
	}
	return cnt
}

func matchInt128BetweenGeneric(src []Int128, a, b Int128, bits, mask []byte) int64 {
	diff := b.Sub(a).Add64(1).Uint128()
	var cnt int64
	if mask != nil {
		for i, v := range src {
			bit := bitmask(i)
			if (mask[i>>3] & bit) == 0 {
				continue
			}
			if v.Sub(a).Uint128().Gte(diff) {
				continue
			}
			bits[i>>3] |= bit
			cnt++
		}
	} else {
		for i, v := range src {
			if v.Sub(a).Uint128().Gte(diff) {
				continue
			}
			bits[i>>3] |= bitmask(i)
			cnt++
		}
	}
	return cnt
}
