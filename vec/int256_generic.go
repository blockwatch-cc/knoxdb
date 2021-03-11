// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package vec

func matchInt256EqualGeneric(src []Int256, val Int256, bits, mask []byte) int64 {
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

func matchInt256NotEqualGeneric(src []Int256, val Int256, bits, mask []byte) int64 {
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

func matchInt256LessThanGeneric(src []Int256, val Int256, bits, mask []byte) int64 {
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

func matchInt256LessThanEqualGeneric(src []Int256, val Int256, bits, mask []byte) int64 {
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

func matchInt256GreaterThanGeneric(src []Int256, val Int256, bits, mask []byte) int64 {
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

func matchInt256GreaterThanEqualGeneric(src []Int256, val Int256, bits, mask []byte) int64 {
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

func matchInt256BetweenGeneric(src []Int256, a, b Int256, bits, mask []byte) int64 {
	diff := b.Sub(a).Add64(1)
	var cnt int64
	if mask != nil {
		for i, v := range src {
			bit := bitmask(i)
			if (mask[i>>3] & bit) == 0 {
				continue
			}
			if v.Sub(a).Gte(diff) {
				continue
			}
			bits[i>>3] |= bit
			cnt++
		}
	} else {
		for i, v := range src {
			if v.Sub(a).Gte(diff) {
				continue
			}
			bits[i>>3] |= bitmask(i)
			cnt++
		}
	}
	return cnt
}
