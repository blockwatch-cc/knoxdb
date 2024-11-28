// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package vec

func matchInt256EqualGeneric(src Int256LLSlice, val Int256, bits, mask []byte) int64 {
	var cnt int64
	if mask != nil {
		for i, _ := range src.X0 {
			bit := bitmask(i)
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
		for i, _ := range src.X0 {
			if src.Elem(i) != val {
				continue
			}
			bits[i>>3] |= bitmask(i)
			cnt++
		}
	}
	return cnt
}

func matchInt256NotEqualGeneric(src Int256LLSlice, val Int256, bits, mask []byte) int64 {
	var cnt int64
	if mask != nil {
		for i, _ := range src.X0 {
			bit := bitmask(i)
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
		for i, _ := range src.X0 {
			if src.Elem(i) == val {
				continue
			}
			bits[i>>3] |= bitmask(i)
			cnt++
		}
	}
	return cnt
}

func matchInt256LessThanGeneric(src Int256LLSlice, val Int256, bits, mask []byte) int64 {
	var cnt int64
	if mask != nil {
		for i, _ := range src.X0 {
			bit := bitmask(i)
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
		for i, _ := range src.X0 {
			if src.Elem(i).Gte(val) {
				continue
			}
			bits[i>>3] |= bitmask(i)
			cnt++
		}
	}
	return cnt
}

func matchInt256LessThanEqualGeneric(src Int256LLSlice, val Int256, bits, mask []byte) int64 {
	var cnt int64
	if mask != nil {
		for i, _ := range src.X0 {
			bit := bitmask(i)
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
		for i, _ := range src.X0 {
			if src.Elem(i).Gt(val) {
				continue
			}
			bits[i>>3] |= bitmask(i)
			cnt++
		}
	}
	return cnt
}

func matchInt256GreaterThanGeneric(src Int256LLSlice, val Int256, bits, mask []byte) int64 {
	var cnt int64
	if mask != nil {
		for i, _ := range src.X0 {
			bit := bitmask(i)
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
		for i, _ := range src.X0 {
			if src.Elem(i).Lte(val) {
				continue
			}
			bits[i>>3] |= bitmask(i)
			cnt++
		}
	}
	return cnt
}

func matchInt256GreaterThanEqualGeneric(src Int256LLSlice, val Int256, bits, mask []byte) int64 {
	var cnt int64
	if mask != nil {
		for i, _ := range src.X0 {
			bit := bitmask(i)
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
		for i, _ := range src.X0 {
			if src.Elem(i).Lt(val) {
				continue
			}
			bits[i>>3] |= bitmask(i)
			cnt++
		}
	}
	return cnt
}

func matchInt256BetweenGeneric(src Int256LLSlice, a, b Int256, bits, mask []byte) int64 {
	diff := b.Sub(a).Add64(1).Uint256()
	var cnt int64
	if mask != nil {
		for i, _ := range src.X0 {
			bit := bitmask(i)
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
		for i, _ := range src.X0 {
			if src.Elem(i).Sub(a).Uint256().Gte(diff) {
				continue
			}
			bits[i>>3] |= bitmask(i)
			cnt++
		}
	}
	return cnt
}
