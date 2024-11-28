// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package vec

func matchInt128EqualGeneric(src Int128LLSlice, val Int128, bits, mask []byte) int64 {
	var cnt int64
	if mask != nil {
		for i, _ := range src.X0 {
			bit := bitmask(i)
			if (mask[i>>3] & bit) == 0 {
				continue
			}
			if uint64(src.X0[i]) != val[0] || src.X1[i] != val[1] {
				continue
			}
			bits[i>>3] |= bitmask(i)
			cnt++
		}
	} else {
		for i, _ := range src.X0 {
			if uint64(src.X0[i]) != val[0] || src.X1[i] != val[1] {
				continue
			}
			bits[i>>3] |= bitmask(i)
			cnt++
		}
	}
	return cnt
}

func matchInt128NotEqualGeneric(src Int128LLSlice, val Int128, bits, mask []byte) int64 {
	var cnt int64
	if mask != nil {
		for i, _ := range src.X0 {
			bit := bitmask(i)
			if (mask[i>>3] & bit) == 0 {
				continue
			}
			if uint64(src.X0[i]) != val[0] || src.X1[i] != val[1] {
				bits[i>>3] |= bitmask(i)
				cnt++
			}
		}
	} else {
		for i, _ := range src.X0 {
			if uint64(src.X0[i]) != val[0] || src.X1[i] != val[1] {
				bits[i>>3] |= bitmask(i)
				cnt++
			}
		}
	}
	return cnt
}

func matchInt128LessThanGeneric(src Int128LLSlice, val Int128, bits, mask []byte) int64 {
	var cnt int64
	if mask != nil {
		for i, _ := range src.X0 {
			bit := bitmask(i)
			if (mask[i>>3] & bit) == 0 {
				continue
			}
			if val.Gt(Int128{uint64(src.X0[i]), src.X1[i]}) {
				bits[i>>3] |= bitmask(i)
				cnt++
			}
		}
	} else {
		for i, _ := range src.X0 {
			if val.Gt(Int128{uint64(src.X0[i]), src.X1[i]}) {
				bits[i>>3] |= bitmask(i)
				cnt++
			}
		}
	}
	return cnt
}

func matchInt128LessThanEqualGeneric(src Int128LLSlice, val Int128, bits, mask []byte) int64 {
	var cnt int64
	if mask != nil {
		for i, _ := range src.X0 {
			bit := bitmask(i)
			if (mask[i>>3] & bit) == 0 {
				continue
			}
			if val.Gte(Int128{uint64(src.X0[i]), src.X1[i]}) {
				bits[i>>3] |= bitmask(i)
				cnt++
			}
		}
	} else {
		for i, _ := range src.X0 {
			if val.Gte(Int128{uint64(src.X0[i]), src.X1[i]}) {
				bits[i>>3] |= bitmask(i)
				cnt++
			}
		}
	}
	return cnt
}

func matchInt128GreaterThanGeneric(src Int128LLSlice, val Int128, bits, mask []byte) int64 {
	var cnt int64
	if mask != nil {
		for i, _ := range src.X0 {
			bit := bitmask(i)
			if (mask[i>>3] & bit) == 0 {
				continue
			}
			if val.Lt(Int128{uint64(src.X0[i]), src.X1[i]}) {
				bits[i>>3] |= bitmask(i)
				cnt++
			}
		}
	} else {
		for i, _ := range src.X0 {
			if val.Lt(Int128{uint64(src.X0[i]), src.X1[i]}) {
				bits[i>>3] |= bitmask(i)
				cnt++
			}
		}
	}
	return cnt
}

func matchInt128GreaterThanEqualGeneric(src Int128LLSlice, val Int128, bits, mask []byte) int64 {
	var cnt int64
	if mask != nil {
		for i, _ := range src.X0 {
			bit := bitmask(i)
			if (mask[i>>3] & bit) == 0 {
				continue
			}
			if val.Lte(Int128{uint64(src.X0[i]), src.X1[i]}) {
				bits[i>>3] |= bitmask(i)
				cnt++
			}
		}
	} else {
		for i, _ := range src.X0 {
			if val.Lte(Int128{uint64(src.X0[i]), src.X1[i]}) {
				bits[i>>3] |= bitmask(i)
				cnt++
			}
		}
	}
	return cnt
}

func matchInt128BetweenGeneric(src Int128LLSlice, a, b Int128, bits, mask []byte) int64 {
	var cnt int64
	if mask != nil {
		for i, _ := range src.X0 {
			bit := bitmask(i)
			if (mask[i>>3] & bit) == 0 {
				continue
			}
			v := Int128{uint64(src.X0[i]), src.X1[i]}
			if a.Lte(v) && b.Gte(v) {
				bits[i>>3] |= bitmask(i)
				cnt++
			}
		}
	} else {
		for i, _ := range src.X0 {
			v := Int128{uint64(src.X0[i]), src.X1[i]}
			if a.Lte(v) && b.Gte(v) {
				bits[i>>3] |= bitmask(i)
				cnt++
			}
		}
	}
	return cnt
}
