// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package vec

func matchInt128EqualGeneric(src []Int128, val Int128, bits []byte) int64 {
	var cnt int64
	for i, v := range src {
		if v == val {
			bits[i>>3] |= 0x1 << uint(7-i&0x7)
			cnt++
		}
	}
	return cnt
}

func matchInt128NotEqualGeneric(src []Int128, val Int128, bits []byte) int64 {
	var cnt int64
	for i, v := range src {
		if v != val {
			bits[i>>3] |= 0x1 << uint(7-i&0x7)
			cnt++
		}
	}
	return cnt
}

func matchInt128LessThanGeneric(src []Int128, val Int128, bits []byte) int64 {
	var cnt int64
	for i, v := range src {
		if v.Lt(val) {
			bits[i>>3] |= 0x1 << uint(7-i&0x7)
			cnt++
		}
	}
	return cnt
}

func matchInt128LessThanEqualGeneric(src []Int128, val Int128, bits []byte) int64 {
	var cnt int64
	for i, v := range src {
		if v.Lte(val) {
			bits[i>>3] |= 0x1 << uint(7-i&0x7)
			cnt++
		}
	}
	return cnt
}

func matchInt128GreaterThanGeneric(src []Int128, val Int128, bits []byte) int64 {
	var cnt int64
	for i, v := range src {
		if v.Gt(val) {
			bits[i>>3] |= 0x1 << uint(7-i&0x7)
			cnt++
		}
	}
	return cnt
}

func matchInt128GreaterThanEqualGeneric(src []Int128, val Int128, bits []byte) int64 {
	var cnt int64
	for i, v := range src {
		if v.Gte(val) {
			bits[i>>3] |= 0x1 << uint(7-i&0x7)
			cnt++
		}
	}
	return cnt
}

func matchInt128BetweenGeneric(src []Int128, a, b Int128, bits []byte) int64 {
	diff := b.Sub(a).Add64(1)
	var cnt int64
	for i, v := range src {
		if v.Sub(a).Lt(diff) {
			bits[i>>3] |= 0x1 << uint(7-i&0x7)
			cnt++
		}
	}
	return cnt
}
