// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package vec

func matchInt256EqualGeneric(src []Int256, val Int256, bits []byte) int64 {
	var cnt int64
	for i, v := range src {
		if v == val {
			bits[i>>3] |= 0x1 << uint(7-i&0x7)
			cnt++
		}
	}
	return cnt
}

func matchInt256NotEqualGeneric(src []Int256, val Int256, bits []byte) int64 {
	var cnt int64
	for i, v := range src {
		if v != val {
			bits[i>>3] |= 0x1 << uint(7-i&0x7)
			cnt++
		}
	}
	return cnt
}

func matchInt256LessThanGeneric(src []Int256, val Int256, bits []byte) int64 {
	var cnt int64
	for i, v := range src {
		if v.Lt(val) {
			bits[i>>3] |= 0x1 << uint(7-i&0x7)
			cnt++
		}
	}
	return cnt
}

func matchInt256LessThanEqualGeneric(src []Int256, val Int256, bits []byte) int64 {
	var cnt int64
	for i, v := range src {
		if v.Lte(val) {
			bits[i>>3] |= 0x1 << uint(7-i&0x7)
			cnt++
		}
	}
	return cnt
}

func matchInt256GreaterThanGeneric(src []Int256, val Int256, bits []byte) int64 {
	var cnt int64
	for i, v := range src {
		if v.Gt(val) {
			bits[i>>3] |= 0x1 << uint(7-i&0x7)
			cnt++
		}
	}
	return cnt
}

func matchInt256GreaterThanEqualGeneric(src []Int256, val Int256, bits []byte) int64 {
	var cnt int64
	for i, v := range src {
		if v.Gte(val) {
			bits[i>>3] |= 0x1 << uint(7-i&0x7)
			cnt++
		}
	}
	return cnt
}

func matchInt256BetweenGeneric(src []Int256, a, b Int256, bits []byte) int64 {
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
