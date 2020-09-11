// Copyright (c) 2020 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

package vec

func matchUint16EqualGeneric(src []uint16, val uint16, bits []byte) int64 {
	var cnt int64
	for i, v := range src {
		if v == val {
			bits[i>>3] |= 0x1 << uint(7-i&0x7)
			cnt++
		}
	}
	return cnt
}

func matchUint16NotEqualGeneric(src []uint16, val uint16, bits []byte) int64 {
	var cnt int64
	for i, v := range src {
		if v != val {
			bits[i>>3] |= 0x1 << uint(7-i&0x7)
			cnt++
		}
	}
	return cnt
}

func matchUint16LessThanGeneric(src []uint16, val uint16, bits []byte) int64 {
	var cnt int64
	for i, v := range src {
		if v < val {
			bits[i>>3] |= 0x1 << uint(7-i&0x7)
			cnt++
		}
	}
	return cnt
}

func matchUint16LessThanEqualGeneric(src []uint16, val uint16, bits []byte) int64 {
	var cnt int64
	for i, v := range src {
		if v <= val {
			bits[i>>3] |= 0x1 << uint(7-i&0x7)
			cnt++
		}
	}
	return cnt
}

func matchUint16GreaterThanGeneric(src []uint16, val uint16, bits []byte) int64 {
	var cnt int64
	for i, v := range src {
		if v > val {
			bits[i>>3] |= 0x1 << uint(7-i&0x7)
			cnt++
		}
	}
	return cnt
}

func matchUint16GreaterThanEqualGeneric(src []uint16, val uint16, bits []byte) int64 {
	var cnt int64
	for i, v := range src {
		if v >= val {
			bits[i>>3] |= 0x1 << uint(7-i&0x7)
			cnt++
		}
	}
	return cnt
}

func matchUint16BetweenGeneric(src []uint16, a, b uint16, bits []byte) int64 {
	diff := b - a + 1
	var cnt int64
	for i, v := range src {
		if v-a < diff {
			bits[i>>3] |= 0x1 << uint(7-i&0x7)
			cnt++
		}
	}
	return cnt
}
