// Copyright (c) 2020 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

package vec

func matchUint32EqualGeneric(src []uint32, val uint32, bits []byte) int64 {
	var cnt int64
	for i, v := range src {
		if v == val {
			bits[i>>3] |= 0x1 << uint(7-i&0x7)
			cnt++
		}
	}
	return cnt
}

func matchUint32NotEqualGeneric(src []uint32, val uint32, bits []byte) int64 {
	var cnt int64
	for i, v := range src {
		if v != val {
			bits[i>>3] |= 0x1 << uint(7-i&0x7)
			cnt++
		}
	}
	return cnt
}

func matchUint32LessThanGeneric(src []uint32, val uint32, bits []byte) int64 {
	var cnt int64
	for i, v := range src {
		if v < val {
			bits[i>>3] |= 0x1 << uint(7-i&0x7)
			cnt++
		}
	}
	return cnt
}

func matchUint32LessThanEqualGeneric(src []uint32, val uint32, bits []byte) int64 {
	var cnt int64
	for i, v := range src {
		if v <= val {
			bits[i>>3] |= 0x1 << uint(7-i&0x7)
			cnt++
		}
	}
	return cnt
}

func matchUint32GreaterThanGeneric(src []uint32, val uint32, bits []byte) int64 {
	var cnt int64
	for i, v := range src {
		if v > val {
			bits[i>>3] |= 0x1 << uint(7-i&0x7)
			cnt++
		}
	}
	return cnt
}

func matchUint32GreaterThanEqualGeneric(src []uint32, val uint32, bits []byte) int64 {
	var cnt int64
	for i, v := range src {
		if v >= val {
			bits[i>>3] |= 0x1 << uint(7-i&0x7)
			cnt++
		}
	}
	return cnt
}

func matchUint32BetweenGeneric(src []uint32, a, b uint32, bits []byte) int64 {
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

