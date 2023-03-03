// Copyright (c) 2020 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

package num

func matchUint8EqualGeneric(src []uint8, val uint8, bits []byte) int64 {
	var cnt int64
	for i, v := range src {
		if v == val {
			bits[i>>3] |= 0x1 << uint(i&0x7)
			cnt++
		}
	}
	return cnt
}

func matchUint8NotEqualGeneric(src []uint8, val uint8, bits []byte) int64 {
	var cnt int64
	for i, v := range src {
		if v != val {
			bits[i>>3] |= 0x1 << uint(i&0x7)
			cnt++
		}
	}
	return cnt
}

func matchUint8LessThanGeneric(src []uint8, val uint8, bits []byte) int64 {
	var cnt int64
	for i, v := range src {
		if v < val {
			bits[i>>3] |= 0x1 << uint(i&0x7)
			cnt++
		}
	}
	return cnt
}

func matchUint8LessThanEqualGeneric(src []uint8, val uint8, bits []byte) int64 {
	var cnt int64
	for i, v := range src {
		if v <= val {
			bits[i>>3] |= 0x1 << uint(i&0x7)
			cnt++
		}
	}
	return cnt
}

func matchUint8GreaterThanGeneric(src []uint8, val uint8, bits []byte) int64 {
	var cnt int64
	for i, v := range src {
		if v > val {
			bits[i>>3] |= 0x1 << uint(i&0x7)
			cnt++
		}
	}
	return cnt
}

func matchUint8GreaterThanEqualGeneric(src []uint8, val uint8, bits []byte) int64 {
	var cnt int64
	for i, v := range src {
		if v >= val {
			bits[i>>3] |= 0x1 << uint(i&0x7)
			cnt++
		}
	}
	return cnt
}

func matchUint8BetweenGeneric(src []uint8, a, b uint8, bits []byte) int64 {
	diff := b - a + 1
	var cnt int64
	for i, v := range src {
		if v-a < diff {
			bits[i>>3] |= 0x1 << uint(i&0x7)
			cnt++
		}
	}
	return cnt
}
