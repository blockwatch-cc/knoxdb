// Copyright (c) 2020 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

package num

func matchInt16EqualGeneric(src []int16, val int16, bits []byte) int64 {
	var cnt int64
	for i, v := range src {
		if v == val {
			bits[i>>3] |= 0x1 << uint(i&0x7)
			cnt++
		}
	}
	return cnt
}

func matchInt16NotEqualGeneric(src []int16, val int16, bits []byte) int64 {
	var cnt int64
	for i, v := range src {
		if v != val {
			bits[i>>3] |= 0x1 << uint(i&0x7)
			cnt++
		}
	}
	return cnt
}

func matchInt16LessThanGeneric(src []int16, val int16, bits []byte) int64 {
	var cnt int64
	for i, v := range src {
		if v < val {
			bits[i>>3] |= 0x1 << uint(i&0x7)
			cnt++
		}
	}
	return cnt
}

func matchInt16LessThanEqualGeneric(src []int16, val int16, bits []byte) int64 {
	var cnt int64
	for i, v := range src {
		if v <= val {
			bits[i>>3] |= 0x1 << uint(i&0x7)
			cnt++
		}
	}
	return cnt
}

func matchInt16GreaterThanGeneric(src []int16, val int16, bits []byte) int64 {
	var cnt int64
	for i, v := range src {
		if v > val {
			bits[i>>3] |= 0x1 << uint(i&0x7)
			cnt++
		}
	}
	return cnt
}

func matchInt16GreaterThanEqualGeneric(src []int16, val int16, bits []byte) int64 {
	var cnt int64
	for i, v := range src {
		if v >= val {
			bits[i>>3] |= 0x1 << uint(i&0x7)
			cnt++
		}
	}
	return cnt
}

func matchInt16BetweenGeneric(src []int16, a, b int16, bits []byte) int64 {
	diff := uint16(b - a + 1)
	var cnt int64
	for i, v := range src {
		if uint16(v-a) < diff {
			bits[i>>3] |= 0x1 << uint(i&0x7)
			cnt++
		}
	}
	return cnt
}
