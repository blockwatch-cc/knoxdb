// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package num

func matchInt64EqualGeneric(src []int64, val int64, bits []byte) int64 {
	var cnt int64
	for i, v := range src {
		if v == val {
			bits[i>>3] |= 0x1 << uint(i&0x7)
			cnt++
		}
	}
	return cnt
}

func matchInt64NotEqualGeneric(src []int64, val int64, bits []byte) int64 {
	var cnt int64
	for i, v := range src {
		if v != val {
			bits[i>>3] |= 0x1 << uint(i&0x7)
			cnt++
		}
	}
	return cnt
}

func matchInt64LessThanGeneric(src []int64, val int64, bits []byte) int64 {
	var cnt int64
	for i, v := range src {
		if v < val {
			bits[i>>3] |= 0x1 << uint(i&0x7)
			cnt++
		}
	}
	return cnt
}

func matchInt64LessThanEqualGeneric(src []int64, val int64, bits []byte) int64 {
	var cnt int64
	for i, v := range src {
		if v <= val {
			bits[i>>3] |= 0x1 << uint(i&0x7)
			cnt++
		}
	}
	return cnt
}

func matchInt64GreaterThanGeneric(src []int64, val int64, bits []byte) int64 {
	var cnt int64
	for i, v := range src {
		if v > val {
			bits[i>>3] |= 0x1 << uint(i&0x7)
			cnt++
		}
	}
	return cnt
}

func matchInt64GreaterThanEqualGeneric(src []int64, val int64, bits []byte) int64 {
	var cnt int64
	for i, v := range src {
		if v >= val {
			bits[i>>3] |= 0x1 << uint(i&0x7)
			cnt++
		}
	}
	return cnt
}

func matchInt64BetweenGeneric(src []int64, a, b int64, bits []byte) int64 {
	diff := uint64(b - a + 1)
	var cnt int64
	for i, v := range src {
		if uint64(v-a) < diff {
			bits[i>>3] |= 0x1 << uint(i&0x7)
			cnt++
		}
	}
	return cnt
}
