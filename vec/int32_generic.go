// Copyright (c) 2020 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

package vec

func matchInt32EqualGeneric(src []int32, val int32, bits []byte) int64 {
	var cnt int64
	for i, v := range src {
		if v == val {
			bits[i>>3] |= 0x1 << uint(i&0x7)
			cnt++
		}
	}
	return cnt
}

func matchInt32NotEqualGeneric(src []int32, val int32, bits []byte) int64 {
	var cnt int64
	for i, v := range src {
		if v != val {
			bits[i>>3] |= 0x1 << uint(i&0x7)
			cnt++
		}
	}
	return cnt
}

func matchInt32LessThanGeneric(src []int32, val int32, bits []byte) int64 {
	var cnt int64
	for i, v := range src {
		if v < val {
			bits[i>>3] |= 0x1 << uint(i&0x7)
			cnt++
		}
	}
	return cnt
}

func matchInt32LessThanEqualGeneric(src []int32, val int32, bits []byte) int64 {
	var cnt int64
	for i, v := range src {
		if v <= val {
			bits[i>>3] |= 0x1 << uint(i&0x7)
			cnt++
		}
	}
	return cnt
}

func matchInt32GreaterThanGeneric(src []int32, val int32, bits []byte) int64 {
	var cnt int64
	for i, v := range src {
		if v > val {
			bits[i>>3] |= 0x1 << uint(i&0x7)
			cnt++
		}
	}
	return cnt
}

func matchInt32GreaterThanEqualGeneric(src []int32, val int32, bits []byte) int64 {
	var cnt int64
	for i, v := range src {
		if v >= val {
			bits[i>>3] |= 0x1 << uint(i&0x7)
			cnt++
		}
	}
	return cnt
}

func matchInt32BetweenGeneric(src []int32, a, b int32, bits []byte) int64 {
	diff := uint32(b - a + 1)
	var cnt int64
	for i, v := range src {
		if uint32(v-a) < diff {
			bits[i>>3] |= 0x1 << uint(i&0x7)
			cnt++
		}
	}
	return cnt
}
