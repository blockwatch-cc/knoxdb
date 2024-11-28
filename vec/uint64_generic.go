// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package vec

func matchUint64EqualGeneric(src []uint64, val uint64, bits []byte) int64 {
	var cnt int64
	for i, v := range src {
		if v == val {
			bits[i>>3] |= 0x1 << uint(i&0x7)
			cnt++
		}
	}
	return cnt
}

func matchUint64NotEqualGeneric(src []uint64, val uint64, bits []byte) int64 {
	var cnt int64
	for i, v := range src {
		if v != val {
			bits[i>>3] |= 0x1 << uint(i&0x7)
			cnt++
		}
	}
	return cnt
}

func matchUint64LessThanGeneric(src []uint64, val uint64, bits []byte) int64 {
	var cnt int64
	for i, v := range src {
		if v < val {
			bits[i>>3] |= 0x1 << uint(i&0x7)
			cnt++
		}
	}
	return cnt
}

func matchUint64LessThanEqualGeneric(src []uint64, val uint64, bits []byte) int64 {
	var cnt int64
	for i, v := range src {
		if v <= val {
			bits[i>>3] |= 0x1 << uint(i&0x7)
			cnt++
		}
	}
	return cnt
}

func matchUint64GreaterThanGeneric(src []uint64, val uint64, bits []byte) int64 {
	var cnt int64
	for i, v := range src {
		if v > val {
			bits[i>>3] |= 0x1 << uint(i&0x7)
			cnt++
		}
	}
	return cnt
}

func matchUint64GreaterThanEqualGeneric(src []uint64, val uint64, bits []byte) int64 {
	var cnt int64
	for i, v := range src {
		if v >= val {
			bits[i>>3] |= 0x1 << uint(i&0x7)
			cnt++
		}
	}
	return cnt
}

func matchUint64BetweenGeneric(src []uint64, a, b uint64, bits []byte) int64 {
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
