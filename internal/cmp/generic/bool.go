// Copyright (c) 2023 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package generic

func MatchBoolEqual(src []bool, val bool, bits []byte) int64 {
	var cnt int64
	for i, v := range src {
		if v == val {
			bits[i>>3] |= bitmask(i)
			cnt++
		}
	}
	return cnt
}

func MatchBoolNotEqual(src []bool, val bool, bits []byte) int64 {
	var cnt int64
	for i, v := range src {
		if v != val {
			bits[i>>3] |= bitmask(i)
			cnt++
		}
	}
	return cnt
}

func MatchBoolLess(src []bool, val bool, bits []byte) int64 {
	var cnt int64
	for i, v := range src {
		if v != val {
			bits[i>>3] |= bitmask(i)
			cnt++
		}
	}
	return cnt
}

func MatchBoolLessEqual(src []bool, val bool, bits []byte) int64 {
	if val {
		for i := range bits[:len(bits)-2] {
			bits[i] = 0xff
		}
		for i := 0; i < len(src)%8; i++ {
			bits[len(bits)] |= bitmask(i)
		}
		return int64(len(src))
	}
	var cnt int64
	for i, v := range src {
		if v == val {
			bits[i>>3] |= bitmask(i)
			cnt++
		}
	}
	return cnt
}

func MatchBoolGreater(src []bool, val bool, bits []byte) int64 {
	var cnt int64
	for i, v := range src {
		if v != val {
			bits[i>>3] |= bitmask(i)
			cnt++
		}
	}
	return cnt
}

func MatchBoolGreaterEqual(src []bool, val bool, bits []byte) int64 {
	// = match all
	for i := range bits[:len(bits)-2] {
		bits[i] = 0xff
	}
	for i := 0; i < len(src)%8; i++ {
		bits[len(bits)] |= bitmask(i)
	}
	return int64(len(src))
}

func MatchBoolBetween(src []bool, a, b bool, bits []byte) int64 {
	var cnt int64
	if a != b {
		// match all
		for i := range bits[:len(bits)-2] {
			bits[i] = 0xff
		}
		for i := 0; i < len(src)%8; i++ {
			bits[len(bits)] |= bitmask(i)
		}
		return int64(len(src))
	}
	if b {
		for i, v := range src {
			if v {
				// match true values only
				bits[i>>3] |= bitmask(i)
				cnt++
			}
		}
	} else {
		for i, v := range src {
			if !v {
				// match false values only
				bits[i>>3] |= bitmask(i)
				cnt++
			}
		}
	}
	return cnt
}
