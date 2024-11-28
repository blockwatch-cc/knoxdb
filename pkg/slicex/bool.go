// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package slicex

func UniqueBools(s []bool) []bool {
	if len(s) == 0 {
		return s
	}
	r := ToBoolBits(s...)
	if r == 3 {
		s = s[:2]
		s[0], s[1] = false, true
	} else {
		s = s[:1]
	}
	return s
}

func ToBoolBits(b ...bool) (r byte) {
	for _, v := range b {
		if r == 3 {
			break
		}
		if v {
			r |= 0x2
		} else {
			r |= 0x1
		}
	}
	return
}

func FromBoolBits(r byte) []bool {
	switch r {
	default:
		return []bool{}
	case 1:
		return []bool{false}
	case 2:
		return []bool{true}
	case 3:
		return []bool{false, true}
	}
}
