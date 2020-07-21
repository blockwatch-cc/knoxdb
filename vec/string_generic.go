// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package vec

import (
	"strings"
)

func matchStringsEqualGeneric(src []string, val string, bits []byte) int64 {
	var cnt int64
	for i, v := range src {
		if strings.Compare(v, val) == 0 {
			bits[i>>3] |= 0x1 << uint(7-i&0x7)
			cnt++
		}
	}
	return cnt
}

func matchStringsNotEqualGeneric(src []string, val string, bits []byte) int64 {
	var cnt int64
	for i, v := range src {
		if strings.Compare(v, val) != 0 {
			bits[i>>3] |= 0x1 << uint(7-i&0x7)
			cnt++
		}
	}
	return cnt
}

func matchStringsLessThanGeneric(src []string, val string, bits []byte) int64 {
	var cnt int64
	for i, v := range src {
		if strings.Compare(v, val) < 0 {
			bits[i>>3] |= 0x1 << uint(7-i&0x7)
			cnt++
		}
	}
	return cnt
}

func matchStringsLessThanEqualGeneric(src []string, val string, bits []byte) int64 {
	var cnt int64
	for i, v := range src {
		if strings.Compare(v, val) <= 0 {
			bits[i>>3] |= 0x1 << uint(7-i&0x7)
			cnt++
		}
	}
	return cnt
}

func matchStringsGreaterThanGeneric(src []string, val string, bits []byte) int64 {
	var cnt int64
	for i, v := range src {
		if strings.Compare(v, val) > 0 {
			bits[i>>3] |= 0x1 << uint(7-i&0x7)
			cnt++
		}
	}
	return cnt
}

func matchStringsGreaterThanEqualGeneric(src []string, val string, bits []byte) int64 {
	var cnt int64
	for i, v := range src {
		if strings.Compare(v, val) >= 0 {
			bits[i>>3] |= 0x1 << uint(7-i&0x7)
			cnt++
		}
	}
	return cnt
}

func matchStringsBetweenGeneric(src []string, a, b string, bits []byte) int64 {
	if d := strings.Compare(a, b); d < 0 {
		b, a = a, b
	} else if d == 0 {
		return matchStringsEqualGeneric(src, a, bits)
	}
	var cnt int64
	for i, v := range src {
		if strings.Compare(v, a) < 0 {
			continue
		}
		if strings.Compare(v, b) > 0 {
			continue
		}
		bits[i>>3] |= 0x1 << uint(7-i&0x7)
		cnt++
	}
	return cnt
}
