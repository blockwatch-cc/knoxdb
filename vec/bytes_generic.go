// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package vec

import (
	"bytes"
)

func matchBytesEqualGeneric(src [][]byte, val []byte, bits []byte) int64 {
	var cnt int64
	for i, v := range src {
		if bytes.Compare(v, val) == 0 {
			bits[i>>3] |= 0x1 << uint(7-i&0x7)
			cnt++
		}
	}
	return cnt
}

func matchBytesNotEqualGeneric(src [][]byte, val []byte, bits []byte) int64 {
	var cnt int64
	for i, v := range src {
		if bytes.Compare(v, val) != 0 {
			bits[i>>3] |= 0x1 << uint(7-i&0x7)
			cnt++
		}
	}
	return cnt
}

func matchBytesLessThanGeneric(src [][]byte, val []byte, bits []byte) int64 {
	var cnt int64
	for i, v := range src {
		if bytes.Compare(v, val) < 0 {
			bits[i>>3] |= 0x1 << uint(7-i&0x7)
			cnt++
		}
	}
	return cnt
}

func matchBytesLessThanEqualGeneric(src [][]byte, val []byte, bits []byte) int64 {
	var cnt int64
	for i, v := range src {
		if bytes.Compare(v, val) <= 0 {
			bits[i>>3] |= 0x1 << uint(7-i&0x7)
			cnt++
		}
	}
	return cnt
}

func matchBytesGreaterThanGeneric(src [][]byte, val []byte, bits []byte) int64 {
	var cnt int64
	for i, v := range src {
		if bytes.Compare(v, val) > 0 {
			bits[i>>3] |= 0x1 << uint(7-i&0x7)
			cnt++
		}
	}
	return cnt
}

func matchBytesGreaterThanEqualGeneric(src [][]byte, val []byte, bits []byte) int64 {
	var cnt int64
	for i, v := range src {
		if bytes.Compare(v, val) >= 0 {
			bits[i>>3] |= 0x1 << uint(7-i&0x7)
			cnt++
		}
	}
	return cnt
}

func matchBytesBetweenGeneric(src [][]byte, a, b, bits []byte) int64 {
	if d := bytes.Compare(a, b); d < 0 {
		b, a = a, b
	} else if d == 0 {
		return matchBytesEqualGeneric(src, a, bits)
	}
	var cnt int64
	for i, v := range src {
		if bytes.Compare(v, a) < 0 {
			continue
		}
		if bytes.Compare(v, b) > 0 {
			continue
		}
		bits[i>>3] = bits[i>>3] | 0x1<<uint(7-i&0x7)
		cnt++
	}
	return cnt
}
