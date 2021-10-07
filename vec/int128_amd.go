// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

// +build go1.7,amd64,!gccgo,!appengine

package vec

import (
	"blockwatch.cc/knoxdb/util"
)

//go:noescape
func matchInt128EqualAVX2Core(src Int128LLSlice, val Int128, bits []byte) int64

//go:noescape
func matchInt128NotEqualAVX2Core(src Int128LLSlice, val Int128, bits []byte) int64

//go:noescape
func matchInt128LessThanAVX2Core(src Int128LLSlice, val Int128, bits []byte) int64

//go:noescape
func matchInt128LessThanEqualAVX2Core(src Int128LLSlice, val Int128, bits []byte) int64

//go:noescape
func matchInt128GreaterThanAVX2Core(src Int128LLSlice, val Int128, bits []byte) int64

//go:noescape
func matchInt128GreaterThanEqualAVX2Core(src Int128LLSlice, val Int128, bits []byte) int64

//go:noescape
func matchInt128BetweenAVX2Core(src Int128LLSlice, a, b Int128, bits []byte) int64

// //go:noescape
// func matchInt128EqualAVX512(src []Int128, val Int128, bits []byte) int64

// //go:noescape
// func matchInt128NotEqualAVX512(src []Int128, val Int128, bits []byte) int64

// //go:noescape
// func matchInt128LessThanAVX512(src []Int128, val Int128, bits []byte) int64

// //go:noescape
// func matchInt128LessThanEqualAVX512(src []Int128, val Int128, bits []byte) int64

// //go:noescape
// func matchInt128GreaterThanAVX512(src []Int128, val Int128, bits []byte) int64

// //go:noescape
// func matchInt128GreaterThanEqualAVX512(src []Int128, val Int128, bits []byte) int64

// //go:noescape
// func matchInt128BetweenAVX512(src []Int128, a, b Int128, bits []byte) int64

func matchInt128Equal(src Int128LLSlice, val Int128, bits, mask []byte) int64 {
	switch {
	// case util.UseAVX512_F:
	// 	return matchInt128EqualAVX512(src, val, bits)
	case util.UseAVX2:
		return matchInt128EqualAVX2(src, val, bits)
	default:
		return matchInt128EqualGeneric(src, val, bits, mask)
	}
}

func matchInt128NotEqual(src Int128LLSlice, val Int128, bits, mask []byte) int64 {
	switch {
	// case util.UseAVX512_F:
	// 	return matchInt128NotEqualAVX512(src, val, bits)
	case util.UseAVX2:
		return matchInt128NotEqualAVX2(src, val, bits)
	default:
		return matchInt128NotEqualGeneric(src, val, bits, mask)
	}
}

func matchInt128LessThan(src Int128LLSlice, val Int128, bits, mask []byte) int64 {
	switch {
	// case util.UseAVX512_F:
	// 	return matchInt128LessThanAVX512(src, val, bits)
	case util.UseAVX2:
		return matchInt128LessThanAVX2(src, val, bits)
	default:
		return matchInt128LessThanGeneric(src, val, bits, mask)
	}
}

func matchInt128LessThanEqual(src Int128LLSlice, val Int128, bits, mask []byte) int64 {
	switch {
	// case util.UseAVX512_F:
	// 	return matchInt128LessThanEqualAVX512(src, val, bits)
	case util.UseAVX2:
		return matchInt128LessThanEqualAVX2(src, val, bits)
	default:
		return matchInt128LessThanEqualGeneric(src, val, bits, mask)
	}
}

func matchInt128GreaterThan(src Int128LLSlice, val Int128, bits, mask []byte) int64 {
	switch {
	// case util.UseAVX512_F:
	// 	return matchInt128GreaterThanAVX512(src, val, bits)
	case util.UseAVX2:
		return matchInt128GreaterThanAVX2(src, val, bits)
	default:
		return matchInt128GreaterThanGeneric(src, val, bits, mask)
	}
}

func matchInt128GreaterThanEqual(src Int128LLSlice, val Int128, bits, mask []byte) int64 {
	switch {
	// case util.UseAVX512_F:
	// 	return matchInt128GreaterThanEqualAVX512(src, val, bits)
	case util.UseAVX2:
		return matchInt128GreaterThanEqualAVX2(src, val, bits)
	default:
		return matchInt128GreaterThanEqualGeneric(src, val, bits, mask)
	}
}

func matchInt128Between(src Int128LLSlice, a, b Int128, bits, mask []byte) int64 {
	switch {
	// case util.UseAVX512_F:
	// 	return matchInt128BetweenAVX512(src, a, b, bits)
	case util.UseAVX2:
		return matchInt128BetweenAVX2(src, a, b, bits)
	default:
		return matchInt128BetweenGeneric(src, a, b, bits, mask)
	}
}

func matchInt128EqualAVX2(src Int128LLSlice, val Int128, bits []byte) int64 {
	len_head := src.Len() & 0x7fffffffffffffe0
	res := matchInt128EqualAVX2Core(src, val, bits)
	res += matchInt128EqualGeneric(src.Tail(len_head), val, bits[bitFieldLen(len_head):], nil)
	return res
}

func matchInt128NotEqualAVX2(src Int128LLSlice, val Int128, bits []byte) int64 {
	len_head := src.Len() & 0x7fffffffffffffe0
	res := matchInt128NotEqualAVX2Core(src, val, bits)
	res += matchInt128NotEqualGeneric(src.Tail(len_head), val, bits[bitFieldLen(len_head):], nil)
	return res
}

func matchInt128LessThanAVX2(src Int128LLSlice, val Int128, bits []byte) int64 {
	len_head := src.Len() & 0x7fffffffffffffe0
	res := matchInt128LessThanAVX2Core(src, val, bits)
	res += matchInt128LessThanGeneric(src.Tail(len_head), val, bits[bitFieldLen(len_head):], nil)
	return res
}

func matchInt128LessThanEqualAVX2(src Int128LLSlice, val Int128, bits []byte) int64 {
	len_head := src.Len() & 0x7fffffffffffffe0
	res := matchInt128LessThanEqualAVX2Core(src, val, bits)
	res += matchInt128LessThanEqualGeneric(src.Tail(len_head), val, bits[bitFieldLen(len_head):], nil)
	return res
}

func matchInt128GreaterThanAVX2(src Int128LLSlice, val Int128, bits []byte) int64 {
	len_head := src.Len() & 0x7fffffffffffffe0
	res := matchInt128GreaterThanAVX2Core(src, val, bits)
	res += matchInt128GreaterThanGeneric(src.Tail(len_head), val, bits[bitFieldLen(len_head):], nil)
	return res
}

func matchInt128GreaterThanEqualAVX2(src Int128LLSlice, val Int128, bits []byte) int64 {
	len_head := src.Len() & 0x7fffffffffffffe0
	res := matchInt128GreaterThanEqualAVX2Core(src, val, bits)
	res += matchInt128GreaterThanEqualGeneric(src.Tail(len_head), val, bits[bitFieldLen(len_head):], nil)
	return res
}

func matchInt128BetweenAVX2(src Int128LLSlice, a, b Int128, bits []byte) int64 {
	len_head := src.Len() & 0x7fffffffffffffe0
	res := matchInt128BetweenAVX2Core(src, a, b, bits)
	res += matchInt128BetweenGeneric(src.Tail(len_head), a, b, bits[bitFieldLen(len_head):], nil)
	return res
}
