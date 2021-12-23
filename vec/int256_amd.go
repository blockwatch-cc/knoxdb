// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

//go:build go1.7 && amd64 && !gccgo && !appengine
// +build go1.7,amd64,!gccgo,!appengine

package vec

//go:noescape
func matchInt256EqualAVX2Core(src Int256LLSlice, val Int256, bits []byte) int64

//go:noescape
func matchInt256NotEqualAVX2Core(src Int256LLSlice, val Int256, bits []byte) int64

//go:noescape
func matchInt256LessThanAVX2Core(src Int256LLSlice, val Int256, bits []byte) int64

//go:noescape
func matchInt256LessThanEqualAVX2Core(src Int256LLSlice, val Int256, bits []byte) int64

//go:noescape
func matchInt256GreaterThanAVX2Core(src Int256LLSlice, val Int256, bits []byte) int64

//go:noescape
func matchInt256GreaterThanEqualAVX2Core(src Int256LLSlice, val Int256, bits []byte) int64

//go:noescape
func matchInt256BetweenAVX2Core(src Int256LLSlice, a, b Int256, bits []byte) int64

// //go:noescape
// func matchInt256EqualAVX512(src Int256LLSlice, val Int256, bits []byte) int64

// //go:noescape
// func matchInt256NotEqualAVX512(src Int256LLSlice, val Int256, bits []byte) int64

// //go:noescape
// func matchInt256LessThanAVX512(src Int256LLSlice, val Int256, bits []byte) int64

// //go:noescape
// func matchInt256LessThanEqualAVX512(src Int256LLSlice, val Int256, bits []byte) int64

// //go:noescape
// func matchInt256GreaterThanAVX512(src Int256LLSlice, val Int256, bits []byte) int64

// //go:noescape
// func matchInt256GreaterThanEqualAVX512(src Int256LLSlice, val Int256, bits []byte) int64

// //go:noescape
// func matchInt256BetweenAVX512(src Int256LLSlice, a, b Int256, bits []byte) int64

func matchInt256Equal(src Int256LLSlice, val Int256, bits, mask []byte) int64 {
	switch {
	// case useAVX512_F:
	// 	return matchInt256EqualAVX512(src, val, bits)
	case useAVX2:
		return matchInt256EqualAVX2(src, val, bits)
	default:
		return matchInt256EqualGeneric(src, val, bits, mask)
	}
}

func matchInt256NotEqual(src Int256LLSlice, val Int256, bits, mask []byte) int64 {
	switch {
	// case useAVX512_F:
	// 	return matchInt256NotEqualAVX512(src, val, bits)
	case useAVX2:
		return matchInt256NotEqualAVX2(src, val, bits)
	default:
		return matchInt256NotEqualGeneric(src, val, bits, mask)
	}
}

func matchInt256LessThan(src Int256LLSlice, val Int256, bits, mask []byte) int64 {
	switch {
	// case useAVX512_F:
	// 	return matchInt256LessThanAVX512(src, val, bits)
	case useAVX2:
		return matchInt256LessThanAVX2(src, val, bits)
	default:
		return matchInt256LessThanGeneric(src, val, bits, mask)
	}
}

func matchInt256LessThanEqual(src Int256LLSlice, val Int256, bits, mask []byte) int64 {
	switch {
	// case useAVX512_F:
	// 	return matchInt256LessThanEqualAVX512(src, val, bits)
	case useAVX2:
		return matchInt256LessThanEqualAVX2(src, val, bits)
	default:
		return matchInt256LessThanEqualGeneric(src, val, bits, mask)
	}
}

func matchInt256GreaterThan(src Int256LLSlice, val Int256, bits, mask []byte) int64 {
	switch {
	// case useAVX512_F:
	// 	return matchInt256GreaterThanAVX512(src, val, bits)
	case useAVX2:
		return matchInt256GreaterThanAVX2(src, val, bits)
	default:
		return matchInt256GreaterThanGeneric(src, val, bits, mask)
	}
}

func matchInt256GreaterThanEqual(src Int256LLSlice, val Int256, bits, mask []byte) int64 {
	switch {
	// case useAVX512_F:
	// 	return matchInt256GreaterThanEqualAVX512(src, val, bits)
	case useAVX2:
		return matchInt256GreaterThanEqualAVX2(src, val, bits)
	default:
		return matchInt256GreaterThanEqualGeneric(src, val, bits, mask)
	}
}

func matchInt256Between(src Int256LLSlice, a, b Int256, bits, mask []byte) int64 {
	switch {
	// case useAVX512_F:
	// 	return matchInt256BetweenAVX512(src, a, b, bits)
	case useAVX2:
		return matchInt256BetweenAVX2(src, a, b, bits)
	default:
		return matchInt256BetweenGeneric(src, a, b, bits, mask)
	}
}

func matchInt256EqualAVX2(src Int256LLSlice, val Int256, bits []byte) int64 {
	len_head := src.Len() & 0x7fffffffffffffe0
	res := matchInt256EqualAVX2Core(src, val, bits)
	res += matchInt256EqualGeneric(src.Tail(len_head), val, bits[bitFieldLen(len_head):], nil)
	return res
}

func matchInt256NotEqualAVX2(src Int256LLSlice, val Int256, bits []byte) int64 {
	len_head := src.Len() & 0x7fffffffffffffe0
	res := matchInt256NotEqualAVX2Core(src, val, bits)
	res += matchInt256NotEqualGeneric(src.Tail(len_head), val, bits[bitFieldLen(len_head):], nil)
	return res
}

func matchInt256LessThanAVX2(src Int256LLSlice, val Int256, bits []byte) int64 {
	len_head := src.Len() & 0x7fffffffffffffe0
	res := matchInt256LessThanAVX2Core(src, val, bits)
	res += matchInt256LessThanGeneric(src.Tail(len_head), val, bits[bitFieldLen(len_head):], nil)
	return res
}

func matchInt256LessThanEqualAVX2(src Int256LLSlice, val Int256, bits []byte) int64 {
	len_head := src.Len() & 0x7fffffffffffffe0
	res := matchInt256LessThanEqualAVX2Core(src, val, bits)
	res += matchInt256LessThanEqualGeneric(src.Tail(len_head), val, bits[bitFieldLen(len_head):], nil)
	return res
}

func matchInt256GreaterThanAVX2(src Int256LLSlice, val Int256, bits []byte) int64 {
	len_head := src.Len() & 0x7fffffffffffffe0
	res := matchInt256GreaterThanAVX2Core(src, val, bits)
	res += matchInt256GreaterThanGeneric(src.Tail(len_head), val, bits[bitFieldLen(len_head):], nil)
	return res
}

func matchInt256GreaterThanEqualAVX2(src Int256LLSlice, val Int256, bits []byte) int64 {
	len_head := src.Len() & 0x7fffffffffffffe0
	res := matchInt256GreaterThanEqualAVX2Core(src, val, bits)
	res += matchInt256GreaterThanEqualGeneric(src.Tail(len_head), val, bits[bitFieldLen(len_head):], nil)
	return res
}

func matchInt256BetweenAVX2(src Int256LLSlice, a, b Int256, bits []byte) int64 {
	len_head := src.Len() & 0x7fffffffffffffe0
	res := matchInt256BetweenAVX2Core(src, a, b, bits)
	res += matchInt256BetweenGeneric(src.Tail(len_head), a, b, bits[bitFieldLen(len_head):], nil)
	return res
}
