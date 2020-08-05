// Copyright (c) 2020 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

// +build go1.7,amd64,!gccgo,!appengine

package vec

//go:noescape
func matchUint16EqualAVX2(src []uint16, val uint16, bits []byte) int64
/*
//go:noescape
func matchUint16NotEqualAVX2(src []uint16, val uint16, bits []byte) int64

//go:noescape
func matchUint16LessThanAVX2(src []uint16, val uint16, bits []byte) int64

//go:noescape
func matchUint16LessThanEqualAVX2(src []uint16, val uint16, bits []byte) int64

//go:noescape
func matchUint16GreaterThanAVX2(src []uint16, val uint16, bits []byte) int64

//go:noescape
func matchUint16GreaterThanEqualAVX2(src []uint16, val uint16, bits []byte) int64

//go:noescape
func matchUint16BetweenAVX2(src []uint16, a, b uint16, bits []byte) int64
*/
func matchUint16Equal(src []uint16, val uint16, bits []byte) int64 {
	switch {
	case useAVX2:
		return matchUint16EqualAVX2(src, val, bits)
	default:
		return matchUint16EqualGeneric(src, val, bits)
	}
}

func matchUint16NotEqual(src []uint16, val uint16, bits []byte) int64 {
	//switch {
	//case useAVX2:
	//	return matchUint16NotEqualAVX2(src, val, bits)
	//default:
		return matchUint16NotEqualGeneric(src, val, bits)
	//}
}

func matchUint16LessThan(src []uint16, val uint16, bits []byte) int64 {
	//switch {
	//case useAVX2:
	//	return matchUint16LessThanAVX2(src, val, bits)
	//default:
		return matchUint16LessThanGeneric(src, val, bits)
	//}
}

func matchUint16LessThanEqual(src []uint16, val uint16, bits []byte) int64 {
	//switch {
	//case useAVX2:
	//	return matchUint16LessThanEqualAVX2(src, val, bits)
	//default:
		return matchUint16LessThanEqualGeneric(src, val, bits)
	//}
}

func matchUint16GreaterThan(src []uint16, val uint16, bits []byte) int64 {
	//switch {
	//case useAVX2:
	//	return matchUint16GreaterThanAVX2(src, val, bits)
	//default:
		return matchUint16GreaterThanGeneric(src, val, bits)
	//}
}

func matchUint16GreaterThanEqual(src []uint16, val uint16, bits []byte) int64 {
	//switch {
	//case useAVX2:
	//	return matchUint16GreaterThanEqualAVX2(src, val, bits)
	//default:
		return matchUint16GreaterThanEqualGeneric(src, val, bits)
	//}
}

func matchUint16Between(src []uint16, a, b uint16, bits []byte) int64 {
	//switch {
	//case useAVX2:
	//	return matchUint16BetweenAVX2(src, a, b, bits)
	//default:
		return matchUint16BetweenGeneric(src, a, b, bits)
	//}
}
