// Copyright (c) 2020 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

// +build go1.7,amd64,!gccgo,!appengine

package vec

//go:noescape
func matchUint32EqualAVX2(src []uint32, val uint32, bits []byte) int64

/*
//go:noescape
func matchUint32NotEqualAVX2(src []uint32, val uint32, bits []byte) int64

//go:noescape
func matchUint32LessThanAVX2(src []uint32, val uint32, bits []byte) int64

//go:noescape
func matchUint32LessThanEqualAVX2(src []uint32, val uint32, bits []byte) int64

//go:noescape
func matchUint32GreaterThanAVX2(src []uint32, val uint32, bits []byte) int64

//go:noescape
func matchUint32GreaterThanEqualAVX2(src []uint32, val uint32, bits []byte) int64

//go:noescape
func matchUint32BetweenAVX2(src []uint32, a, b uint32, bits []byte) int64
*/

//go:noescape
func matchUint32EqualAVX512(src []uint32, val uint32, bits []byte) int64

//go:noescape
func matchUint32NotEqualAVX512(src []uint32, val uint32, bits []byte) int64

//go:noescape
func matchUint32LessThanAVX512(src []uint32, val uint32, bits []byte) int64

//go:noescape
func matchUint32LessThanEqualAVX512(src []uint32, val uint32, bits []byte) int64

//go:noescape
func matchUint32GreaterThanAVX512(src []uint32, val uint32, bits []byte) int64

//go:noescape
func matchUint32GreaterThanEqualAVX512(src []uint32, val uint32, bits []byte) int64

//go:noescape
func matchUint32BetweenAVX512(src []uint32, a, b uint32, bits []byte) int64

func matchUint32Equal(src []uint32, val uint32, bits []byte) int64 {
	switch {
	case useAVX2:
		return matchUint32EqualAVX2(src, val, bits)
	default:
		return matchUint32EqualGeneric(src, val, bits)
	}
}

func matchUint32NotEqual(src []uint32, val uint32, bits []byte) int64 {
	//switch {
	//case useAVX2:
	//	return matchUint32NotEqualAVX2(src, val, bits)
	//default:
	return matchUint32NotEqualGeneric(src, val, bits)
	//}
}

func matchUint32LessThan(src []uint32, val uint32, bits []byte) int64 {
	//switch {
	//case useAVX2:
	//	return matchUint32LessThanAVX2(src, val, bits)
	//default:
	return matchUint32LessThanGeneric(src, val, bits)
	//}
}

func matchUint32LessThanEqual(src []uint32, val uint32, bits []byte) int64 {
	//switch {
	//case useAVX2:
	//	return matchUint32LessThanEqualAVX2(src, val, bits)
	//default:
	return matchUint32LessThanEqualGeneric(src, val, bits)
	//}
}

func matchUint32GreaterThan(src []uint32, val uint32, bits []byte) int64 {
	//switch {
	//case useAVX2:
	//	return matchUint32GreaterThanAVX2(src, val, bits)
	//default:
	return matchUint32GreaterThanGeneric(src, val, bits)
	//}
}

func matchUint32GreaterThanEqual(src []uint32, val uint32, bits []byte) int64 {
	//switch {
	//case useAVX2:
	//	return matchUint32GreaterThanEqualAVX2(src, val, bits)
	//default:
	return matchUint32GreaterThanEqualGeneric(src, val, bits)
	//}
}

func matchUint32Between(src []uint32, a, b uint32, bits []byte) int64 {
	//switch {
	//case useAVX2:
	//	return matchUint32BetweenAVX2(src, a, b, bits)
	//default:
	return matchUint32BetweenGeneric(src, a, b, bits)
	//}
}
