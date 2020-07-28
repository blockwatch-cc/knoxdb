// Copyright (c) 2020 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

// +build go1.7,amd64,!gccgo,!appengine

package vec
/*
//go:noescape
func matchUint64EqualAVX2(src []uint64, val uint64, bits []byte) int64

//go:noescape
func matchUint64NotEqualAVX2(src []uint64, val uint64, bits []byte) int64

//go:noescape
func matchUint64LessThanAVX2(src []uint64, val uint64, bits []byte) int64

//go:noescape
func matchUint64LessThanEqualAVX2(src []uint64, val uint64, bits []byte) int64

//go:noescape
func matchUint64GreaterThanAVX2(src []uint64, val uint64, bits []byte) int64

//go:noescape
func matchUint64GreaterThanEqualAVX2(src []uint64, val uint64, bits []byte) int64

//go:noescape
func matchUint64BetweenAVX2(src []uint64, a, b uint64, bits []byte) int64
*/
func matchUint32Equal(src []uint32, val uint32, bits []byte) int64 {
	//switch {
	//case useAVX2:
	//	return matchUint64EqualAVX2(src, val, bits)
	//default:
		return matchUint32EqualGeneric(src, val, bits)
	//}
}
/*
func matchUint64NotEqual(src []uint64, val uint64, bits []byte) int64 {
	switch {
	case useAVX2:
		return matchUint64NotEqualAVX2(src, val, bits)
	default:
		return matchUint64NotEqualGeneric(src, val, bits)
	}
}
*/
func matchUint32LessThan(src []uint32, val uint32, bits []byte) int64 {
	//switch {
	//case useAVX2:
	//	return matchUint64LessThanAVX2(src, val, bits)
	//default:
		return matchUint32LessThanGeneric(src, val, bits)
	//}
}
/*
func matchUint64LessThanEqual(src []uint64, val uint64, bits []byte) int64 {
	switch {
	case useAVX2:
		return matchUint64LessThanEqualAVX2(src, val, bits)
	default:
		return matchUint64LessThanEqualGeneric(src, val, bits)
	}
}

func matchUint64GreaterThan(src []uint64, val uint64, bits []byte) int64 {
	switch {
	case useAVX2:
		return matchUint64GreaterThanAVX2(src, val, bits)
	default:
		return matchUint64GreaterThanGeneric(src, val, bits)
	}
}

func matchUint64GreaterThanEqual(src []uint64, val uint64, bits []byte) int64 {
	switch {
	case useAVX2:
		return matchUint64GreaterThanEqualAVX2(src, val, bits)
	default:
		return matchUint64GreaterThanEqualGeneric(src, val, bits)
	}
}

func matchUint64Between(src []uint64, a, b uint64, bits []byte) int64 {
	switch {
	case useAVX2:
		return matchUint64BetweenAVX2(src, a, b, bits)
	default:
		return matchUint64BetweenGeneric(src, a, b, bits)
	}
}
*/
