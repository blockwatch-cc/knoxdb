// Copyright (c) 2020 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

// +build go1.7,amd64,!gccgo,!appengine

package vec

/*
//go:noescape
func matchInt8EqualAVX2(src []int8, val int8, bits []byte) int64

//go:noescape
func matchInt8NotEqualAVX2(src []int8, val int8, bits []byte) int64

//go:noescape
func matchInt8LessThanAVX2(src []int8, val int8, bits []byte) int64

//go:noescape
func matchInt8LessThanEqualAVX2(src []int8, val int8, bits []byte) int64

//go:noescape
func matchInt8GreaterThanAVX2(src []int8, val int8, bits []byte) int64

//go:noescape
func matchInt8GreaterThanEqualAVX2(src []int8, val int8, bits []byte) int64

//go:noescape
func matchInt8BetweenAVX2(src []int8, a, b int8, bits []byte) int64
*/
//go:noescape
func matchInt8EqualAVX512(src []int8, val int8, bits []byte) int64

//go:noescape
func matchInt8NotEqualAVX512(src []int8, val int8, bits []byte) int64

//go:noescape
func matchInt8LessThanAVX512(src []int8, val int8, bits []byte) int64

//go:noescape
func matchInt8LessThanEqualAVX512(src []int8, val int8, bits []byte) int64

//go:noescape
func matchInt8GreaterThanAVX512(src []int8, val int8, bits []byte) int64

//go:noescape
func matchInt8GreaterThanEqualAVX512(src []int8, val int8, bits []byte) int64

//go:noescape
func matchInt8BetweenAVX512(src []int8, a, b int8, bits []byte) int64

func matchInt8Equal(src []int8, val int8, bits []byte) int64 {
	//switch {
	//case useAVX2:
	//	return matchInt8EqualAVX2(src, val, bits)
	//default:
	return matchInt8EqualGeneric(src, val, bits)
	//}
}

func matchInt8NotEqual(src []int8, val int8, bits []byte) int64 {
	//switch {
	//case useAVX2:
	//	return matchInt8NotEqualAVX2(src, val, bits)
	//default:
	return matchInt8NotEqualGeneric(src, val, bits)
	//}
}

func matchInt8LessThan(src []int8, val int8, bits []byte) int64 {
	//switch {
	//case useAVX2:
	//	return matchInt8LessThanAVX2(src, val, bits)
	//default:
	return matchInt8LessThanGeneric(src, val, bits)
	//}
}

func matchInt8LessThanEqual(src []int8, val int8, bits []byte) int64 {
	//switch {
	//case useAVX2:
	//	return matchInt8LessThanEqualAVX2(src, val, bits)
	//default:
	return matchInt8LessThanEqualGeneric(src, val, bits)
	//}
}

func matchInt8GreaterThan(src []int8, val int8, bits []byte) int64 {
	//switch {
	//case useAVX2:
	//	return matchInt8GreaterThanAVX2(src, val, bits)
	//default:
	return matchInt8GreaterThanGeneric(src, val, bits)
	//}
}

func matchInt8GreaterThanEqual(src []int8, val int8, bits []byte) int64 {
	//switch {
	//case useAVX2:
	//	return matchInt8GreaterThanEqualAVX2(src, val, bits)
	//default:
	return matchInt8GreaterThanEqualGeneric(src, val, bits)
	//}
}

func matchInt8Between(src []int8, a, b int8, bits []byte) int64 {
	//switch {
	//case useAVX2:
	//	return matchInt8BetweenAVX2(src, a, b, bits)
	//default:
	return matchInt8BetweenGeneric(src, a, b, bits)
	//}
}
