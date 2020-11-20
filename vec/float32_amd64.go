// Copyright (c) 2020 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

// +build go1.7,amd64,!gccgo,!appengine

package vec

/*
//go:noescape
func matchFloat32EqualAVX2(src []float32, val float32, bits []byte) int64

//go:noescape
func matchFloat32NotEqualAVX2(src []float32, val float32, bits []byte) int64

//go:noescape
func matchFloat32LessThanAVX2(src []float32, val float32, bits []byte) int64

//go:noescape
func matchFloat32LessThanEqualAVX2(src []float32, val float32, bits []byte) int64

//go:noescape
func matchFloat32GreaterThanAVX2(src []float32, val float32, bits []byte) int64

//go:noescape
func matchFloat32GreaterThanEqualAVX2(src []float32, val float32, bits []byte) int64

//go:noescape
func matchFloat32BetweenAVX2(src []float32, a, b float32, bits []byte) int64
*/
//go:noescape
func matchFloat32EqualAVX512(src []float32, val float32, bits []byte) int64

//go:noescape
func matchFloat32NotEqualAVX512(src []float32, val float32, bits []byte) int64

//go:noescape
func matchFloat32LessThanAVX512(src []float32, val float32, bits []byte) int64

//go:noescape
func matchFloat32LessThanEqualAVX512(src []float32, val float32, bits []byte) int64

//go:noescape
func matchFloat32GreaterThanAVX512(src []float32, val float32, bits []byte) int64

//go:noescape
func matchFloat32GreaterThanEqualAVX512(src []float32, val float32, bits []byte) int64

//go:noescape
func matchFloat32BetweenAVX512(src []float32, a, b float32, bits []byte) int64

func matchFloat32Equal(src []float32, val float32, bits []byte) int64 {
	/*	switch {
		case useAVX512_F:
			return matchFloat32EqualAVX512(src, val, bits)
		case useAVX2:
			return matchFloat32EqualAVX2(src, val, bits)
		default:*/
	return matchFloat32EqualGeneric(src, val, bits)
	//}
}

func matchFloat32NotEqual(src []float32, val float32, bits []byte) int64 {
	/*	switch {
		case useAVX512_F:
			return matchFloat32NotEqualAVX512(src, val, bits)
		case useAVX2:
			return matchFloat32NotEqualAVX2(src, val, bits)
		default:*/
	return matchFloat32NotEqualGeneric(src, val, bits)
	//}
}

func matchFloat32LessThan(src []float32, val float32, bits []byte) int64 {
	/*	switch {
		case useAVX512_F:
			return matchFloat32LessThanAVX512(src, val, bits)
		case useAVX2:
			return matchFloat32LessThanAVX2(src, val, bits)
		default:*/
	return matchFloat32LessThanGeneric(src, val, bits)
	//}
}

func matchFloat32LessThanEqual(src []float32, val float32, bits []byte) int64 {
	/*	switch {
		case useAVX512_F:
			return matchFloat32LessThanEqualAVX512(src, val, bits)
		case useAVX2:
			return matchFloat32LessThanEqualAVX2(src, val, bits)
		default:*/
	return matchFloat32LessThanEqualGeneric(src, val, bits)
	//}
}

func matchFloat32GreaterThan(src []float32, val float32, bits []byte) int64 {
	/*	switch {
		case useAVX512_F:
			return matchFloat32GreaterThanAVX512(src, val, bits)
		case useAVX2:
			return matchFloat32GreaterThanAVX2(src, val, bits)
		default:*/
	return matchFloat32GreaterThanGeneric(src, val, bits)
	//}
}

func matchFloat32GreaterThanEqual(src []float32, val float32, bits []byte) int64 {
	/*	switch {
		case useAVX512_F:
			return matchFloat32GreaterThanEqualAVX512(src, val, bits)
		case useAVX2:
			return matchFloat32GreaterThanEqualAVX2(src, val, bits)
		default:*/
	return matchFloat32GreaterThanEqualGeneric(src, val, bits)
	//}
}

func matchFloat32Between(src []float32, a, b float32, bits []byte) int64 {
	/*	switch {
		case useAVX512_F:
			return matchFloat32BetweenAVX512(src, a, b, bits)
		case useAVX2:
			return matchFloat32BetweenAVX2(src, a, b, bits)
		default:*/
	return matchFloat32BetweenGeneric(src, a, b, bits)
	//}
}
