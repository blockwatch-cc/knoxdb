// Copyright (c) 2020 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

// +build go1.7,amd64,!gccgo,!appengine

package vec

//go:noescape
func matchInt32EqualAVX2(src []int32, val int32, bits []byte) int64

//go:noescape
func matchInt32NotEqualAVX2(src []int32, val int32, bits []byte) int64

//go:noescape
func matchInt32LessThanAVX2(src []int32, val int32, bits []byte) int64

//go:noescape
func matchInt32LessThanEqualAVX2(src []int32, val int32, bits []byte) int64

//go:noescape
func matchInt32GreaterThanAVX2(src []int32, val int32, bits []byte) int64

//go:noescape
func matchInt32GreaterThanEqualAVX2(src []int32, val int32, bits []byte) int64

//go:noescape
func matchInt32BetweenAVX2(src []int32, a, b int32, bits []byte) int64

//go:noescape
func matchInt32EqualAVX512(src []int32, val int32, bits []byte) int64

//go:noescape
func matchInt32NotEqualAVX512(src []int32, val int32, bits []byte) int64

//go:noescape
func matchInt32LessThanAVX512(src []int32, val int32, bits []byte) int64

//go:noescape
func matchInt32LessThanEqualAVX512(src []int32, val int32, bits []byte) int64

//go:noescape
func matchInt32GreaterThanAVX512(src []int32, val int32, bits []byte) int64

//go:noescape
func matchInt32GreaterThanEqualAVX512(src []int32, val int32, bits []byte) int64

//go:noescape
func matchInt32BetweenAVX512(src []int32, a, b int32, bits []byte) int64

func matchInt32Equal(src []int32, val int32, bits []byte) int64 {
	switch {
	case useAVX512_F:
		return matchInt32EqualAVX512(src, val, bits)
	case useAVX2:
		return matchInt32EqualAVX2(src, val, bits)
	default:
		return matchInt32EqualGeneric(src, val, bits)
	}
}

func matchInt32NotEqual(src []int32, val int32, bits []byte) int64 {
	switch {
	case useAVX512_F:
		return matchInt32NotEqualAVX512(src, val, bits)
	case useAVX2:
		return matchInt32NotEqualAVX2(src, val, bits)
	default:
		return matchInt32NotEqualGeneric(src, val, bits)
	}
}

func matchInt32LessThan(src []int32, val int32, bits []byte) int64 {
	switch {
	case useAVX512_F:
		return matchInt32LessThanAVX512(src, val, bits)
	case useAVX2:
		return matchInt32LessThanAVX2(src, val, bits)
	default:
		return matchInt32LessThanGeneric(src, val, bits)
	}
}

func matchInt32LessThanEqual(src []int32, val int32, bits []byte) int64 {
	switch {
	case useAVX512_F:
		return matchInt32LessThanEqualAVX512(src, val, bits)
	case useAVX2:
		return matchInt32LessThanEqualAVX2(src, val, bits)
	default:
		return matchInt32LessThanEqualGeneric(src, val, bits)
	}
}

func matchInt32GreaterThan(src []int32, val int32, bits []byte) int64 {
	switch {
	case useAVX512_F:
		return matchInt32GreaterThanAVX512(src, val, bits)
	case useAVX2:
		return matchInt32GreaterThanAVX2(src, val, bits)
	default:
		return matchInt32GreaterThanGeneric(src, val, bits)
	}
}

func matchInt32GreaterThanEqual(src []int32, val int32, bits []byte) int64 {
	switch {
	case useAVX512_F:
		return matchInt32GreaterThanEqualAVX512(src, val, bits)
	case useAVX2:
		return matchInt32GreaterThanEqualAVX2(src, val, bits)
	default:
		return matchInt32GreaterThanEqualGeneric(src, val, bits)
	}
}

func matchInt32Between(src []int32, a, b int32, bits []byte) int64 {
	switch {
	case useAVX512_F:
		return matchInt32BetweenAVX512(src, a, b, bits)
	case useAVX2:
		return matchInt32BetweenAVX2(src, a, b, bits)
	default:
		return matchInt32BetweenGeneric(src, a, b, bits)
	}
}
