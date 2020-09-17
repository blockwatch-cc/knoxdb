// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

// +build go1.7,amd64,!gccgo,!appengine

package vec

//go:noescape
func matchInt64EqualAVX2(src []int64, val int64, bits []byte) int64

//go:noescape
func matchInt64NotEqualAVX2(src []int64, val int64, bits []byte) int64

//go:noescape
func matchInt64LessThanAVX2(src []int64, val int64, bits []byte) int64

//go:noescape
func matchInt64LessThanEqualAVX2(src []int64, val int64, bits []byte) int64

//go:noescape
func matchInt64GreaterThanAVX2(src []int64, val int64, bits []byte) int64

//go:noescape
func matchInt64GreaterThanEqualAVX2(src []int64, val int64, bits []byte) int64

//go:noescape
func matchInt64BetweenAVX2(src []int64, a, b int64, bits []byte) int64

//go:noescape
func matchInt64EqualAVX512(src []int64, val int64, bits []byte) int64

//go:noescape
func matchInt64NotEqualAVX512(src []int64, val int64, bits []byte) int64

//go:noescape
func matchInt64LessThanAVX512(src []int64, val int64, bits []byte) int64

//go:noescape
func matchInt64LessThanEqualAVX512(src []int64, val int64, bits []byte) int64

//go:noescape
func matchInt64GreaterThanAVX512(src []int64, val int64, bits []byte) int64

//go:noescape
func matchInt64GreaterThanEqualAVX512(src []int64, val int64, bits []byte) int64

//go:noescape
func matchInt64BetweenAVX512(src []int64, a, b int64, bits []byte) int64

func matchInt64Equal(src []int64, val int64, bits []byte) int64 {
	switch {
	case useAVX2:
		return matchInt64EqualAVX2(src, val, bits)
	default:
		return matchInt64EqualGeneric(src, val, bits)
	}
}

func matchInt64NotEqual(src []int64, val int64, bits []byte) int64 {
	switch {
	case useAVX2:
		return matchInt64NotEqualAVX2(src, val, bits)
	default:
		return matchInt64NotEqualGeneric(src, val, bits)
	}
}

func matchInt64LessThan(src []int64, val int64, bits []byte) int64 {
	switch {
	case useAVX2:
		return matchInt64LessThanAVX2(src, val, bits)
	default:
		return matchInt64LessThanGeneric(src, val, bits)
	}
}

func matchInt64LessThanEqual(src []int64, val int64, bits []byte) int64 {
	switch {
	case useAVX2:
		return matchInt64LessThanEqualAVX2(src, val, bits)
	default:
		return matchInt64LessThanEqualGeneric(src, val, bits)
	}
}

func matchInt64GreaterThan(src []int64, val int64, bits []byte) int64 {
	switch {
	case useAVX2:
		return matchInt64GreaterThanAVX2(src, val, bits)
	default:
		return matchInt64GreaterThanGeneric(src, val, bits)
	}
}

func matchInt64GreaterThanEqual(src []int64, val int64, bits []byte) int64 {
	switch {
	case useAVX2:
		return matchInt64GreaterThanEqualAVX2(src, val, bits)
	default:
		return matchInt64GreaterThanEqualGeneric(src, val, bits)
	}
}

func matchInt64Between(src []int64, a, b int64, bits []byte) int64 {
	switch {
	case useAVX2:
		return matchInt64BetweenAVX2(src, a, b, bits)
	default:
		return matchInt64BetweenGeneric(src, a, b, bits)
	}
}
