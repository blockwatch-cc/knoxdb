// Copyright (c) 2020 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

//go:build go1.7 && amd64 && !gccgo && !appengine
// +build go1.7,amd64,!gccgo,!appengine

package num

import (
	"blockwatch.cc/knoxdb/util"
)

//go:noescape
func matchUint8EqualAVX2(src []uint8, val uint8, bits []byte) int64

//go:noescape
func matchUint8NotEqualAVX2(src []uint8, val uint8, bits []byte) int64

//go:noescape
func matchUint8LessThanAVX2(src []uint8, val uint8, bits []byte) int64

//go:noescape
func matchUint8LessThanEqualAVX2(src []uint8, val uint8, bits []byte) int64

//go:noescape
func matchUint8GreaterThanAVX2(src []uint8, val uint8, bits []byte) int64

//go:noescape
func matchUint8GreaterThanEqualAVX2(src []uint8, val uint8, bits []byte) int64

//go:noescape
func matchUint8BetweenAVX2(src []uint8, a, b uint8, bits []byte) int64

//go:noescape
func matchUint8EqualAVX512(src []uint8, val uint8, bits []byte) int64

//go:noescape
func matchUint8NotEqualAVX512(src []uint8, val uint8, bits []byte) int64

//go:noescape
func matchUint8LessThanAVX512(src []uint8, val uint8, bits []byte) int64

//go:noescape
func matchUint8LessThanEqualAVX512(src []uint8, val uint8, bits []byte) int64

//go:noescape
func matchUint8GreaterThanAVX512(src []uint8, val uint8, bits []byte) int64

//go:noescape
func matchUint8GreaterThanEqualAVX512(src []uint8, val uint8, bits []byte) int64

//go:noescape
func matchUint8BetweenAVX512(src []uint8, a, b uint8, bits []byte) int64

func matchUint8Equal(src []uint8, val uint8, bits []byte) int64 {
	switch {
	case util.UseAVX512_BW:
		return matchUint8EqualAVX512(src, val, bits)
	case util.UseAVX2:
		return matchUint8EqualAVX2(src, val, bits)
	default:
		return matchUint8EqualGeneric(src, val, bits)
	}
}

func matchUint8NotEqual(src []uint8, val uint8, bits []byte) int64 {
	switch {
	case util.UseAVX512_BW:
		return matchUint8NotEqualAVX512(src, val, bits)
	case util.UseAVX2:
		return matchUint8NotEqualAVX2(src, val, bits)
	default:
		return matchUint8NotEqualGeneric(src, val, bits)
	}
}

func matchUint8LessThan(src []uint8, val uint8, bits []byte) int64 {
	switch {
	case util.UseAVX512_BW:
		return matchUint8LessThanAVX512(src, val, bits)
	case util.UseAVX2:
		return matchUint8LessThanAVX2(src, val, bits)
	default:
		return matchUint8LessThanGeneric(src, val, bits)
	}
}

func matchUint8LessThanEqual(src []uint8, val uint8, bits []byte) int64 {
	switch {
	case util.UseAVX512_BW:
		return matchUint8LessThanEqualAVX512(src, val, bits)
	case util.UseAVX2:
		return matchUint8LessThanEqualAVX2(src, val, bits)
	default:
		return matchUint8LessThanEqualGeneric(src, val, bits)
	}
}

func matchUint8GreaterThan(src []uint8, val uint8, bits []byte) int64 {
	switch {
	case util.UseAVX512_BW:
		return matchUint8GreaterThanAVX512(src, val, bits)
	case util.UseAVX2:
		return matchUint8GreaterThanAVX2(src, val, bits)
	default:
		return matchUint8GreaterThanGeneric(src, val, bits)
	}
}

func matchUint8GreaterThanEqual(src []uint8, val uint8, bits []byte) int64 {
	switch {
	case util.UseAVX512_BW:
		return matchUint8GreaterThanEqualAVX512(src, val, bits)
	case util.UseAVX2:
		return matchUint8GreaterThanEqualAVX2(src, val, bits)
	default:
		return matchUint8GreaterThanEqualGeneric(src, val, bits)
	}
}

func matchUint8Between(src []uint8, a, b uint8, bits []byte) int64 {
	switch {
	case util.UseAVX512_BW:
		return matchUint8BetweenAVX512(src, a, b, bits)
	case util.UseAVX2:
		return matchUint8BetweenAVX2(src, a, b, bits)
	default:
		return matchUint8BetweenGeneric(src, a, b, bits)
	}
}
