// Copyright (c) 2020 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

//go:build go1.7 && amd64 && !gccgo && !appengine
// +build go1.7,amd64,!gccgo,!appengine

package vec

import (
	"blockwatch.cc/knoxdb/util"
)

//go:noescape
func matchInt16EqualAVX2(src []int16, val int16, bits []byte) int64

//go:noescape
func matchInt16NotEqualAVX2(src []int16, val int16, bits []byte) int64

//go:noescape
func matchInt16LessThanAVX2(src []int16, val int16, bits []byte) int64

//go:noescape
func matchInt16LessThanEqualAVX2(src []int16, val int16, bits []byte) int64

//go:noescape
func matchInt16GreaterThanAVX2(src []int16, val int16, bits []byte) int64

//go:noescape
func matchInt16GreaterThanEqualAVX2(src []int16, val int16, bits []byte) int64

//go:noescape
func matchInt16BetweenAVX2(src []int16, a, b int16, bits []byte) int64

//go:noescape
func matchInt16EqualAVX512(src []int16, val int16, bits []byte) int64

//go:noescape
func matchInt16NotEqualAVX512(src []int16, val int16, bits []byte) int64

//go:noescape
func matchInt16LessThanAVX512(src []int16, val int16, bits []byte) int64

//go:noescape
func matchInt16LessThanEqualAVX512(src []int16, val int16, bits []byte) int64

//go:noescape
func matchInt16GreaterThanAVX512(src []int16, val int16, bits []byte) int64

//go:noescape
func matchInt16GreaterThanEqualAVX512(src []int16, val int16, bits []byte) int64

//go:noescape
func matchInt16BetweenAVX512(src []int16, a, b int16, bits []byte) int64

func matchInt16Equal(src []int16, val int16, bits []byte) int64 {
	switch {
	case util.UseAVX512_BW:
		return matchInt16EqualAVX512(src, val, bits)
	case util.UseAVX2:
		return matchInt16EqualAVX2(src, val, bits)
	default:
		return matchInt16EqualGeneric(src, val, bits)
	}
}

func matchInt16NotEqual(src []int16, val int16, bits []byte) int64 {
	switch {
	case util.UseAVX512_BW:
		return matchInt16NotEqualAVX512(src, val, bits)
	case util.UseAVX2:
		return matchInt16NotEqualAVX2(src, val, bits)
	default:
		return matchInt16NotEqualGeneric(src, val, bits)
	}
}

func matchInt16LessThan(src []int16, val int16, bits []byte) int64 {
	switch {
	case util.UseAVX512_BW:
		return matchInt16LessThanAVX512(src, val, bits)
	case util.UseAVX2:
		return matchInt16LessThanAVX2(src, val, bits)
	default:
		return matchInt16LessThanGeneric(src, val, bits)
	}
}

func matchInt16LessThanEqual(src []int16, val int16, bits []byte) int64 {
	switch {
	case util.UseAVX512_BW:
		return matchInt16LessThanEqualAVX512(src, val, bits)
	case util.UseAVX2:
		return matchInt16LessThanEqualAVX2(src, val, bits)
	default:
		return matchInt16LessThanEqualGeneric(src, val, bits)
	}
}

func matchInt16GreaterThan(src []int16, val int16, bits []byte) int64 {
	switch {
	case util.UseAVX512_BW:
		return matchInt16GreaterThanAVX512(src, val, bits)
	case util.UseAVX2:
		return matchInt16GreaterThanAVX2(src, val, bits)
	default:
		return matchInt16GreaterThanGeneric(src, val, bits)
	}
}

func matchInt16GreaterThanEqual(src []int16, val int16, bits []byte) int64 {
	switch {
	case util.UseAVX512_BW:
		return matchInt16GreaterThanEqualAVX512(src, val, bits)
	case util.UseAVX2:
		return matchInt16GreaterThanEqualAVX2(src, val, bits)
	default:
		return matchInt16GreaterThanEqualGeneric(src, val, bits)
	}
}

func matchInt16Between(src []int16, a, b int16, bits []byte) int64 {
	switch {
	case util.UseAVX512_BW:
		return matchInt16BetweenAVX512(src, a, b, bits)
	case util.UseAVX2:
		return matchInt16BetweenAVX2(src, a, b, bits)
	default:
		return matchInt16BetweenGeneric(src, a, b, bits)
	}
}
