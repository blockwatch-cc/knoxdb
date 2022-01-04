// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

//go:build !amd64 || appengine || gccgo
// +build !amd64 appengine gccgo

package vec

func matchUint16Equal(src []uint16, val uint16, bits []byte) int64 {
    return matchUint16EqualGeneric(src, val, bits)
}

func matchUint16NotEqual(src []uint16, val uint16, bits []byte) int64 {
    return matchUint16NotEqualGeneric(src, val, bits)
}

func matchUint16LessThan(src []uint16, val uint16, bits []byte) int64 {
    return matchUint16LessThanGeneric(src, val, bits)
}

func matchUint16LessThanEqual(src []uint16, val uint16, bits []byte) int64 {
    return matchUint16LessThanEqualGeneric(src, val, bits)
}

func matchUint16GreaterThan(src []uint16, val uint16, bits []byte) int64 {
    return matchUint16GreaterThanGeneric(src, val, bits)
}

func matchUint16GreaterThanEqual(src []uint16, val uint16, bits []byte) int64 {
    return matchUint16GreaterThanEqualGeneric(src, val, bits)
}

func matchUint16Between(src []uint16, a, b uint16, bits []byte) int64 {
    return matchUint16BetweenGeneric(src, a, b, bits)
}
