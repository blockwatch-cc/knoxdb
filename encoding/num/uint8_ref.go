// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

//go:build !amd64 || appengine || gccgo
// +build !amd64 appengine gccgo

package num

func matchUint8Equal(src []uint8, val uint8, bits []byte) int64 {
    return matchUint8EqualGeneric(src, val, bits)
}

func matchUint8NotEqual(src []uint8, val uint8, bits []byte) int64 {
    return matchUint8NotEqualGeneric(src, val, bits)
}

func matchUint8LessThan(src []uint8, val uint8, bits []byte) int64 {
    return matchUint8LessThanGeneric(src, val, bits)
}

func matchUint8LessThanEqual(src []uint8, val uint8, bits []byte) int64 {
    return matchUint8LessThanEqualGeneric(src, val, bits)
}

func matchUint8GreaterThan(src []uint8, val uint8, bits []byte) int64 {
    return matchUint8GreaterThanGeneric(src, val, bits)
}

func matchUint8GreaterThanEqual(src []uint8, val uint8, bits []byte) int64 {
    return matchUint8GreaterThanEqualGeneric(src, val, bits)
}

func matchUint8Between(src []uint8, a, b uint8, bits []byte) int64 {
    return matchUint8BetweenGeneric(src, a, b, bits)
}
