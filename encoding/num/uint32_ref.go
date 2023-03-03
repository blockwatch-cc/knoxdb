// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

//go:build !amd64 || appengine || gccgo
// +build !amd64 appengine gccgo

package num

func matchUint32Equal(src []uint32, val uint32, bits []byte) int64 {
    return matchUint32EqualGeneric(src, val, bits)
}

func matchUint32NotEqual(src []uint32, val uint32, bits []byte) int64 {
    return matchUint32NotEqualGeneric(src, val, bits)
}

func matchUint32LessThan(src []uint32, val uint32, bits []byte) int64 {
    return matchUint32LessThanGeneric(src, val, bits)
}

func matchUint32LessThanEqual(src []uint32, val uint32, bits []byte) int64 {
    return matchUint32LessThanEqualGeneric(src, val, bits)
}

func matchUint32GreaterThan(src []uint32, val uint32, bits []byte) int64 {
    return matchUint32GreaterThanGeneric(src, val, bits)
}

func matchUint32GreaterThanEqual(src []uint32, val uint32, bits []byte) int64 {
    return matchUint32GreaterThanEqualGeneric(src, val, bits)
}

func matchUint32Between(src []uint32, a, b uint32, bits []byte) int64 {
    return matchUint32BetweenGeneric(src, a, b, bits)
}
