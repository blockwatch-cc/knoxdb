// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

//go:build !amd64 || appengine || gccgo
// +build !amd64 appengine gccgo

package vec

func matchInt16Equal(src []int16, val int16, bits []byte) int64 {
    return matchInt16EqualGeneric(src, val, bits)
}

func matchInt16NotEqual(src []int16, val int16, bits []byte) int64 {
    return matchInt16NotEqualGeneric(src, val, bits)
}

func matchInt16LessThan(src []int16, val int16, bits []byte) int64 {
    return matchInt16LessThanGeneric(src, val, bits)
}

func matchInt16LessThanEqual(src []int16, val int16, bits []byte) int64 {
    return matchInt16LessThanEqualGeneric(src, val, bits)
}

func matchInt16GreaterThan(src []int16, val int16, bits []byte) int64 {
    return matchInt16GreaterThanGeneric(src, val, bits)
}

func matchInt16GreaterThanEqual(src []int16, val int16, bits []byte) int64 {
    return matchInt16GreaterThanEqualGeneric(src, val, bits)
}

func matchInt16Between(src []int16, a, b int16, bits []byte) int64 {
    return matchInt16BetweenGeneric(src, a, b, bits)
}
