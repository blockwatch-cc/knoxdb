// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

//go:build !amd64 || appengine || gccgo
// +build !amd64 appengine gccgo

package vec

func matchInt32Equal(src []int32, val int32, bits []byte) int64 {
    return matchInt32EqualGeneric(src, val, bits)
}

func matchInt32NotEqual(src []int32, val int32, bits []byte) int64 {
    return matchInt32NotEqualGeneric(src, val, bits)
}

func matchInt32LessThan(src []int32, val int32, bits []byte) int64 {
    return matchInt32LessThanGeneric(src, val, bits)
}

func matchInt32LessThanEqual(src []int32, val int32, bits []byte) int64 {
    return matchInt32LessThanEqualGeneric(src, val, bits)
}

func matchInt32GreaterThan(src []int32, val int32, bits []byte) int64 {
    return matchInt32GreaterThanGeneric(src, val, bits)
}

func matchInt32GreaterThanEqual(src []int32, val int32, bits []byte) int64 {
    return matchInt32GreaterThanEqualGeneric(src, val, bits)
}

func matchInt32Between(src []int32, a, b int32, bits []byte) int64 {
    return matchInt32BetweenGeneric(src, a, b, bits)
}
