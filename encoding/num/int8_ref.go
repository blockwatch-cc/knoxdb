// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

//go:build !amd64 || appengine || gccgo
// +build !amd64 appengine gccgo

package num

func matchInt8Equal(src []int8, val int8, bits []byte) int64 {
    return matchInt8EqualGeneric(src, val, bits)
}

func matchInt8NotEqual(src []int8, val int8, bits []byte) int64 {
    return matchInt8NotEqualGeneric(src, val, bits)
}

func matchInt8LessThan(src []int8, val int8, bits []byte) int64 {
    return matchInt8LessThanGeneric(src, val, bits)
}

func matchInt8LessThanEqual(src []int8, val int8, bits []byte) int64 {
    return matchInt8LessThanEqualGeneric(src, val, bits)
}

func matchInt8GreaterThan(src []int8, val int8, bits []byte) int64 {
    return matchInt8GreaterThanGeneric(src, val, bits)
}

func matchInt8GreaterThanEqual(src []int8, val int8, bits []byte) int64 {
    return matchInt8GreaterThanEqualGeneric(src, val, bits)
}

func matchInt8Between(src []int8, a, b int8, bits []byte) int64 {
    return matchInt8BetweenGeneric(src, a, b, bits)
}
