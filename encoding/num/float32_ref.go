// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

//go:build !amd64 || appengine || gccgo
// +build !amd64 appengine gccgo

package num

func matchFloat32Equal(src []float32, val float32, bits []byte) int64 {
    return matchFloat32EqualGeneric(src, val, bits)
}

func matchFloat32NotEqual(src []float32, val float32, bits []byte) int64 {
    return matchFloat32NotEqualGeneric(src, val, bits)
}

func matchFloat32LessThan(src []float32, val float32, bits []byte) int64 {
    return matchFloat32LessThanGeneric(src, val, bits)
}

func matchFloat32LessThanEqual(src []float32, val float32, bits []byte) int64 {
    return matchFloat32LessThanEqualGeneric(src, val, bits)
}

func matchFloat32GreaterThan(src []float32, val float32, bits []byte) int64 {
    return matchFloat32GreaterThanGeneric(src, val, bits)
}

func matchFloat32GreaterThanEqual(src []float32, val float32, bits []byte) int64 {
    return matchFloat32GreaterThanEqualGeneric(src, val, bits)
}

func matchFloat32Between(src []float32, a, b float32, bits []byte) int64 {
    return matchFloat32BetweenGeneric(src, a, b, bits)
}
