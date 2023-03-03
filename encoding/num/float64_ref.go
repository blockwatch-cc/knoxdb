// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

//go:build !amd64 || appengine || gccgo
// +build !amd64 appengine gccgo

package num

func matchFloat64Equal(src []float64, val float64, bits []byte) int64 {
	return matchFloat64EqualGeneric(src, val, bits)
}

func matchFloat64NotEqual(src []float64, val float64, bits []byte) int64 {
	return matchFloat64NotEqualGeneric(src, val, bits)
}

func matchFloat64LessThan(src []float64, val float64, bits []byte) int64 {
	return matchFloat64LessThanGeneric(src, val, bits)
}

func matchFloat64LessThanEqual(src []float64, val float64, bits []byte) int64 {
	return matchFloat64LessThanEqualGeneric(src, val, bits)
}

func matchFloat64GreaterThan(src []float64, val float64, bits []byte) int64 {
	return matchFloat64GreaterThanGeneric(src, val, bits)
}

func matchFloat64GreaterThanEqual(src []float64, val float64, bits []byte) int64 {
	return matchFloat64GreaterThanEqualGeneric(src, val, bits)
}

func matchFloat64Between(src []float64, a, b float64, bits []byte) int64 {
	return matchFloat64BetweenGeneric(src, a, b, bits)
}
