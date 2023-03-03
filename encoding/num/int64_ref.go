// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

//go:build !amd64 || appengine || gccgo
// +build !amd64 appengine gccgo

package num

func matchInt64Equal(src []int64, val int64, bits []byte) int64 {
	return matchInt64EqualGeneric(src, val, bits)
}

func matchInt64NotEqual(src []int64, val int64, bits []byte) int64 {
	return matchInt64NotEqualGeneric(src, val, bits)
}

func matchInt64LessThan(src []int64, val int64, bits []byte) int64 {
	return matchInt64LessThanGeneric(src, val, bits)
}

func matchInt64LessThanEqual(src []int64, val int64, bits []byte) int64 {
	return matchInt64LessThanEqualGeneric(src, val, bits)
}

func matchInt64GreaterThan(src []int64, val int64, bits []byte) int64 {
	return matchInt64GreaterThanGeneric(src, val, bits)
}

func matchInt64GreaterThanEqual(src []int64, val int64, bits []byte) int64 {
	return matchInt64GreaterThanEqualGeneric(src, val, bits)
}

func matchInt64Between(src []int64, a, b int64, bits []byte) int64 {
	return matchInt64BetweenGeneric(src, a, b, bits)
}
