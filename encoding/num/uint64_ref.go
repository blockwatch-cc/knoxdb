// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

//go:build !amd64 || appengine || gccgo
// +build !amd64 appengine gccgo

package num

func matchUint64Equal(src []uint64, val uint64, bits []byte) int64 {
	return matchUint64EqualGeneric(src, val, bits)
}

func matchUint64NotEqual(src []uint64, val uint64, bits []byte) int64 {
	return matchUint64NotEqualGeneric(src, val, bits)
}

func matchUint64LessThan(src []uint64, val uint64, bits []byte) int64 {
	return matchUint64LessThanGeneric(src, val, bits)
}

func matchUint64LessThanEqual(src []uint64, val uint64, bits []byte) int64 {
	return matchUint64LessThanEqualGeneric(src, val, bits)
}

func matchUint64GreaterThan(src []uint64, val uint64, bits []byte) int64 {
	return matchUint64GreaterThanGeneric(src, val, bits)
}

func matchUint64GreaterThanEqual(src []uint64, val uint64, bits []byte) int64 {
	return matchUint64GreaterThanEqualGeneric(src, val, bits)
}

func matchUint64Between(src []uint64, a, b uint64, bits []byte) int64 {
	return matchUint64BetweenGeneric(src, a, b, bits)
}
