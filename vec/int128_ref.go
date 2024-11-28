// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

//go:build !amd64 || appengine || gccgo
// +build !amd64 appengine gccgo

package vec

func matchInt128Equal(src Int128LLSlice, val Int128, bits, mask []byte) int64 {
	return matchInt128EqualGeneric(src, val, bits, mask)
}

func matchInt128NotEqual(src Int128LLSlice, val Int128, bits, mask []byte) int64 {
	return matchInt128NotEqualGeneric(src, val, bits, mask)
}

func matchInt128LessThan(src Int128LLSlice, val Int128, bits, mask []byte) int64 {
	return matchInt128LessThanGeneric(src, val, bits, mask)
}

func matchInt128LessThanEqual(src Int128LLSlice, val Int128, bits, mask []byte) int64 {
	return matchInt128LessThanEqualGeneric(src, val, bits, mask)
}

func matchInt128GreaterThan(src Int128LLSlice, val Int128, bits, mask []byte) int64 {
	return matchInt128GreaterThanGeneric(src, val, bits, mask)
}

func matchInt128GreaterThanEqual(src Int128LLSlice, val Int128, bits, mask []byte) int64 {
	return matchInt128GreaterThanEqualGeneric(src, val, bits, mask)
}

func matchInt128Between(src Int128LLSlice, a, b Int128, bits, mask []byte) int64 {
	return matchInt128BetweenGeneric(src, a, b, bits, mask)
}
