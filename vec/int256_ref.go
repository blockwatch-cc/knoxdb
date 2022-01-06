// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

//go:build !amd64 || appengine || gccgo
// +build !amd64 appengine gccgo

package vec

func matchInt256Equal(src Int256LLSlice, val Int256, bits, mask []byte) int64 {
	return matchInt256EqualGeneric(src, val, bits, mask)
}

func matchInt256NotEqual(src Int256LLSlice, val Int256, bits, mask []byte) int64 {
	return matchInt256NotEqualGeneric(src, val, bits, mask)
}

func matchInt256LessThan(src Int256LLSlice, val Int256, bits, mask []byte) int64 {
	return matchInt256LessThanGeneric(src, val, bits, mask)
}

func matchInt256LessThanEqual(src Int256LLSlice, val Int256, bits, mask []byte) int64 {
	return matchInt256LessThanEqualGeneric(src, val, bits, mask)
}

func matchInt256GreaterThan(src Int256LLSlice, val Int256, bits, mask []byte) int64 {
	return matchInt256GreaterThanGeneric(src, val, bits, mask)
}

func matchInt256GreaterThanEqual(src Int256LLSlice, val Int256, bits, mask []byte) int64 {
	return matchInt256GreaterThanEqualGeneric(src, val, bits, mask)
}

func matchInt256Between(src Int256LLSlice, a, b Int256, bits, mask []byte) int64 {
	return matchInt256BetweenGeneric(src, a, b, bits, mask)
}
