// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

// +build !amd64 appengine gccgo

package vec

func bitsetAnd(dst, src []byte, size int) int {
	return bitsetAndGeneric(dst, src, size)
}

func bitsetAndNot(dst, src []byte, size int) {
	bitsetAndNotGeneric(dst, src, size)
}

func bitsetOr(dst, src []byte, size int) {
	bitsetOrGeneric(dst, src, size)
}

func bitsetXor(dst, src []byte, size int) {
	bitsetXorGeneric(dst, src, size)
}

func bitsetNeg(src []byte, size int) {
	bitsetNegGeneric(src, size)
}

func bitsetReverse(src []byte) {
	return bitsetReverseGeneric(src, index, size)
}

func bitsetPopCount(src []byte, size int) int64 {
	return bitsetPopCountGeneric(src, size)
}

func bitsetRun(src []byte, index, size int) (int, int) {
	return bitsetRunGeneric(src, index, size)
}

func bitsetIndexes(src []byte, size int, dst []uint32) int {
	return bitsetIndexesGenericSkip64(src, size, dst)
}
