// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

// +build go1.7,amd64,!gccgo,!appengine

package vec

//go:noescape
func matchInt256EqualAVX2New(src0 []int64, src1, src2, src3 []uint64, val Int256, bits []byte) int64

// //go:noescape
// func matchInt256NotEqualAVX2(src []Int256, val Int256, bits []byte) int64

//go:noescape
func matchInt256LessThanAVX2New(src0 []int64, src1, src2, src3 []uint64, val Int256, bits []byte) int64

// //go:noescape
// func matchInt256LessThanEqualAVX2(src []Int256, val Int256, bits []byte) int64

// //go:noescape
// func matchInt256GreaterThanAVX2(src []Int256, val Int256, bits []byte) int64

// //go:noescape
// func matchInt256GreaterThanEqualAVX2(src []Int256, val Int256, bits []byte) int64

// //go:noescape
// func matchInt256BetweenAVX2(src []Int256, a, b Int256, bits []byte) int64

// //go:noescape
// func matchInt256EqualAVX512(src []Int256, val Int256, bits []byte) int64

// //go:noescape
// func matchInt256NotEqualAVX512(src []Int256, val Int256, bits []byte) int64

// //go:noescape
// func matchInt256LessThanAVX512(src []Int256, val Int256, bits []byte) int64

// //go:noescape
// func matchInt256LessThanEqualAVX512(src []Int256, val Int256, bits []byte) int64

// //go:noescape
// func matchInt256GreaterThanAVX512(src []Int256, val Int256, bits []byte) int64

// //go:noescape
// func matchInt256GreaterThanEqualAVX512(src []Int256, val Int256, bits []byte) int64

// //go:noescape
// func matchInt256BetweenAVX512(src []Int256, a, b Int256, bits []byte) int64

func matchInt256Equal(src []Int256, val Int256, bits, mask []byte) int64 {
	switch {
	// case useAVX512_F:
	// 	return matchInt256EqualAVX512(src, val, bits)
	// case useAVX2:
	// 	return matchInt256EqualAVX2(src, val, bits)
	default:
		return matchInt256EqualGeneric(src, val, bits, mask)
	}
}

func matchInt256NotEqual(src []Int256, val Int256, bits, mask []byte) int64 {
	switch {
	// case useAVX512_F:
	// 	return matchInt256NotEqualAVX512(src, val, bits)
	// case useAVX2:
	// 	return matchInt256NotEqualAVX2(src, val, bits)
	default:
		return matchInt256NotEqualGeneric(src, val, bits, mask)
	}
}

func matchInt256LessThan(src []Int256, val Int256, bits, mask []byte) int64 {
	switch {
	// case useAVX512_F:
	// 	return matchInt256LessThanAVX512(src, val, bits)
	// case useAVX2:
	// 	return matchInt256LessThanAVX2(src, val, bits)
	default:
		return matchInt256LessThanGeneric(src, val, bits, mask)
	}
}

func matchInt256LessThanEqual(src []Int256, val Int256, bits, mask []byte) int64 {
	switch {
	// case useAVX512_F:
	// 	return matchInt256LessThanEqualAVX512(src, val, bits)
	// case useAVX2:
	// 	return matchInt256LessThanEqualAVX2(src, val, bits)
	default:
		return matchInt256LessThanEqualGeneric(src, val, bits, mask)
	}
}

func matchInt256GreaterThan(src []Int256, val Int256, bits, mask []byte) int64 {
	switch {
	// case useAVX512_F:
	// 	return matchInt256GreaterThanAVX512(src, val, bits)
	// case useAVX2:
	// 	return matchInt256GreaterThanAVX2(src, val, bits)
	default:
		return matchInt256GreaterThanGeneric(src, val, bits, mask)
	}
}

func matchInt256GreaterThanEqual(src []Int256, val Int256, bits, mask []byte) int64 {
	switch {
	// case useAVX512_F:
	// 	return matchInt256GreaterThanEqualAVX512(src, val, bits)
	// case useAVX2:
	// 	return matchInt256GreaterThanEqualAVX2(src, val, bits)
	default:
		return matchInt256GreaterThanEqualGeneric(src, val, bits, mask)
	}
}

func matchInt256Between(src []Int256, a, b Int256, bits, mask []byte) int64 {
	switch {
	// case useAVX512_F:
	// 	return matchInt256BetweenAVX512(src, a, b, bits)
	// case useAVX2:
	// 	return matchInt256BetweenAVX2(src, a, b, bits)
	default:
		return matchInt256BetweenGeneric(src, a, b, bits, mask)
	}
}

func matchInt256EqualAVX2Easy(src0 []int64, src1, src2, src3 []uint64, val Int256, bits []byte) int64 {
	tmp := make([]byte, len(bits))
	matchInt64EqualAVX2(src0, int64(val[0]), bits)
	matchUint64EqualAVX2(src1, val[1], tmp)
	bitsetAndAVX2(bits, tmp)
	matchUint64EqualAVX2(src2, val[2], tmp)
	bitsetAndAVX2(bits, tmp)
	matchUint64EqualAVX2(src3, val[3], tmp)
	bitsetAndAVX2(bits, tmp)
	return bitsetPopCountAVX2(bits)
}

func matchInt256LessThanAVX2Easy(src0 []int64, src1, src2, src3 []uint64, val Int256, bits []byte) int64 {
	tmp0 := make([]byte, len(bits))
	tmp1 := make([]byte, len(bits))
	matchUint64LessThanAVX2(src3, val[3], bits)
	matchUint64EqualAVX2(src2, val[2], tmp0)
	matchUint64LessThanAVX2(src2, val[2], tmp1)
	bitsetAndAVX2(bits, tmp0)
	bitsetOrAVX2(bits, tmp1)
	matchUint64EqualAVX2(src1, val[1], tmp0)
	matchUint64LessThanAVX2(src1, val[1], tmp1)
	bitsetAndAVX2(bits, tmp0)
	bitsetOrAVX2(bits, tmp1)
	matchInt64EqualAVX2(src0, int64(val[0]), tmp0)
	matchInt64LessThanAVX2(src0, int64(val[0]), tmp1)
	bitsetAndAVX2(bits, tmp0)
	bitsetOrAVX2(bits, tmp1)
	return bitsetPopCountAVX2(bits)
}
