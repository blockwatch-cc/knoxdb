// Copyright (c) 2023 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package bitset

import (
	"blockwatch.cc/knoxdb/internal/bitset/avx2"
	"blockwatch.cc/knoxdb/internal/bitset/generic"
	"blockwatch.cc/knoxdb/pkg/util"
)

var (
	bitsetAnd      = generic.And
	bitsetAndFlag  = generic.AndFlag
	bitsetAndNot   = generic.AndNot
	bitsetOr       = generic.Or
	bitsetOrFlag   = generic.OrFlag
	bitsetXor      = generic.Xor
	bitsetNeg      = generic.Neg
	bitsetReverse  = generic.Reverse
	bitsetPopCount = generic.PopCount
	bitsetRun      = generic.Run
	bitsetIndexes  = generic.Indexes

	bitFieldLen   = generic.BitFieldLen
	bytemask      = generic.Bytemask
	bitmask       = generic.Bitmask
	reverseLut256 = generic.ReverseLut256
	roundUpPow2   = generic.RoundUpPow2
)

func init() {
	if util.UseAVX2 {
		bitsetAnd = avx2.And
		bitsetAndFlag = avx2.AndFlag
		bitsetAndNot = avx2.AndNot
		bitsetOr = avx2.Or
		bitsetOrFlag = avx2.OrFlag
		bitsetXor = avx2.Xor
		bitsetNeg = avx2.Neg
		bitsetReverse = avx2.Reverse
		bitsetPopCount = avx2.PopCount
		bitsetRun = avx2.Run
		bitsetIndexes = avx2.Indexes
	}
}
