// Copyright (c) 2025 Blockwatch Data Inc.
// Author: abdul@blockwatch.cc

package tests

import (
	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/util"
)

func decoder[T types.Integer](buf []byte, log2 int) DecodeIndex[T] {
	mask := uint64((1 << log2) - 1)
	bits := 64
	inBuff := util.FromByteSlice[uint64](buf)
	return func(index int) T {
		idx := index * log2
		codeword := idx >> 6
		shift := idx & 63
		pack := inBuff[codeword] >> shift
		if diff := bits - shift; diff < log2 && codeword+1 < len(inBuff) {
			pack |= inBuff[codeword+1] << diff
		}
		return T(pack & mask)
	}
}
