// Copyright (c) 2025 Blockwatch Data Inc.
// Author: abdul@blockwatch.cc

package tests

import (
	"unsafe"

	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/util"
)

var (
	ShiftAmount = [8]int{3, 4, 0, 5, 0, 0, 0, 6}
)

func decoder[T types.Integer](buf []byte, log2 int) DecodeIndex[T] {
	mask := uint64((1 << log2) - 1)
	bits := int(unsafe.Sizeof(T(0)) * 8)
	inBuff := util.FromByteSlice[T](buf)

	return func(index int) T {
		idx := index * log2
		codeword := idx >> ShiftAmount[bits>>3-1]

		shift := idx & (1<<bits - 1)
		if shift > bits {
			shift = shift - (codeword * bits)
		}
		pack := uint64(inBuff[codeword]) >> shift

		if diff := bits - shift; diff < log2 {
			pack |= uint64(inBuff[codeword+1]) << diff
		}

		pack &= mask

		return T(pack & mask)
	}
}
