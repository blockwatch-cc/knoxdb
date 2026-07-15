// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package bitpack

import (
	"encoding/binary"
	"math/bits"

	"blockwatch.cc/knoxdb/internal/bitset"
	"blockwatch.cc/knoxdb/internal/types"
)

type Bitset = bitset.Bitset

var (
	BE = binary.BigEndian
)

func log2Range[T types.Integer](minv, maxv T) int {
	isSigned := T(0)-T(1) < T(0)
	if isSigned {
		return bits.Len64(uint64(int64(maxv) - int64(minv)))
	} else {
		return bits.Len64(uint64(maxv - minv))
	}
}

// Bool2Uint64 - compiler optimized to 1 opcode CSET
// See issue 6011. https://tip.golang.org/src/cmd/compile/internal/ssa/phiopt.go
func b2u64(b bool) uint64 {
	if b {
		return 1
	}
	return 0
}
