// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package generic

// Bool2Uint64 - compiler optimized to 1 opcode CSET
// See issue 6011. https://tip.golang.org/src/cmd/compile/internal/ssa/phiopt.go
func b2u64(b bool) uint64 {
	if b {
		return 1
	}
	return 0
}
