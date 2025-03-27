// Copyright (c) 2023 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package util

// The compiler optimizes a few patterns including this form.
// See issue 6011. https://tip.golang.org/src/cmd/compile/internal/ssa/phiopt.go
func Bool2byte(b bool) uint8 {
	if b {
		return 1
	}
	return 0
}

func Bool2int(b bool) int {
	if b {
		return 1
	}
	return 0
}
