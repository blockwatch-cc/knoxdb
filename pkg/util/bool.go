// Copyright (c) 2023 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package util

// The compiler optimizes a few patterns including this form.
// See issue 6011.
func Bool2int(b bool) int {
	if b {
		return 1
	}
	return 0
}

func Bool2byte(b bool) byte {
	if b {
		return 1
	}
	return 0
}
