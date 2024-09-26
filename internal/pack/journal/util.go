// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package journal

const sizeStep int = 1 << 8 // 256

// RoundSize rounds size up to a multiple of sizeStep
func roundSize(sz int) int {
    return (sz + (sizeStep - 1)) & ^(sizeStep - 1)
}
