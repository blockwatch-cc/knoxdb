// Copyright (c) 2025 Blockwatch Data Inc.
// Author: abdul@blockwatch.cc

package fsst

func isEscapeCode(pos uint64) bool {
	return pos < FSST_CODE_BASE
}

func FSSTHash(v uint64) uint64 {
	return (v * FSST_HASH_PRIME) ^ ((v * FSST_HASH_PRIME) >> FSST_SHIFT)
}
