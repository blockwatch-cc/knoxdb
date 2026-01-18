// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package hash

import (
	"blockwatch.cc/knoxdb/internal/hash/xxhash"
	"blockwatch.cc/knoxdb/internal/hash/xxhash64"
)

type Hasher interface {
	Sum64() uint64
	Write([]byte) (int, error)
	WriteString(string) (int, error)
}

func Sum64(buf []byte) uint64 {
	return xxhash.Sum64(buf)
}

func HashUint32(v uint32) uint64 {
	return xxhash.Hash64u32(v)
}

func HashUint64(v uint64) uint64 {
	return xxhash.Hash64u64(v)
}

func New() Hasher {
	return xxhash64.New()
}
