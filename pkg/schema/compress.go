// Copyright (c) 2024-2026 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package schema

type Compression byte

const (
	Uncompressed Compression = iota
	Snappy
	LZ4
	Zstd
)

func (i Compression) Is(f Compression) bool {
	return i&f > 0
}

var (
	blockCompressNames    = "__snappy_lz4_zstd"
	blockCompressNamesOfs = [...]int8{0, 2, 7, 13, 18}
)

func (t Compression) String() string {
	return blockCompressNames[blockCompressNamesOfs[t] : blockCompressNamesOfs[t+1]-1]
}
