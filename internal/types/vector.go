// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package types

const (
	CHUNK_SIZE = 128
	CHUNK_MASK = CHUNK_SIZE - 1
)

func ChunkBase(n int) int {
	return n &^ CHUNK_MASK
}

func ChunkPos(n int) int {
	return n & CHUNK_MASK
}

func ToChunkSize(n int) int {
	return (n + CHUNK_MASK) &^ CHUNK_MASK
}

type Set[E any] interface {
	Set(E)
	Unset(E)
	Get(E) bool
	Contains(E) bool
	Clear()
}
