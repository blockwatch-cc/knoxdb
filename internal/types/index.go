// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package types

import "fmt"

type IndexType byte

const (
	IndexTypeNone IndexType = iota
	IndexTypeHash
	IndexTypeInt
	IndexTypeComposite
	IndexTypeBloom
	IndexTypeBfuse
	IndexTypeBits
)

func (i IndexType) Is(f IndexType) bool {
	return i&f > 0
}

var (
	indexTypeString  = "__hash_int_composite_bloom_bfuse_bits"
	indexTypeIdx     = [...]int{0, 2, 7, 11, 21, 27, 33, 37}
	indexTypeReverse = map[string]IndexType{}
)

func init() {
	for t := IndexTypeNone; t <= IndexTypeBits; t++ {
		indexTypeReverse[t.String()] = t
	}
}

func (t IndexType) IsValid() bool {
	return t >= IndexTypeNone && t <= IndexTypeBits
}

func (t IndexType) String() string {
	return indexTypeString[indexTypeIdx[t] : indexTypeIdx[t+1]-1]
}

func ParseIndexType(s string) (IndexType, error) {
	t, ok := indexTypeReverse[s]
	if ok {
		return t, nil
	}
	return 0, fmt.Errorf("invalid index type %q", s)

}
