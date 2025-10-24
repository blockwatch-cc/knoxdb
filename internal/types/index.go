// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package types

import "fmt"

type IndexType byte

const (
	IndexTypeNone IndexType = iota
	IndexTypeHash
	IndexTypeInt
	IndexTypePk
	IndexTypeComposite
)

func (i IndexType) Is(f IndexType) bool {
	return i&f > 0
}

var (
	indexTypeString  = "__hash_int_pk_composite"
	indexTypeIdx     = [...]int{0, 2, 7, 11, 14, 24}
	indexTypeReverse = map[string]IndexType{}
)

func init() {
	for t := IndexTypeNone; t <= IndexTypeComposite; t++ {
		indexTypeReverse[t.String()] = t
	}
}

func (t IndexType) IsValid() bool {
	return t > IndexTypeNone && t <= IndexTypeComposite
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
