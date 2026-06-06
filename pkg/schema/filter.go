// Copyright (c) 2025-2026 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package schema

import "errors"

type FilterType byte

const (
	NoFilter FilterType = iota
	BitsFilter
	BloomFilter2b
	BloomFilter3b
	BloomFilter4b
	BloomFilter5b
	BinaryFuseFilter8
	BinaryFuseFilter16
)

func (i FilterType) Is(f FilterType) bool {
	return i&f > 0
}

var (
	filterTypeString  = "__bits_bloom2b_bloom3b_bloom4b_bloom5b_bfuse8_bfuse16"
	filterTypeIdx     = [...]int8{0, 1, 7, 15, 23, 31, 39, 46, 54}
	filterTypeReverse = map[string]FilterType{}
)

func init() {
	for t := NoFilter; t <= BinaryFuseFilter16; t++ {
		filterTypeReverse[t.String()] = t
	}
}

func (t FilterType) IsValid() bool {
	return t <= BinaryFuseFilter16
}

func (t FilterType) String() string {
	return filterTypeString[filterTypeIdx[t] : filterTypeIdx[t+1]-1]
}

func ParseFilterType(s string) (FilterType, error) {
	t, ok := filterTypeReverse[s]
	if ok {
		return t, nil
	}
	return 0, errors.New("invalid filter type " + s)

}

func (f FilterType) Factor() int {
	switch f {
	case BloomFilter2b:
		return 2
	case BloomFilter3b:
		return 3
	case BloomFilter4b:
		return 4
	case BloomFilter5b:
		return 5
	case BinaryFuseFilter8:
		return 8
	case BinaryFuseFilter16:
		return 16
	default:
		return 1
	}
}
