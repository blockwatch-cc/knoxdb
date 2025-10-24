// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package types

import "fmt"

type FilterType byte

const (
	FilterTypeNone FilterType = iota
	FilterTypeBits
	FilterTypeBloom2b
	FilterTypeBloom3b
	FilterTypeBloom4b
	FilterTypeBloom5b
	FilterTypeBfuse8
	FilterTypeBfuse16
)

func (i FilterType) Is(f FilterType) bool {
	return i&f > 0
}

var (
	filterTypeString  = "__bits_bloom2b_bloom3b_bloom4b_bloom5b_bfuse8_bfuse16"
	filterTypeIdx     = [...]int{0, 2, 7, 15, 23, 31, 39, 46, 54}
	filterTypeReverse = map[string]FilterType{}
)

func init() {
	for t := FilterTypeNone; t <= FilterTypeBfuse16; t++ {
		filterTypeReverse[t.String()] = t
	}
}

func (t FilterType) IsValid() bool {
	return t >= FilterTypeNone && t <= FilterTypeBfuse16
}

func (t FilterType) String() string {
	return filterTypeString[filterTypeIdx[t] : filterTypeIdx[t+1]-1]
}

func ParseFilterType(s string) (FilterType, error) {
	t, ok := filterTypeReverse[s]
	if ok {
		return t, nil
	}
	return 0, fmt.Errorf("invalid filter type %q", s)

}

func (f FilterType) Factor() int {
	switch f {
	case FilterTypeBloom2b:
		return 2
	case FilterTypeBloom3b:
		return 3
	case FilterTypeBloom4b:
		return 4
	case FilterTypeBloom5b:
		return 5
	case FilterTypeBfuse8:
		return 8
	case FilterTypeBfuse16:
		return 16
	default:
		return 1
	}
}
