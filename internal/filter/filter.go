// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package filter

type Filter interface {
	Contains(uint64) bool
	ContainsAny([]uint64) bool
}
