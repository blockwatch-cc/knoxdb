// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

//go:build !amd64
// +build !amd64

package hashprobe

import "blockwatch.cc/knoxdb/internal/types"

func buildDictAVX2[T types.Integer](vals []T, numUnique int) ([]T, []uint16) {
	return buildDictGeneric(vals, numUnique)
}
