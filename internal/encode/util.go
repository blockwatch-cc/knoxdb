// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package encode

import (
	"math/bits"
	"unsafe"

	"blockwatch.cc/knoxdb/internal/types"
)

var (
	BitLen64 = bits.Len64
)

func SizeOf[T types.Number]() int {
	return int(unsafe.Sizeof(T(0)))
}
