// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package bitmap

import (
	"blockwatch.cc/knoxdb/internal/xroar"
)

type Bitmap = xroar.Bitmap

var (
	New            = xroar.New
	NewFromBytes   = xroar.NewFromBytes
	NewFromIndexes = xroar.NewFromIndexes[uint64]
	NewFromSorted  = xroar.NewFromSorted[uint64]
	And            = xroar.And
	Or             = xroar.Or
	FastAnd        = xroar.FastAnd
	FastOr         = xroar.FastOr
	AndNot         = xroar.AndNot
)
