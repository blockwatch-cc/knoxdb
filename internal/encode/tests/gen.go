// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package tests

import (
	"blockwatch.cc/knoxdb/internal/tests"
	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/util"
)

func GenForIntScheme[T types.Integer](scheme, n int) []T {
	switch scheme {
	case 0: // TIntegerConstant,
		return tests.GenConst[T](n, 42)
	case 1: // TIntegerDelta,
		return tests.GenSeq[T](n, 0)
	case 2: // TIntegerRunEnd,
		return tests.GenRuns[T](n, min(n, 5), -1)
	case 3: // TIntegerBitpacked,
		return tests.GenRnd[T](n)
	case 4: // TIntegerDictionary,
		return tests.GenDups[T](n, n/10, -1)
	case 5: // TIntegerSimple8,
		return tests.GenRnd[T](n)
	case 6: // TIntegerRaw,
		return tests.GenRnd[T](n)
	default:
		return tests.GenRnd[T](n)
	}
}

func GenForFloatScheme[T types.Float](scheme, n int) []T {
	switch scheme {
	case 0: // TFloatConstant,
		return tests.GenConst[T](n, 4.225)
	case 1: // TFloatRunEnd,
		return tests.GenRuns[T](n, min(n, 5), -1)
	case 2: // TFloatDictionary,
		return tests.GenDups[T](n, n/10, -1)
	case 3: // TFloatAlp,
		w := 29
		if util.SizeOf[T]() == 4 {
			w = 14
		}
		return tests.GenRndBits[T](n, w)
	case 4: // TFloatAlpRd,
		w := 49
		if util.SizeOf[T]() == 4 {
			w = 28
		}
		return tests.GenRndBits[T](n, w)
	case 5: // TFloatRaw,
		return tests.GenRnd[T](n)
	default:
		return tests.GenRnd[T](n)
	}
}
