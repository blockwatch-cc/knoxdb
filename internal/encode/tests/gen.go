// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package tests

import (
	"fmt"

	"blockwatch.cc/knoxdb/internal/tests"
	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/stringx"
	"blockwatch.cc/knoxdb/pkg/util"
)

func GenForIntScheme[T types.Number](scheme, n int) []T {
	sz := util.SizeOf[T]()
	switch scheme {
	case 1: // TIntConstant,
		return tests.GenConst[T](n, 42)
	case 2: // TIntDelta (every n offset by n)
		seq := tests.GenSeq[T](n, 2)
		for i := range seq {
			seq[i] += T(1)
		}
		return seq
	case 3: // TIntRunEnd,
		return tests.GenRuns[T](n, min(n, 5), sz*2)
	case 4: // TIntBitpacked,
		return tests.GenRndBits[T](n, sz*2)
	case 5: // TIntDictionary,
		return tests.GenDups[T](n, n/10, sz*2)
	case 6: // TIntSimple8,
		return tests.GenRndBits[T](n, sz*2)
	case 7: // TIntRaw,
		return tests.GenRnd[T](n)
	// 8 TInt128
	// 9 TInt256
	default:
		panic(fmt.Errorf("GenForIntScheme: unsupported scheme %d", scheme))
	}
}

func GenForFloatScheme[T types.Float](scheme, n int) []T {
	switch scheme {
	case 10: // TFloatConstant,
		return tests.GenConst[T](n, 4.225)
	case 11: // TFloatRunEnd,
		return tests.GenRuns[T](n, min(n, 5), -1)
	case 12: // TFloatDictionary,
		return tests.GenDups[T](n, n/10, -1)
	case 13: // TFloatAlp,
		w := 29
		if util.SizeOf[T]() == 4 {
			w = 14
		}
		return tests.GenRndBits[T](n, w)
	case 14: // TFloatAlpRd,
		w := 49
		if util.SizeOf[T]() == 4 {
			w = 28
		}
		return tests.GenRndBits[T](n, w)
	case 15: // TFloatRaw,
		return tests.GenRnd[T](n)
	default:
		panic(fmt.Errorf("GenForFloatScheme: unsupported scheme %d", scheme))
	}
}

func GenForStringScheme(scheme, n int) *stringx.StringPool {
	switch scheme {
	case 16:
		// TStringConstant
		return tests.GenStringConst(n, []byte("42"))
	case 17:
		// TStringFixed
		return tests.GenStringRnd(n, 8)
	case 18:
		// TStringCompact
		return tests.GenStringDups(n, min(1, n*3/4), -1)
	case 19:
		// TStringDictionary
		return tests.GenStringDups(n, n/5, -1)
	default:
		panic(fmt.Errorf("GenForStringScheme: unsupported scheme %d", scheme))
	}
}
