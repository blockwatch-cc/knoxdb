// Copyright (c) 2025 Blockwatch Data Inc.
// Author: abdul@blockwatch.cc,alex@blockwatch.cc

package alp

import (
	"math"
	"unsafe"
)

const (
	uvnan    = 0x7FF8000000000001
	uvinf    = 0x7FF0000000000000
	uvneginf = 0xFFF0000000000000
	sign     = 0x8000000000000000
	hi       = 0x43EFFFFFFFFFFFFF
	lo       = 0xC3EFFFFFFFFFFFFF
)

/*
 * Check for special values which are impossible for ALP to encode
 * because they cannot be cast to int64 without an undefined behaviour
 */
func isImpossibleToEncode(f float64) bool {
	x := *(*uint64)(unsafe.Pointer(&f))
	return f > ENCODING_UPPER_LIMIT ||
		f < ENCODING_LOWER_LIMIT ||
		x == uvinf || x == uvneginf || // isInf
		x == uvnan || // isNan
		x == sign //! Verification for -0.0
}

// see
// https://github.com/cwida/ALP/blob/main/include/alp/encoder.hpp#L322

func isImpossibleToEncodeSlow(f float64) bool {
	return math.IsInf(f, 0) || math.IsNaN(f) ||
		f > ENCODING_UPPER_LIMIT ||
		f < ENCODING_LOWER_LIMIT ||
		(f == 0.0 && math.Signbit(f))
}
