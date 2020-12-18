// Copyright (c) 2018-2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc
// +build ignore

package block

import (
	"github.com/ericlagergren/decimal"
	"math"
)

// ToFixed64 converts a floating point number, which may or may not be representable
// as an integer, to an integer type by rounding to the nearest integer.
// This is performed consistent with the General Decimal Arithmetic spec as
// implemented by github.com/ericlagergren/decimal instead of simply by adding or
// subtracting 0.5 depending on the sign, and relying on integer truncation to round
// the value to the nearest Amount.
func ToFixed64(f float64, dec int) uint64 {
	var big = decimal.New(0, dec)
	i, _ := big.SetFloat64(f * math.Pow10(dec)).RoundToInt().Int64()
	return uint64(i)
}

func FromFixed64(amount uint64, dec int) float64 {
	return float64(int64(amount)) / math.Pow10(dec)
}
