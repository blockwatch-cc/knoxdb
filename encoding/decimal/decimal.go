// Copyright (c) 2018-2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

// half-even rounding mode (IEEE 754-2008 roundTiesToEven)

// Inspiration
//
// https://en.wikipedia.org/wiki/Rounding#Round_half_to_even
// Decimal32-256 https://clickhouse.tech/docs/en/sql-reference/data-types/decimal/
// IEEE 754R Golang https://github.com/anz-bank/decimal
// DEC64 https://www.crockford.com/dec64.html

package decimal

import (
	"errors"
	"regexp"
)

var decimalRegexp = regexp.MustCompile("^[+-]?([0-9]*[.])?[0-9]+$")

var (
	ErrScaleOverflow      = errors.New("decimal: scale overflow")
	ErrScaleUnderflow     = errors.New("decimal: scale underflow")
	ErrPrecisionOverflow  = errors.New("decimal: precision overflow")
	ErrPrecisionUnderflow = errors.New("decimal: precision underflow")
	ErrInvalidFloat64     = errors.New("decimal: invalid float64 number")
	ErrInvalidDecimal     = errors.New("decimal: invalid decimal number")
)

const (
	MinDecimal32Precision  = 1
	MaxDecimal32Precision  = 9
	MinDecimal64Precision  = 10
	MaxDecimal64Precision  = 18
	MinDecimal128Precision = 19
	MaxDecimal128Precision = 38
	MinDecimal256Precision = 39
	MaxDecimal256Precision = 76
)

const zeros = "000000000000000000000000000000000000000000000000000000000000000000000000000000"

// 1,                    // 0
// 10,                   // 1
// 100,                  // 2
// 1000,                 // 3
// 10000,                // 4
// 100000,               // 5
// 1000000,              // 6
// 10000000,             // 7
// 100000000,            // 8
// 1000000000,           // 9
// 10000000000,          // 10
// 100000000000,         // 11
// 1000000000000,        // 12
// 10000000000000,       // 13
// 100000000000000,      // 14
// 1000000000000000,     // 15
// 10000000000000000,    // 16
// 100000000000000000,   // 17
// 1000000000000000000,  // 18
// 10000000000000000000, // 19
var pow10 = [20]uint64{}

func init() {
	pow10[0] = 1
	for i := 1; i < len(pow10); i++ {
		pow10[i] = pow10[i-1] * 10
	}
}

func abs(n int64) uint64 {
	y := n >> 63
	return uint64((n ^ y) - y)
}

func digits64(val int64) int {
	v := abs(val)
	for i := range pow10 {
		if v >= pow10[i] {
			continue
		}
		return i
	}
	return 0
}
