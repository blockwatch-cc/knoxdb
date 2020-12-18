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
	// "encoding"
	// "fmt"
	"regexp"
)

// type Decimal interface {
// 	Bitsize() int
// 	Scale() int
// 	Precision() int
// 	Int64() int64
// 	Int128() Int128
// 	Int256() Int256
// 	Float64() float64
// 	encoding.TextMarshaler

// 	// pointer-receiver methods
// 	Quantize(scale int) Decimal
// 	SetInt64(value int64, scale int) error
// 	SetFloat64(value float64, scale int) error
// 	encoding.TextUnmarshaler
// }

var decimalRegexp = regexp.MustCompile("^[+-]?([0-9]*[.])?[0-9]+$")

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

var pow10 = []uint64{
	1,                    // 0
	10,                   // 1
	100,                  // 2
	1000,                 // 3
	10000,                // 4
	100000,               // 5
	1000000,              // 6
	10000000,             // 7
	100000000,            // 8
	1000000000,           // 9
	10000000000,          // 10
	100000000000,         // 11
	1000000000000,        // 12
	10000000000000,       // 13
	100000000000000,      // 14
	1000000000000000,     // 15
	10000000000000000,    // 16
	100000000000000000,   // 17
	1000000000000000000,  // 18
	10000000000000000000, // 19
}

// func NewDecimal(prec, scale int) (Decimal, error) {
// 	if prec < 1 {
// 		return nil, fmt.Errorf("decimal: invalid negative precision %d", prec)
// 	}
// 	if scale < 0 {
// 		return nil, fmt.Errorf("decimal: invalid negative scale %d", scale)
// 	}
// 	switch true {
// 	case prec <= MaxDecimal32Precision:
// 		d := NewDecimal32(0, scale)
// 		_, err := d.Check()
// 		return &d, err
// 	case prec <= MaxDecimal64Precision:
// 		d := NewDecimal64(0, scale)
// 		_, err := d.Check()
// 		return &d, err
// 	case prec <= MaxDecimal128Precision:
// 		d := NewDecimal128(Int128{}, scale)
// 		_, err := d.Check()
// 		return &d, err
// 	case prec <= MaxDecimal256Precision:
// 		d := NewDecimal256(Int256{}, scale)
// 		_, err := d.Check()
// 		return &d, err
// 	default:
// 		return nil, fmt.Errorf("decimal: precision %d out of range", prec)
// 	}
// }

func abs(n int64) uint64 {
	y := n >> 63
	return uint64((n ^ y) - y)
}
