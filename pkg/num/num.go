// Copyright (c) 2023 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package num

import (
	"errors"
)

var (
	ErrScaleOverflow      = errors.New("num: decimal scale overflow")
	ErrScaleUnderflow     = errors.New("num: decimal scale underflow")
	ErrPrecisionOverflow  = errors.New("num: decimal precision overflow")
	ErrPrecisionUnderflow = errors.New("num: decimal precision underflow")
	ErrInvalidFloat64     = errors.New("num: invalid float64 number")
	ErrInvalidDecimal     = errors.New("num: invalid decimal number")
	ErrInvalidNumber      = errors.New("num: invalid number")
)

type Accuracy int8

const (
	Below Accuracy = -1
	Exact Accuracy = 0
	Above Accuracy = +1
)

func (a Accuracy) String() string {
	switch a {
	case Below:
		return "below"
	case Above:
		return "above"
	default:
		return "exact"
	}
}

// var decimalRegexp = regexp.MustCompile("^[+-]?([0-9]*[.])?[0-9]+$")

const (
	MinDecimal32Precision  uint8 = 1
	MaxDecimal32Precision  uint8 = 9
	MinDecimal64Precision  uint8 = 10
	MaxDecimal64Precision  uint8 = 18
	MinDecimal128Precision uint8 = 19
	MaxDecimal128Precision uint8 = 38
	MinDecimal256Precision uint8 = 39
	MaxDecimal256Precision uint8 = 76
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

func min(x, y int) int {
	if x < y {
		return x
	}
	return y
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
