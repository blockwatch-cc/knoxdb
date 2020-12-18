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
	"encoding"
	"fmt"
	"github.com/ericlagergren/decimal"
	"regexp"
	"strconv"
	"strings"
)

type decimalTestcase struct {
	String string
	Int64  int64
	Scale  int
}

var decimal32Testcases = []decimalTestcase{
	{"+1234.56789", 123456789, 5},
	{"-1234.56789", -123456789, 5},
	{"23.5", 235, 1},
}

// j, ok := decimal.
// 	WithContext(decimal.Context64).
// 	SetFloat64(value).
// 	Quantize(scale).
// 	SetScale(0).
// 	RoundToInt().
// 	Int64()
// if i != j || !ok {
// 	fmt.Printf("Rounding error %d != %d\n", i, j)
// }
