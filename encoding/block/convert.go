// Copyright (c) 2018-2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package block

import (
	"github.com/ericlagergren/decimal"
	"math"
)

// -----------------------------------------------------------------------------
// In order to reduce the size of stored amounts, a domain specific compression
// algorithm is used which relies on there typically being a lot of zeroes at
// end of the amounts.  The compression algorithm used here was obtained from
// Bitcoin Core, so all credits for the algorithm go to it.
//
// While this is simply exchanging one uint64 for another, the resulting value
// for typical amounts has a much smaller magnitude which results in fewer bytes
// when encoded as variable length quantity.  For example, consider the amount
// of 0.1 DCR which is 10000000 atoms.  Encoding 10000000 as a VarInt would take
// 4 bytes while encoding the compressed value of 8 as a VarInt only takes 1 byte.
//
// Essentially the compression is achieved by splitting the value into an
// exponent in the range [0-9] and a digit in the range [1-9], when possible,
// and encoding them in a way that can be decoded.  More specifically, the
// encoding is as follows:
// - 0 is 0
// - Find the exponent, e, as the largest power of 10 that evenly divides the
//   value up to a maximum of 9
// - When e < 9, the final digit can't be 0 so store it as d and remove it by
//   dividing the value by 10 (call the result n).  The encoded value is thus:
//   1 + 10*(9*n + d-1) + e
// - When e==9, the only thing known is the amount is not 0.  The encoded value
//   is thus:
//   1 + 10*(n-1) + e   ==   10 + 10*(n-1)
//
// Example encodings:
// (The numbers in parenthesis are the number of bytes when serialized as a VarInt)
//            0 (1) -> 0        (1)           *  0.00000000 BTC
//         1000 (2) -> 4        (1)           *  0.00001000 BTC
//        10000 (2) -> 5        (1)           *  0.00010000 BTC
//     12345678 (4) -> 111111101(4)           *  0.12345678 BTC
//     50000000 (4) -> 47       (1)           *  0.50000000 BTC
//    100000000 (4) -> 9        (1)           *  1.00000000 BTC
//    500000000 (5) -> 49       (1)           *  5.00000000 BTC
//   1000000000 (5) -> 10       (1)           * 10.00000000 BTC
// -----------------------------------------------------------------------------

// Compact64 compresses the passed amount according to the domain
// specific compression algorithm described above.
func Compact64(amount uint64) uint64 {
	// No need to do any work if it's zero.
	if amount == 0 {
		return 0
	}

	// Find the largest power of 10 (max of 9) that evenly divides the
	// value.
	exponent := uint64(0)
	for amount%10 == 0 && exponent < 9 {
		amount /= 10
		exponent++
	}

	// The compressed result for exponents less than 9 is:
	// 1 + 10*(9*n + d-1) + e
	if exponent < 9 {
		lastDigit := amount % 10
		amount /= 10
		return 1 + 10*(9*amount+lastDigit-1) + exponent
	}

	// The compressed result for an exponent of 9 is:
	// 1 + 10*(n-1) + e   ==   10 + 10*(n-1)
	return 10 + 10*(amount-1)
}

// Decompact64 returns the original amount the passed compressed
// amount represents according to the domain specific compression algorithm
// described above.
func Decompact64(amount uint64) uint64 {
	// No need to do any work if it's zero.
	if amount == 0 {
		return 0
	}

	// The decompressed amount is either of the following two equations:
	// x = 1 + 10*(9*n + d - 1) + e
	// x = 1 + 10*(n - 1)       + 9
	amount--

	// The decompressed amount is now one of the following two equations:
	// x = 10*(9*n + d - 1) + e
	// x = 10*(n - 1)       + 9
	exponent := amount % 10
	amount /= 10

	// The decompressed amount is now one of the following two equations:
	// x = 9*n + d - 1  | where e < 9
	// x = n - 1        | where e = 9
	var n uint64
	if exponent < 9 {
		lastDigit := amount%9 + 1
		amount /= 9
		n = amount*10 + lastDigit
	} else {
		n = amount + 1
	}

	// Apply the exponent.
	for ; exponent > 0; exponent-- {
		n *= 10
	}

	return n
}

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
