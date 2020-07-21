// Copyright (c) 2019 - 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package vec

// Vector algorithms for sorted slices based on binary search from Go's
// sort package. The following slice types are supported
//
// ```
// ByteSlice [][]byte
// StringSlice []string
// TimeSlice []time.Time
// BoolSlice []bool
// Int64Slice []int64
// Uint64Slice []uint64
// Float64Slice []float64
// ```
//
// Each type defines the following methods
//
// Contains
// Index
// MinMax
// ContainsRange
//
// ## Range coverage algorithm
//
// Checks if a sparse sorted slice contains any value(s) in the
// closed interval [from, to].
//
// This is used when deciding whether a pack contains any of the
// values from an IN condition based on the packs min/max range.
//
//    val slice ->    |- - - - - - - - - -|
//                    .                   .
// Range A      [--]  .                   .
// Range B.1       [--]                   .
// Range B.2      [-------]               .
// Range B.3          [--]                .
// Range C.1          .       [--]        .            // some values in range
// Range C.2          .       [--]        .            // no values in range
// Range D.1          .                [--]
// Range D.2          .               [-------]
// Range D.3          .                   [--]
// Range E            .                   .  [--]
// Range F     [-----------------------------------]
//
