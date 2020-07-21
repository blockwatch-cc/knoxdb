// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

// +build go1.7,amd64,!gccgo,!appengine

package vec

//go:noescape
func matchFloat64EqualAVX2(src []float64, val float64, bits []byte) int64

//go:noescape
func matchFloat64NotEqualAVX2(src []float64, val float64, bits []byte) int64

//go:noescape
func matchFloat64LessThanAVX2(src []float64, val float64, bits []byte) int64

//go:noescape
func matchFloat64LessThanEqualAVX2(src []float64, val float64, bits []byte) int64

//go:noescape
func matchFloat64GreaterThanAVX2(src []float64, val float64, bits []byte) int64

//go:noescape
func matchFloat64GreaterThanEqualAVX2(src []float64, val float64, bits []byte) int64

//go:noescape
func matchFloat64BetweenAVX2(src []float64, a, b float64, bits []byte) int64

func matchFloat64Equal(src []float64, val float64, bits []byte) int64 {
	switch {
	case useAVX2:
		return matchFloat64EqualAVX2(src, val, bits)
	default:
		return matchFloat64EqualGeneric(src, val, bits)
	}
}

func matchFloat64NotEqual(src []float64, val float64, bits []byte) int64 {
	switch {
	case useAVX2:
		return matchFloat64NotEqualAVX2(src, val, bits)
	default:
		return matchFloat64NotEqualGeneric(src, val, bits)
	}
}

func matchFloat64LessThan(src []float64, val float64, bits []byte) int64 {
	switch {
	case useAVX2:
		return matchFloat64LessThanAVX2(src, val, bits)
	default:
		return matchFloat64LessThanGeneric(src, val, bits)
	}
}

func matchFloat64LessThanEqual(src []float64, val float64, bits []byte) int64 {
	switch {
	case useAVX2:
		return matchFloat64LessThanEqualAVX2(src, val, bits)
	default:
		return matchFloat64LessThanEqualGeneric(src, val, bits)
	}
}

func matchFloat64GreaterThan(src []float64, val float64, bits []byte) int64 {
	switch {
	case useAVX2:
		return matchFloat64GreaterThanAVX2(src, val, bits)
	default:
		return matchFloat64GreaterThanGeneric(src, val, bits)
	}
}

func matchFloat64GreaterThanEqual(src []float64, val float64, bits []byte) int64 {
	switch {
	case useAVX2:
		return matchFloat64GreaterThanEqualAVX2(src, val, bits)
	default:
		return matchFloat64GreaterThanEqualGeneric(src, val, bits)
	}
}

func matchFloat64Between(src []float64, a, b float64, bits []byte) int64 {
	switch {
	case useAVX2:
		return matchFloat64BetweenAVX2(src, a, b, bits)
	default:
		return matchFloat64BetweenGeneric(src, a, b, bits)
	}
}
