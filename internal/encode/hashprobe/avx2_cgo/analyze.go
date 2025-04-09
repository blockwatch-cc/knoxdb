package main

import (
	"math/bits"
	"unsafe"
)

type IntegerContext[T Integer] struct {
	Min         T                  // vector minimum
	Max         T                  // vector maximum
	Delta       T                  // common delta between vector values
	NumRuns     int                // vector runs
	NumUnique   int                // vector cardinality (hint, may not be precise)
	NumValues   int                // vector length
	PhyBits     int                // phy type bit width 8, 16, 32, 64
	UseBits     int                // used bits for bit-packing
	Sample      []T                // data sample (optional)
	SampleCtx   *IntegerContext[T] // sample analysis
	FreeSample  bool               // hint whether sample may be reused
	UniqueArray []T                // unique values as array (optional)
	UniqueMap   map[T]uint16       // unique values (optional)
}

func AnalyzeInt[T Integer](vals []T, checkUnique bool) *IntegerContext[T] {
	c := &IntegerContext[T]{}
	c.PhyBits = int(unsafe.Sizeof(T(0))) * 8
	c.NumValues = len(vals)
	c.Min, c.Max, c.Delta, c.NumRuns = Analyze(vals)

	// count unique only if necessary
	isSigned := IsSigned[T]()
	c.NumUnique = estimateCardinality(vals)

	switch c.PhyBits {
	case 64:
		if isSigned {
			c.UseBits = bits.Len64(uint64(int64(c.Max) - int64(c.Min)))
		} else {
			c.UseBits = bits.Len64(uint64(c.Max - c.Min))
		}
	case 32:
		if isSigned {
			c.UseBits = bits.Len32(uint32(int32(c.Max) - int32(c.Min)))
		} else {
			c.UseBits = bits.Len32(uint32(c.Max - c.Min))
		}
	case 16:
		if isSigned {
			c.UseBits = bits.Len16(uint16(int16(c.Max) - int16(c.Min)))
		} else {
			c.UseBits = bits.Len16(uint16(c.Max - c.Min))
		}
	case 8:
		if isSigned {
			c.UseBits = bits.Len8(uint8(int8(c.Max) - int8(c.Min)))
		} else {
			c.UseBits = bits.Len8(uint8(c.Max - c.Min))
		}
	}
	return c
}

func Analyze[T Integer](vals []T) (minv T, maxv T, delta T, numRuns int) {
	if len(vals) == 0 {
		return
	}
	minv = vals[0]
	maxv = vals[0]
	if len(vals) > 1 {
		delta = vals[1] - vals[0]
	}
	numRuns = 1
	hasDelta := delta != 0

	i := 1
	for ; i < len(vals); i++ {
		v := vals[i]
		if v < minv {
			minv = v
		} else if v > maxv {
			maxv = v
		}
		if vals[i-1] != v {
			numRuns++
			hasDelta = hasDelta && delta == v-vals[i-1]
		}
	}

	if !hasDelta {
		delta = 0
	}
	return
}

func estimateCardinality[T Integer](vals []T) int {
	m := make(map[T]struct{})
	for _, v := range vals {
		m[v] = struct{}{}
	}
	return len(m)
}
