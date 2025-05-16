// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package encode

import (
	"math/bits"
	"sync"
	"unsafe"

	"blockwatch.cc/knoxdb/internal/arena"
	"blockwatch.cc/knoxdb/internal/encode/analyze"
	"blockwatch.cc/knoxdb/internal/encode/hashprobe"
	"blockwatch.cc/knoxdb/internal/filter/llb"
	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/num"
	"blockwatch.cc/knoxdb/pkg/util"
)

type IntegerContext[T types.Integer] struct {
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
}

func NewIntegerContext[T types.Integer](minv, maxv T, n int) *IntegerContext[T] {
	c := newIntegerContext[T]()
	c.PhyBits = int(unsafe.Sizeof(T(0))) * 8
	c.UseBits = types.Log2Range(minv, maxv)
	c.NumValues = n
	c.Min = minv
	c.Max = maxv
	c.NumRuns = n
	c.NumUnique = n
	return c
}

// AnalyzeInt produces statistics about slice vals which are used to
// find the most efficient encoding scheme.
func AnalyzeInt[T types.Integer](vals []T, checkUnique bool) *IntegerContext[T] {
	c := newIntegerContext[T]()
	c.PhyBits = util.SizeOf[T]() * 8
	c.NumValues = len(vals)

	// vector analyze
	switch any(T(0)).(type) {
	case int64:
		minv, maxv, delta, nruns := analyze.AnalyzeInt64(util.ReinterpretSlice[T, int64](vals))
		c.Min, c.Max, c.Delta, c.NumRuns = T(minv), T(maxv), T(delta), nruns
	case int32:
		minv, maxv, delta, nruns := analyze.AnalyzeInt32(util.ReinterpretSlice[T, int32](vals))
		c.Min, c.Max, c.Delta, c.NumRuns = T(minv), T(maxv), T(delta), nruns
	case int16:
		minv, maxv, delta, nruns := analyze.AnalyzeInt16(util.ReinterpretSlice[T, int16](vals))
		c.Min, c.Max, c.Delta, c.NumRuns = T(minv), T(maxv), T(delta), nruns
	case int8:
		minv, maxv, delta, nruns := analyze.AnalyzeInt8(util.ReinterpretSlice[T, int8](vals))
		c.Min, c.Max, c.Delta, c.NumRuns = T(minv), T(maxv), T(delta), nruns
	case uint64:
		minv, maxv, delta, nruns := analyze.AnalyzeUint64(util.ReinterpretSlice[T, uint64](vals))
		c.Min, c.Max, c.Delta, c.NumRuns = T(minv), T(maxv), T(delta), nruns
	case uint32:
		minv, maxv, delta, nruns := analyze.AnalyzeUint32(util.ReinterpretSlice[T, uint32](vals))
		c.Min, c.Max, c.Delta, c.NumRuns = T(minv), T(maxv), T(delta), nruns
	case uint16:
		minv, maxv, delta, nruns := analyze.AnalyzeUint16(util.ReinterpretSlice[T, uint16](vals))
		c.Min, c.Max, c.Delta, c.NumRuns = T(minv), T(maxv), T(delta), nruns
	case uint8:
		minv, maxv, delta, nruns := analyze.AnalyzeUint8(util.ReinterpretSlice[T, uint8](vals))
		c.Min, c.Max, c.Delta, c.NumRuns = T(minv), T(maxv), T(delta), nruns
	}

	// count unique only if necessary
	doCountUnique := checkUnique && c.Min != c.Max && c.Delta == 0
	c.NumUnique = min(c.NumRuns, int(c.Max)-int(c.Min)+1)
	c.UseBits = types.Log2Range(c.Min, c.Max)

	if doCountUnique {
		switch c.PhyBits {
		case 64:
			// use array when c.Max-c.Min < 64k
			sz := int(c.Max) - int(c.Min) + 1
			if sz <= 1<<16 {
				c.NumUnique = c.buildUniqueArray(vals)
			} else {
				c.NumUnique = max(1, c.estimateCardinality(vals))
			}
		case 32:
			// use array when c.Max-c.Min < 64k
			sz := int(c.Max) - int(c.Min) + 1
			if sz <= 1<<16 {
				c.NumUnique = c.buildUniqueArray(vals)
			} else {
				c.NumUnique = max(1, c.estimateCardinality(vals))
			}
		case 16:
			c.NumUnique = c.buildUniqueArray(vals)
		case 8:
			c.NumUnique = c.buildUniqueArray(vals)
		}
	}
	return c
}

func (c *IntegerContext[T]) estimateCardinality(vals []T) int {
	var scratch [256]byte // need 256 byte scratch space
	unique, _ := llb.NewFilterBuffer(scratch[:], 8)
	if c.PhyBits == 64 {
		unique.AddMultiUint64(util.ReinterpretSlice[T, uint64](vals))
	} else {
		unique.AddMultiUint32(util.ReinterpretSlice[T, uint32](vals))
	}
	card := int(unique.Cardinality())
	return card
}

func (c *IntegerContext[T]) buildUniqueArray(vals []T) int {
	// we only need enough space for our data range
	sz := int64(c.Max) - int64(c.Min) + 1
	if cap(c.UniqueArray) < int(sz) {
		c.UniqueArray = make([]T, sz)
	}
	c.UniqueArray = c.UniqueArray[:sz]

	// mark existing values
	for _, v := range vals {
		c.UniqueArray[int64(v)-int64(c.Min)] = T(1)
	}

	// count unique values and assign codewords (+1 to distinguish empty slots)
	var numUnique int
	for i, v := range c.UniqueArray {
		if v > 0 {
			c.UniqueArray[i] = T(numUnique) + 1
			numUnique++
		}
	}

	return numUnique
}

func (c *IntegerContext[T]) EligibleSchemes() []IntegerContainerType {
	// constant only
	if c.NumRuns == 1 {
		return []IntegerContainerType{TIntegerConstant}
	}
	// delta only with at least 3 values
	if c.Delta > 0 && c.NumValues > 2 {
		return []IntegerContainerType{TIntegerDelta}
	}
	// raw always works
	schemes := []IntegerContainerType{
		TIntegerRaw,
	}
	// bit-packed width must decrease
	if c.UseBits < c.PhyBits {
		schemes = append(schemes, TIntegerBitpacked)
	}

	// FIXME: disabled s8 because s8b.Iterator decodes partial chunks which breaks
	// container iterators plus s8b is much slower than bitpack although it often
	// compresses better
	//
	// simple8b supports max 60bit values but is inefficient if many values are > 20bit
	// if c.UseBits < c.PhyBits && c.UseBits <= 60 {
	// 	schemes = append(schemes, TIntegerSimple8)
	// }

	// run-end requires avg run lengths >= 4
	if c.preferRunEnd() {
		schemes = append(schemes, TIntegerRunEnd)
	}
	// dict makes only sense when more efficient than bit-packing
	if c.preferDict() {
		schemes = append(schemes, TIntegerDictionary)
	}
	return schemes
}

func (c *IntegerContext[T]) preferDict() bool {
	return c.NumUnique <= hashprobe.MAX_DICT_LIMIT && c.dictCosts() < c.bitPackCosts()
}

func (c *IntegerContext[T]) preferRunEnd() bool {
	return c.NumRuns*RUN_END_THRESHOLD <= c.NumValues && c.runEndCosts() < c.bitPackCosts()
}

func (c *IntegerContext[T]) dictCosts() int {
	return dictCosts(c.NumValues, c.UseBits, c.NumUnique)
}

func (c *IntegerContext[T]) bitPackCosts() int {
	return bitPackCosts(c.NumValues, c.UseBits)
}

func (c *IntegerContext[T]) runEndCosts() int {
	return runEndCosts(c.NumValues, c.NumRuns, c.UseBits)
}

func dictCosts(n, w, c int) int {
	return 1 + bitPackCosts(c, w) + bitPackCosts(n, bits.Len(uint(c-1)))
}

func bitPackCosts(n, w int) int {
	return 2 + num.MaxVarintLen32 + num.UvarintLen(n) + (n*w+63)&^63/8
}

func runEndCosts(n, r, w int) int {
	return 1 + bitPackCosts(r, w) + bitPackCosts(r, bits.Len(uint(n-1)))
}

func (c *IntegerContext[T]) Close() {
	if c.UniqueArray != nil {
		clear(c.UniqueArray)
		c.UniqueArray = c.UniqueArray[:0]
	}
	if c.SampleCtx != nil {
		c.SampleCtx.Close()
		c.SampleCtx = nil
	}
	if c.Sample != nil {
		if c.FreeSample {
			arena.Free(c.Sample)
		}
		c.FreeSample = false
		c.Sample = nil
	}

	c.Min = 0
	c.Max = 0
	c.Delta = 0
	c.NumRuns = 0
	c.NumUnique = 0
	c.NumValues = 0
	c.PhyBits = 0
	c.UseBits = 0

	putIntegerContext(c)
}

type IntegerContextFactory struct {
	i64Pool sync.Pool
	i32Pool sync.Pool
	i16Pool sync.Pool
	i8Pool  sync.Pool
	u64Pool sync.Pool
	u32Pool sync.Pool
	u16Pool sync.Pool
	u8Pool  sync.Pool
}

func newIntegerContext[T types.Integer]() *IntegerContext[T] {
	switch (any(T(0))).(type) {
	case int64:
		return intContextFactory.i64Pool.Get().(*IntegerContext[T])
	case int32:
		return intContextFactory.i32Pool.Get().(*IntegerContext[T])
	case int16:
		return intContextFactory.i16Pool.Get().(*IntegerContext[T])
	case int8:
		return intContextFactory.i8Pool.Get().(*IntegerContext[T])
	case uint64:
		return intContextFactory.u64Pool.Get().(*IntegerContext[T])
	case uint32:
		return intContextFactory.u32Pool.Get().(*IntegerContext[T])
	case uint16:
		return intContextFactory.u16Pool.Get().(*IntegerContext[T])
	case uint8:
		return intContextFactory.u8Pool.Get().(*IntegerContext[T])
	default:
		return nil
	}
}

func putIntegerContext[T types.Integer](c *IntegerContext[T]) {
	switch (any(T(0))).(type) {
	case int64:
		intContextFactory.i64Pool.Put(c)
	case int32:
		intContextFactory.i32Pool.Put(c)
	case int16:
		intContextFactory.i16Pool.Put(c)
	case int8:
		intContextFactory.i8Pool.Put(c)
	case uint64:
		intContextFactory.u64Pool.Put(c)
	case uint32:
		intContextFactory.u32Pool.Put(c)
	case uint16:
		intContextFactory.u16Pool.Put(c)
	case uint8:
		intContextFactory.u8Pool.Put(c)
	}
}

var intContextFactory = IntegerContextFactory{
	i64Pool: sync.Pool{
		New: func() any { return new(IntegerContext[int64]) },
	},
	i32Pool: sync.Pool{
		New: func() any { return new(IntegerContext[int32]) },
	},
	i16Pool: sync.Pool{
		New: func() any { return new(IntegerContext[int16]) },
	},
	i8Pool: sync.Pool{
		New: func() any { return new(IntegerContext[int8]) },
	},
	u64Pool: sync.Pool{
		New: func() any { return new(IntegerContext[uint64]) },
	},
	u32Pool: sync.Pool{
		New: func() any { return new(IntegerContext[uint32]) },
	},
	u16Pool: sync.Pool{
		New: func() any { return new(IntegerContext[uint16]) },
	},
	u8Pool: sync.Pool{
		New: func() any { return new(IntegerContext[uint8]) },
	},
}
