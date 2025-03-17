// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package encode

import (
	"math/bits"
	"sync"
	"unsafe"

	"blockwatch.cc/knoxdb/internal/arena"
	"blockwatch.cc/knoxdb/internal/filter/loglogbeta"
	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/util"
)

type IntegerContext[T types.Integer] struct {
	Min         T                  // vector minimum
	Max         T                  // vector maximum
	Delta       T                  // common delta between vector values
	PhyBits     int                // phy type bit width 8, 16, 32, 64
	UseBits     int                // used bits for bit-packing
	NumUnique   int                // vector cardinality (hint, may not be precise)
	NumRuns     int                // vector runs
	NumValues   int                // vector length
	Sample      []T                // data sample (optional)
	SampleCtx   *IntegerContext[T] // sample analysis
	FreeSample  bool               // hint whether sample may be reused
	UniqueArray []T                // unique values as array (optional)
	UniqueMap   map[T]uint16       // unique values (optional)
}

// AnalyzeInt produces statistics about slice vals which are used to
// find the most efficient encoding scheme.
func AnalyzeInt[T types.Integer](vals []T, checkUnique bool) *IntegerContext[T] {
	c := newIntegerContext[T]()
	c.Min = vals[0]
	c.Max = vals[0]
	c.Delta = vals[util.Bool2int(len(vals) > 1)] - vals[0]
	c.PhyBits = int(unsafe.Sizeof(T(0))) * 8
	c.NumRuns = 1
	c.NumValues = len(vals)
	for i, v := range vals[1:] {
		if v < c.Min {
			c.Min = v
		} else if v > c.Max {
			c.Max = v
		}
		c.NumRuns += util.Bool2int(vals[i] != v)
		delta := v - vals[i]
		c.Delta = delta * T(util.Bool2int(c.Delta == delta))
	}

	// count unique only if necessary
	doCountUnique := checkUnique && c.Min != c.Max && c.Delta == 0
	c.NumUnique = min(c.NumRuns, int(c.Max)-int(c.Min))

	switch c.PhyBits {
	case 64:
		c.UseBits = bits.Len64(uint64(c.Max - c.Min))
		if doCountUnique {
			// use array when c.Max-c.Min < 64k
			sz := int(c.Max) - int(c.Min) + 1
			if sz <= 1<<16 {
				c.NumUnique = c.buildUniqueArray(vals)
			} else {
				c.NumUnique = c.estimateCardinality(vals)
			}
		}
	case 32:
		c.UseBits = bits.Len32(uint32(c.Max - c.Min))
		if doCountUnique {
			// use array when c.Max-c.Min < 64k
			sz := int(c.Max) - int(c.Min) + 1
			if sz <= 1<<16 {
				c.NumUnique = c.buildUniqueArray(vals)
			} else {
				c.NumUnique = c.estimateCardinality(vals)
			}
		}
	case 16:
		c.UseBits = bits.Len16(uint16(c.Max - c.Min))
		if doCountUnique {
			c.NumUnique = c.buildUniqueArray(vals)
		}
	case 8:
		c.UseBits = bits.Len8(uint8(c.Max - c.Min))
		if doCountUnique {
			c.NumUnique = c.buildUniqueArray(vals)
		}
	}
	return c
}

func (c *IntegerContext[T]) estimateCardinality(vals []T) int {
	sz := 256 / (c.PhyBits / 8) // need 256 byte scratch space
	if cap(c.UniqueArray) < sz {
		c.UniqueArray = make([]T, sz)
	}
	c.UniqueArray = c.UniqueArray[:sz]
	unique, _ := loglogbeta.NewFilterBuffer(util.ToByteSlice(c.UniqueArray), 8)
	if c.PhyBits == 64 {
		unique.AddManyUint64(util.ReinterpretSlice[T, uint64](vals))
	} else {
		unique.AddManyUint32(util.ReinterpretSlice[T, uint32](vals))
	}
	card := int(unique.Cardinality())
	clear(c.UniqueArray)
	c.UniqueArray = c.UniqueArray[:0]
	return card
}

func (c *IntegerContext[T]) buildUniqueArray(vals []T) int {
	// we only need enough space for our data range
	sz := int(c.Max) - int(c.Min) + 1
	if cap(c.UniqueArray) < sz {
		c.UniqueArray = make([]T, sz)
	}

	// mark existing values
	for _, v := range vals {
		c.UniqueArray[int(v)-int(c.Min)] = T(1)
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
	if c.Min == c.Max {
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
	// bit packed must decrease bit width by at least 8
	if c.UseBits+8 < c.PhyBits {
		schemes = append(schemes, TIntegerBitpacked)
	}
	// simple 8 requires max 60bit values
	if c.UseBits <= 60 {
		schemes = append(schemes, TIntegerSimple8)
	}
	// run-end requires avg run lengths >= 2
	if c.NumRuns*2 <= c.NumValues {
		schemes = append(schemes, TIntegerRunEnd)
	}
	// dict makes only sense if <64k entries, value range is reduced and card < 3/4
	if c.NumUnique < 1<<16-1 && c.Max-c.Min > T(c.NumUnique) && c.NumUnique*4/3 < c.NumValues {
		schemes = append(schemes, TIntegerDictionary)
	}
	return schemes
}

func (c *IntegerContext[T]) Close() {
	clear(c.UniqueArray)
	clear(c.UniqueMap)
	if c.SampleCtx != nil {
		c.SampleCtx.Close()
		c.SampleCtx = nil
	}
	if c.Sample != nil {
		if c.FreeSample {
			arena.FreeT(c.Sample)
		}
		c.FreeSample = false
		c.Sample = nil
	}
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
