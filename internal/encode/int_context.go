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
	Unique      map[T]uint16       // unique values (optional)
	Sample      []T                // data sample (optional)
	SampleCtx   *IntegerContext[T] // sample analysis
	FreeSample  bool
	UniqueArray []T // unique values as array (optional)
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
			// TODO: when c.Max-c.Min < 64k use array
			// TODO: optimize HLL (keep memory in context, optimize params for NumValues)
			// may reuse unique array as bytes
			if c.UniqueArray == nil {
				c.UniqueArray = make([]T, 32)
			}
			unique, _ := loglogbeta.NewFilterBuffer(util.ToByteSlice(c.UniqueArray), 8)
			unique.AddManyUint64(util.ReinterpretSlice[T, uint64](vals))
			c.NumUnique = int(unique.Cardinality())
		}
	case 32:
		c.UseBits = bits.Len32(uint32(c.Max - c.Min))
		if doCountUnique {
			// TODO: when c.Max-c.Min < 64k use array
			// TODO: optimize HLL (keep memory in context, optimize params for NumValues)
			// may reuse unique array as bytes
			if c.UniqueArray == nil {
				c.UniqueArray = make([]T, 64)
			}
			unique, _ := loglogbeta.NewFilterBuffer(util.ToByteSlice(c.UniqueArray), 8)
			unique.AddManyUint32(util.ReinterpretSlice[T, uint32](vals))
			c.NumUnique = int(unique.Cardinality())
		}
	case 16:
		c.UseBits = bits.Len16(uint16(c.Max - c.Min))
		if doCountUnique {
			sz := int(c.Max) - int(c.Min) + 1
			if cap(c.UniqueArray) < sz {
				c.UniqueArray = make([]T, sz)
			}
			for _, v := range vals {
				c.UniqueArray[int(v)-int(c.Min)] = T(1)
			}
			c.NumUnique = 0
			for i, v := range c.UniqueArray {
				if v > 0 {
					c.UniqueArray[i] = T(c.NumUnique)
					c.NumUnique++
				}
			}

			// if c.Unique == nil {
			// 	// TODO: use array make([]uint16, int(c.Max-c.Min)) instead
			// 	// set FF, then in dict encode, reset to sequence & use for dict encode
			// 	// reuse alloc through pool, realloc from arena when cap is insufficient
			// 	c.Unique = make(map[T]uint16, int(c.Max-c.Min))
			// }
			// for _, v := range vals {
			// 	c.Unique[v] = 0
			// }
			// c.NumUnique = len(c.Unique)
		}
	case 8:
		c.UseBits = bits.Len8(uint8(c.Max - c.Min))
		if doCountUnique {
			if c.Unique == nil {
				// TODO: use array make([]uint8, int(c.Max-c.Min)) instead
				// set FF, then in dict encode, reset to sequence & use for dict encode
				// reuse alloc, always alloc 256 max (small enough) no arena
				c.Unique = make(map[T]uint16, int(c.Max-c.Min))
			}
			for _, v := range vals {
				c.Unique[v] = 0
			}
			c.NumUnique = len(c.Unique)
		}
	}
	return c
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
	clear(c.Unique)
	clear(c.UniqueArray)
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
