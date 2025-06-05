// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package encode

import (
	"math/bits"
	"sync"
	"unsafe"

	"blockwatch.cc/knoxdb/internal/arena"
	"blockwatch.cc/knoxdb/internal/encode/alp"
	"blockwatch.cc/knoxdb/internal/encode/analyze"
	"blockwatch.cc/knoxdb/internal/encode/hashprobe"
	"blockwatch.cc/knoxdb/internal/filter/llb"
	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/num"
	"blockwatch.cc/knoxdb/pkg/util"
)

type ContextExporter interface {
	MinMax() (any, any)
	Unique() int
	Close()
}

// max encoder nesting level
const MAX_LEVEL = 3

type Context[T types.Number] struct {
	Min         T            // vector minimum
	Max         T            // vector maximum
	Delta       T            // common delta between vector values
	Lvl         int          // max encoder nesting level
	NumRuns     int          // vector runs
	NumUnique   int          // vector cardinality (hint, may not be precise)
	NumValues   int          // vector length
	PhyBits     int          // phy type bit width 8, 16, 32, 64
	UseBits     int          // used bits for bit-packing
	Sample      []T          // data sample (optional)
	SampleCtx   *Context[T]  // sample analysis
	FreeSample  bool         // hint whether sample may be reused
	UniqueArray []T          // unique values as array (optional)
	Alp         alp.Analysis // ALP analysis
}

func (c *Context[T]) WithLevel(l int) *Context[T] {
	c.Lvl = l
	return c
}

func NewIntContext[T types.Integer](minv, maxv T, n int) *Context[T] {
	c := newContext[T]()
	c.Lvl = MAX_LEVEL
	c.PhyBits = int(unsafe.Sizeof(T(0))) * 8
	c.UseBits = types.Log2Range(minv, maxv)
	c.NumValues = n
	c.Min = minv
	c.Max = maxv
	c.NumRuns = n
	c.NumUnique = n
	return c
}

func NewFloatContext[T types.Float](minv, maxv T, n int) *Context[T] {
	c := newContext[T]()
	c.Lvl = MAX_LEVEL
	c.PhyBits = util.SizeOf[T]() * 8
	c.UseBits = c.PhyBits
	c.NumValues = n
	c.Min = minv
	c.Max = maxv
	c.NumRuns = n
	c.NumUnique = n
	return c
}

// AnalyzeInt produces statistics about signed and unsigned integer vectors.
func AnalyzeInt[T types.Integer](vals []T, checkUnique bool) *Context[T] {
	c := newContext[T]()
	c.Lvl = MAX_LEVEL
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

// AnalyzeFloat produces statistics about float64 and float32 vectors.
func AnalyzeFloat[T types.Float](vals []T, checkUnique, checkALP bool) *Context[T] {
	c := newContext[T]()
	c.Lvl = MAX_LEVEL
	c.PhyBits = util.SizeOf[T]() * 8
	c.UseBits = c.PhyBits
	if len(vals) == 0 {
		return c
	}
	c.NumValues = len(vals)

	if c.PhyBits == 64 {
		minv, maxv, nruns := analyze.AnalyzeFloat64(util.ReinterpretSlice[T, float64](vals))
		c.Min, c.Max, c.NumRuns = T(minv), T(maxv), nruns
	} else {
		minv, maxv, nruns := analyze.AnalyzeFloat32(util.ReinterpretSlice[T, float32](vals))
		c.Min, c.Max, c.NumRuns = T(minv), T(maxv), nruns
	}

	c.NumUnique = c.NumRuns

	// run more analysis steps when const encoding is excluded
	if c.NumRuns > 1 {
		// let ALP estimate the best scheme, avoid ALP-in-ALP nesting
		if checkALP {
			// analyze full vector for compatibility, ALP will do its own sampling
			if c.PhyBits == 64 {
				c.Alp = alp.Analyze[T, int64](vals)
			} else {
				c.Alp = alp.Analyze[T, int32](vals)
			}
		}

		// count unique only if requested, prefer ALP over float dict
		// i.e. disable unique estimation when ALP is enabled which leads
		// to not selecting dict encoding at this level
		if !checkALP && checkUnique {
			c.NumUnique = max(1, c.estimateCardinality(vals))
		}
	}

	return c
}

func (c *Context[T]) estimateCardinality(vals []T) int {
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

func (c *Context[T]) buildUniqueArray(vals []T) int {
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

func (c *Context[T]) EligibleIntSchemes() []ContainerType {
	// constant only
	if c.NumRuns == 1 {
		return []ContainerType{TIntConstant}
	}
	// delta only with at least 3 values
	if c.Delta > 0 && c.NumValues > 2 {
		return []ContainerType{TIntDelta}
	}
	// raw always works
	schemes := []ContainerType{
		TIntRaw,
	}
	// bit-packed width must decrease
	if c.UseBits < c.PhyBits {
		schemes = append(schemes, TIntBitpacked)
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
		schemes = append(schemes, TIntRunEnd)
	}
	// dict makes only sense when more efficient than bit-packing
	if c.preferDict() {
		schemes = append(schemes, TIntDictionary)
	}
	return schemes
}

func (c *Context[T]) EligibleFloatSchemes() []ContainerType {
	// constant only
	if c.NumRuns == 1 {
		return []ContainerType{TFloatConstant}
	}

	// raw always works
	schemes := []ContainerType{
		TFloatRaw,
	}

	// use ALP only when requested in analysis step (otherwise scheme is invalid)
	switch c.Alp.Scheme {
	case alp.ALP_SCHEME:
		schemes = append(schemes, TFloatAlp)
	case alp.ALP_RD_SCHEME:
		schemes = append(schemes, TFloatAlpRd)
	}

	// run-end requires avg run lengths >= 2
	if c.preferRunEnd() {
		schemes = append(schemes, TFloatRunEnd)
	}

	// dict makes only sense when more efficient than raw
	if c.preferDict() {
		schemes = append(schemes, TFloatDictionary)
	}

	return schemes
}

func (c *Context[T]) preferDict() bool {
	return c.NumUnique <= hashprobe.MAX_DICT_LIMIT && c.dictCosts() < c.bitPackCosts()
}

func (c *Context[T]) preferRunEnd() bool {
	return c.NumRuns*RUN_END_THRESHOLD <= c.NumValues && c.runEndCosts() < c.bitPackCosts()
}

func (c *Context[T]) dictCosts() int {
	return dictCosts(c.NumValues, c.UseBits, c.NumUnique)
}

func (c *Context[T]) bitPackCosts() int {
	return bitPackCosts(c.NumValues, c.UseBits)
}

func (c *Context[T]) runEndCosts() int {
	return runEndCosts(c.NumValues, c.NumRuns, c.UseBits)
}

func (c *Context[T]) rawCosts() int {
	return 1 + num.UvarintLen(c.NumValues) + c.NumValues*c.PhyBits/8
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

func (c *Context[T]) MinMax() (any, any) {
	return c.Min, c.Max
}

func (c *Context[T]) Unique() int {
	return c.NumUnique
}

func (c *Context[T]) Close() {
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
	c.Alp = alp.Analysis{}
	putContext(c)
}

type ContextFactory struct {
	i64Pool sync.Pool
	i32Pool sync.Pool
	i16Pool sync.Pool
	i8Pool  sync.Pool
	u64Pool sync.Pool
	u32Pool sync.Pool
	u16Pool sync.Pool
	u8Pool  sync.Pool
	f64Pool sync.Pool
	f32Pool sync.Pool
}

func newContext[T types.Number]() *Context[T] {
	switch (any(T(0))).(type) {
	case int64:
		return contextFactory.i64Pool.Get().(*Context[T])
	case int32:
		return contextFactory.i32Pool.Get().(*Context[T])
	case int16:
		return contextFactory.i16Pool.Get().(*Context[T])
	case int8:
		return contextFactory.i8Pool.Get().(*Context[T])
	case uint64:
		return contextFactory.u64Pool.Get().(*Context[T])
	case uint32:
		return contextFactory.u32Pool.Get().(*Context[T])
	case uint16:
		return contextFactory.u16Pool.Get().(*Context[T])
	case uint8:
		return contextFactory.u8Pool.Get().(*Context[T])
	case float64:
		return contextFactory.f64Pool.Get().(*Context[T])
	case float32:
		return contextFactory.f32Pool.Get().(*Context[T])
	default:
		return nil
	}
}

func putContext[T types.Number](c *Context[T]) {
	switch (any(T(0))).(type) {
	case int64:
		contextFactory.i64Pool.Put(c)
	case int32:
		contextFactory.i32Pool.Put(c)
	case int16:
		contextFactory.i16Pool.Put(c)
	case int8:
		contextFactory.i8Pool.Put(c)
	case uint64:
		contextFactory.u64Pool.Put(c)
	case uint32:
		contextFactory.u32Pool.Put(c)
	case uint16:
		contextFactory.u16Pool.Put(c)
	case uint8:
		contextFactory.u8Pool.Put(c)
	case float64:
		contextFactory.f64Pool.Put(c)
	case float32:
		contextFactory.f32Pool.Put(c)
	}
}

var contextFactory = ContextFactory{
	i64Pool: sync.Pool{
		New: func() any { return new(Context[int64]) },
	},
	i32Pool: sync.Pool{
		New: func() any { return new(Context[int32]) },
	},
	i16Pool: sync.Pool{
		New: func() any { return new(Context[int16]) },
	},
	i8Pool: sync.Pool{
		New: func() any { return new(Context[int8]) },
	},
	u64Pool: sync.Pool{
		New: func() any { return new(Context[uint64]) },
	},
	u32Pool: sync.Pool{
		New: func() any { return new(Context[uint32]) },
	},
	u16Pool: sync.Pool{
		New: func() any { return new(Context[uint16]) },
	},
	u8Pool: sync.Pool{
		New: func() any { return new(Context[uint8]) },
	},
	f64Pool: sync.Pool{
		New: func() any { return new(Context[float64]) },
	},
	f32Pool: sync.Pool{
		New: func() any { return new(Context[float32]) },
	},
}
