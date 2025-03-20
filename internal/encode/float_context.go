// Copyright (c) 2025 Blockwatch Data Inc.
// Author: abdul@blockwatch.cc

package encode

import (
	"math"
	"sync"
	"unsafe"

	"blockwatch.cc/knoxdb/internal/arena"
	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/util"
)

type FloatContext[T types.Float] struct {
	Min         T                // vector minimum
	Max         T                // vector maximum
	PhyBits     int              // phy type bit width 8, 16, 32, 64
	UseBits     int              // used bits for bit-packing
	NumUnique   int              // vector cardinality (hint, may not be precise)
	NumRuns     int              // vector runs
	NumValues   int              // vector length
	Sample      []T              // data sample (optional)
	SampleCtx   *FloatContext[T] // sample analysis
	FreeSample  bool             // hint whether sample may be reused
	UniqueArray []T              // unique values as array (optional)
	UniqueMap   map[T]uint16     // unique values (optional)
}

// AnalyzeFloat produces statistics about slice vals which are used to
// find the most efficient encoding scheme.
func AnalyzeFloat[T types.Float](vals []T, checkUnique bool) *FloatContext[T] {
	c := newFloatContext[T]()
	c.Min = vals[0]
	c.Max = vals[0]
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
	}

	// count unique only if necessary
	doCountUnique := checkUnique && c.Min != c.Max
	c.NumUnique = min(c.NumRuns, int(math.Floor(float64(c.Max))-math.Floor(float64(c.Min))))

	switch c.PhyBits {
	case 64:
		c.UseBits = size[float64]()
		if doCountUnique {
			c.NumUnique = c.buildUniqueMap(vals)
		}
	case 32:
		c.UseBits = size[float32]()
		if doCountUnique {
			c.NumUnique = c.buildUniqueMap(vals)
		}
	}
	return c
}

func (c *FloatContext[T]) buildUniqueMap(vals []T) int {
	// construct unique values map
	if c.UniqueMap == nil {
		c.UniqueMap = make(map[T]uint16, len(vals))
	}

	for _, v := range vals {
		c.UniqueMap[v] += 1
	}

	var uniqueCount int
	for k, v := range c.UniqueMap {
		if v > 0 {
			c.UniqueArray = append(c.UniqueArray, k)
			uniqueCount++
		}
	}

	return uniqueCount
}

func (c *FloatContext[T]) EligibleSchemes() []FloatContainerType {
	// constant only
	if c.Min == c.Max {
		return []FloatContainerType{TFloatConstant}
	}
	// raw always works
	schemes := []FloatContainerType{
		TFloatRaw,
	}
	// run-end requires avg run lengths >= 2
	if c.NumRuns*2 <= c.NumValues {
		schemes = append(schemes, TFloatRunEnd)
	}
	// dict makes only sense if <64k entries, value range is reduced and card < 3/4
	if c.NumUnique <= 1<<16 && c.Max-c.Min > T(c.NumUnique) && c.NumUnique*4/3 < c.NumValues {
		schemes = append(schemes, TFloatDictionary)
	}

	// schemes = append(schemes, TFloatAlp)
	// schemes = append(schemes, TFloatAlpRd)

	return schemes
}

func (c *FloatContext[T]) Close() {
	clear(c.UniqueArray)
	if c.UniqueArray != nil {
		c.UniqueArray = c.UniqueArray[:0]
	}
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
	putFloatContext(c)
}

type FloatContextFactory struct {
	f64Pool sync.Pool
	f32Pool sync.Pool
}

func newFloatContext[T types.Float]() *FloatContext[T] {
	switch (any(T(0))).(type) {
	case float64:
		return floatContextFactory.f64Pool.Get().(*FloatContext[T])
	case float32:
		return floatContextFactory.f32Pool.Get().(*FloatContext[T])
	default:
		return nil
	}
}

func putFloatContext[T types.Float](c *FloatContext[T]) {
	switch (any(T(0))).(type) {
	case float64:
		floatContextFactory.f64Pool.Put(c)
	case float32:
		floatContextFactory.f32Pool.Put(c)
	}
}

var floatContextFactory = FloatContextFactory{
	f64Pool: sync.Pool{
		New: func() any { return new(FloatContext[float64]) },
	},
	f32Pool: sync.Pool{
		New: func() any { return new(FloatContext[float32]) },
	},
}
