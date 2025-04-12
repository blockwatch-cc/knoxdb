// Copyright (c) 2025 Blockwatch Data Inc.
// Author: abdul@blockwatch.cc

package encode

import (
	"sync"

	"blockwatch.cc/knoxdb/internal/arena"
	"blockwatch.cc/knoxdb/internal/encode/alp"
	"blockwatch.cc/knoxdb/internal/encode/hashprobe"
	"blockwatch.cc/knoxdb/internal/filter/llb"
	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/util"
)

type FloatContext[T types.Float] struct {
	Min        T                // vector minimum
	Max        T                // vector maximum
	PhyBits    int              // float bit width
	NumUnique  int              // vector cardinality (hint, may not be precise)
	NumRuns    int              // vector runs
	NumValues  int              // vector length
	AlpScheme  int              // suggested ALP encoder scheme
	Sample     []T              // data sample (optional)
	SampleCtx  *FloatContext[T] // sample analysis
	FreeSample bool             // hint whether sample may be reused
}

// AnalyzeFloat produces statistics about slice vals which are used to
// find the most efficient encoding scheme.
func AnalyzeFloat[T types.Float](vals []T, checkUnique bool) *FloatContext[T] {
	c := newFloatContext[T]()
	c.Min = vals[0]
	c.Max = vals[0]
	c.NumRuns = 1
	c.NumValues = len(vals)
	c.PhyBits = SizeOf[T]() * 8
	for i, v := range vals[1:] {
		if v < c.Min {
			c.Min = v
		} else if v > c.Max {
			c.Max = v
		}
		c.NumRuns += util.Bool2int(vals[i] != v)
	}

	// TODO: avoid on ALP nested float containers
	// let ALP estimate the best scheme
	if c.Min != c.Max {
		c.AlpScheme = alp.Scheme(vals)
	}

	// count unique only if necessary
	c.NumUnique = c.NumRuns
	if checkUnique {
		c.NumUnique = c.estimateCardinality(vals)
	}
	return c
}

func (c *FloatContext[T]) estimateCardinality(vals []T) int {
	var scratch [256]byte // need 256 byte scratch space
	unique, _ := llb.NewFilterBuffer(scratch[:], 8)
	if SizeOf[T]() == 8 {
		unique.AddMultiUint64(util.ReinterpretSlice[T, uint64](vals))
	} else {
		unique.AddMultiUint32(util.ReinterpretSlice[T, uint32](vals))
	}
	card := int(unique.Cardinality())
	return card
}

func (c *FloatContext[T]) EligibleSchemes(lvl int) []FloatContainerType {
	// constant only
	if c.Min == c.Max {
		return []FloatContainerType{TFloatConstant}
	}

	// raw always works
	schemes := []FloatContainerType{
		TFloatRaw,
	}

	// use ALP as top-level scheme only
	if lvl == MAX_CASCADE {
		switch c.AlpScheme {
		case alp.AlpScheme:
			schemes = append(schemes, TFloatAlp)
		case alp.AlpRdScheme:
			schemes = append(schemes, TFloatAlpRd)
		}
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

func (c *FloatContext[T]) preferDict() bool {
	dcost := dictCosts(c.NumValues, c.PhyBits, c.NumUnique)
	rcost := 5 + c.NumValues*c.PhyBits/8
	return c.NumUnique <= hashprobe.MAX_DICT_LIMIT && dcost < rcost
}

func (c *FloatContext[T]) preferRunEnd() bool {
	rcost := runEndCosts(c.NumValues, c.NumRuns, c.PhyBits)
	bcost := bitPackCosts(c.NumValues, c.PhyBits)
	return c.NumRuns*2 <= c.NumValues && rcost < bcost
}

func (c *FloatContext[T]) Close() {
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
