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
	Sample     []T              // data sample (optional)
	SampleCtx  *FloatContext[T] // sample analysis
	FreeSample bool             // hint whether sample may be reused
	AlpEncoder *alp.Encoder[T]  // ALP encoder state
}

// AnalyzeFloat produces statistics about slice vals which are used to
// find the most efficient encoding scheme.
func AnalyzeFloat[T types.Float](vals []T, checkUnique, checkALP bool) *FloatContext[T] {
	c := newFloatContext[T]()
	c.PhyBits = SizeOf[T]() * 8
	if len(vals) == 0 {
		return c
	}
	c.Min = vals[0]
	c.Max = vals[0]
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
	c.NumUnique = c.NumRuns

	// run more analysis steps when const encoding is excluded
	if c.Min != c.Max {
		// let ALP estimate the best scheme, avoid ALP-in-ALP nesting
		if checkALP {
			// analyze full vector for compatibility, ALP will do its own sampling
			c.AlpEncoder = alp.NewEncoder[T]().Analyze(vals)
		}

		// count unique only if requested, prefer ALP over float dict
		// Note: float dict construction via hashprobe requires the
		// upper bits of the multiplicative hash to avoid excessive
		// collisions, however, we just mask out the lower 16 bits.
		if !checkALP && checkUnique {
			c.NumUnique = max(1, c.estimateCardinality(vals))
		}
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

	// use ALP only when requested in analysis step (otherwise encoder is nil)
	if c.AlpEncoder != nil {
		switch c.AlpEncoder.State().Scheme {
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
	dcost := c.dictCosts()
	rcost := c.rawCosts()
	return c.NumUnique <= hashprobe.MAX_DICT_LIMIT && dcost < rcost
}

func (c *FloatContext[T]) preferRunEnd() bool {
	rcost := c.runEndCosts()
	bcost := c.bitPackCosts()
	return c.NumRuns*2 <= c.NumValues && rcost < bcost
}

func (c *FloatContext[T]) rawCosts() int {
	return 5 + c.NumValues*c.PhyBits/8
}

func (c *FloatContext[T]) dictCosts() int {
	return dictCosts(c.NumValues, c.PhyBits, c.NumUnique)
}

func (c *FloatContext[T]) bitPackCosts() int {
	return bitPackCosts(c.NumValues, c.PhyBits)
}

func (c *FloatContext[T]) runEndCosts() int {
	return runEndCosts(c.NumValues, c.NumRuns, c.PhyBits)
}

func (c *FloatContext[T]) Close() {
	if c.SampleCtx != nil {
		c.SampleCtx.Close()
		c.SampleCtx = nil
	}
	if c.Sample != nil {
		if c.FreeSample {
			// clear(c.Sample)
			arena.FreeT(c.Sample)
		}
		c.FreeSample = false
		c.Sample = nil
	}
	if c.AlpEncoder != nil {
		c.AlpEncoder.Close()
		c.AlpEncoder = nil
	}
	c.Min = 0
	c.Max = 0
	c.PhyBits = 0
	c.NumUnique = 0
	c.NumRuns = 0
	c.NumValues = 0
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
