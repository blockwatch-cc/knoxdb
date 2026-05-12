// Copyright (c) 2021-2026 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc, alex@blockwatch.cc

package llb

import (
	"fmt"
	"math"
	"math/bits"
)

const (
	precision = 14
	m         = uint32(1 << precision) // 16384
	alpha     = 0.7213 / (1 + 1.079/float64(m))
)

func beta(ez float64) float64 {
	zl := math.Log(ez + 1)
	return -0.370393911*ez +
		0.070471823*zl +
		0.17393686*zl*zl +
		0.16339839*zl*zl*zl +
		-0.09237745*math.Pow(zl, 4) +
		0.03738027*math.Pow(zl, 5) +
		-0.005384159*math.Pow(zl, 6) +
		0.00042419*math.Pow(zl, 7)
}

func regSumAndZeros(regs []uint8) (float64, float64) {
	var sum, ez float64
	for _, val := range regs {
		if val == 0 {
			ez++
		}

		// float64 pow is expensive, we can get 4-18x faster
		// with integer math; to prevent  although this clips precision as
		// ints saturate after a count of 63
		if val < 64 {
			tmp := float64(uint64(1) << val)
			sum += 1.0 / tmp
		} else {
			sum += 1.0 / math.Pow(2.0, float64(val))
		}
	}
	return float64(sum), float64(ez)
}

type LogLogBeta struct {
	prec  uint32
	m     uint32
	max   uint32
	maxX  uint32
	alpha float64
	buf   []uint8
}

// New returns a LogLogBeta with fixed precision 14.
func NewFilter() *LogLogBeta {
	return &LogLogBeta{
		prec:  precision, // fixed 14
		m:     m,
		max:   32 - precision,
		maxX:  math.MaxUint32 >> (32 - precision),
		alpha: alpha,
		buf:   make([]uint8, int(m)),
	}
}

// NewFilterWithPrecision creates a custom filter with
// user-define precision. Values between 8 and 16 are useful.
func NewFilterWithPrecision(p uint32) *LogLogBeta {
	m := uint32(1 << p)
	return &LogLogBeta{
		prec:  p,
		m:     m,
		max:   32 - p,
		maxX:  math.MaxUint32 >> (32 - p),
		alpha: 0.7213 / (1 + 1.079/float64(m)),
		buf:   make([]uint8, m),
	}
}

func NewFilterBuffer(buf []byte, p uint32) (*LogLogBeta, error) {
	m := uint32(1 << p)
	if len(buf) != int(m) {
		return nil, fmt.Errorf("llbVec: invalid buffer size %d for precision %d", len(buf), p)
	}
	return &LogLogBeta{
		prec:  p,
		m:     m,
		max:   32 - p,
		maxX:  math.MaxUint32 >> (32 - p),
		alpha: 0.7213 / (1 + 1.079/float64(m)),
		buf:   buf,
	}, nil
}

func (llb *LogLogBeta) P() uint32 {
	return llb.prec
}

func (llb *LogLogBeta) Add(hashes ...uint64) {
	for _, h := range hashes {
		x := uint32(h)
		k := x >> uint(llb.max)
		val := uint8(bits.LeadingZeros32((x<<llb.prec)^llb.maxX)) + 1
		if llb.buf[k] < val {
			llb.buf[k] = val
		}
	}
}

// Cardinality returns the number of unique elements added to the sketch
func (llb *LogLogBeta) Cardinality() uint64 {
	return llb_cardinality(llb)
}

// Merge creates the union between llb and other merging the
// result into llb.
func (llb *LogLogBeta) Merge(other *LogLogBeta) {
	if llb.prec != other.prec {
		return
	}
	if len(llb.buf) != len(other.buf) {
		return
	}
	llb_merge(llb.buf, other.buf)
}

func (llb *LogLogBeta) Bytes() []byte {
	return llb.buf
}
