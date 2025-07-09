// Copyright (c) 2021 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

package llb

import (
	"fmt"
	"math"
	"math/bits"

	"blockwatch.cc/knoxdb/internal/hash/xxhash"
	"blockwatch.cc/knoxdb/internal/hash/xxhash32"
	"blockwatch.cc/knoxdb/pkg/util"
)

const (
	precision = 14
	m         = uint32(1 << precision) // 16384
	max       = 32 - precision
	maxX      = math.MaxUint32 >> max
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

func regSumAndZeros(registers []uint8) (float64, float64) {
	var sum, ez float32
	for _, val := range registers {
		if val == 0 {
			ez++
		}
		// sum += 1.0 / math.Pow(2.0, float64(val))
		tmp := float32(uint32(1) << val)
		sum += 1.0 / tmp
	}
	return float64(sum), float64(ez)
}

type LogLogBeta struct {
	precision uint32
	m         uint32
	max       uint32
	maxX      uint32
	alpha     float64
	buf       []uint8
}

// New returns a LogLogBeta
func NewFilter() *LogLogBeta {
	return &LogLogBeta{
		precision: precision,
		m:         m,
		max:       32 - precision,
		maxX:      math.MaxUint32 >> (32 - precision),
		alpha:     alpha,
		buf:       make([]uint8, int(m)),
	}
}

func NewFilterWithPrecision(p uint32) *LogLogBeta {
	m := uint32(1 << p)
	return &LogLogBeta{
		precision: p,
		m:         m,
		max:       32 - p,
		maxX:      math.MaxUint32 >> (32 - p),
		alpha:     0.7213 / (1 + 1.079/float64(m)),
		buf:       make([]uint8, m),
	}
}

func NewFilterBuffer(buf []byte, p uint32) (*LogLogBeta, error) {
	m := uint32(1 << p)
	if len(buf) != int(m) {
		return nil, fmt.Errorf("llbVec: invalid buffer size %d for precision %d", len(buf), p)
	}
	return &LogLogBeta{
		precision: p,
		m:         m,
		max:       32 - p,
		maxX:      math.MaxUint32 >> (32 - p),
		alpha:     0.7213 / (1 + 1.079/float64(m)),
		buf:       buf,
	}, nil
}

func (llb *LogLogBeta) P() uint32 {
	return llb.precision
}

func (llb *LogLogBeta) AddHash(x uint32) {
	k := x >> uint(llb.max)
	val := uint8(bits.LeadingZeros32((x<<llb.precision)^llb.maxX)) + 1
	if llb.buf[k] < val {
		llb.buf[k] = val
	}
}

func (llb *LogLogBeta) AddHashes(h []uint64) {
	for _, v := range h {
		llb.AddHash(uint32(v))
	}
}

func (llb *LogLogBeta) Add(value []byte) {
	llb.AddHash(xxhash32.Checksum(value, 0))
}

func (llb *LogLogBeta) AddUint32(val uint32) {
	llb.AddHash(xxhash.Hash32u32(val, 0))
}

func (llb *LogLogBeta) AddInt32(val int32) {
	llb.AddHash(xxhash.Hash32u32(uint32(val), 0))
}

func (llb *LogLogBeta) AddUint64(val uint64) {
	llb.AddHash(xxhash.Hash32u64(val, 0))
}

func (llb *LogLogBeta) AddInt64(val int64) {
	llb.AddHash(xxhash.Hash32u64(uint64(val), 0))
}

func (llb *LogLogBeta) AddMultiUint32(data []uint32) {
	llb_add_u32(llb, data, 0)
}

func (llb *LogLogBeta) AddMultiInt32(data []int32) {
	llb_add_u32(llb, util.ReinterpretSlice[int32, uint32](data), 0)
}

func (llb *LogLogBeta) AddMultiUint64(data []uint64) {
	llb_add_u64(llb, data, 0)
}

func (llb *LogLogBeta) AddMultiInt64(data []int64) {
	llb_add_u64(llb, util.ReinterpretSlice[int64, uint64](data), 0)
}

// Cardinality returns the number of unique elements added to the sketch
func (llb *LogLogBeta) Cardinality() uint64 {
	return llb_cardinality(llb)
}

// Merge takes another LogLogBeta and combines it with llb one, making llb the union of both.
func (llb *LogLogBeta) Merge(other *LogLogBeta) {
	if llb.precision != other.precision {
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
