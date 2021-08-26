package loglogbeta

import (
	"fmt"
	"math"
	"math/bits"

	// "blockwatch.cc/knoxdb/hash/metro"
	"blockwatch.cc/knoxdb/hash/xxhash"
	// "blockwatch.cc/knoxdb/hash/murmur3"
)

const (
	precision = 14
	m         = uint32(1 << precision) // 16384
	max       = 64 - precision
	maxX      = math.MaxUint64 >> max
	alpha     = 0.7213 / (1 + 1.079/float64(m))
)

func beta(ez float64) float64 {
	zl := math.Log(ez + 1)
	return -0.370393911*ez +
		0.070471823*zl +
		0.17393686*math.Pow(zl, 2) +
		0.16339839*math.Pow(zl, 3) +
		-0.09237745*math.Pow(zl, 4) +
		0.03738027*math.Pow(zl, 5) +
		-0.005384159*math.Pow(zl, 6) +
		0.00042419*math.Pow(zl, 7)
}

func regSumAndZeros(registers []uint8) (float64, float64) {
	sum, ez := 0.0, 0.0
	for _, val := range registers {
		if val == 0 {
			ez++
		}
		sum += 1.0 / math.Pow(2.0, float64(val))
	}
	return sum, ez
}

// LogLogBeta is a sketch for cardinality estimation based on LogLog counting
type LogLogBeta struct {
	precision uint
	m         uint32
	max       uint
	maxX      uint64
	alpha     float64
	buf       []uint8
}

// New returns a LogLogBeta
func NewFilter() *LogLogBeta {
	return &LogLogBeta{
		precision: precision,
		m:         m,
		max:       max,
		maxX:      maxX,
		alpha:     alpha,
		buf:       make([]uint8, int(m)),
	}
}

func NewFilterWithPrecision(p uint) *LogLogBeta {
	m := uint32(1 << p)
	return &LogLogBeta{
		precision: p,
		m:         m,
		max:       64 - p,
		maxX:      math.MaxUint64 >> (64 - p),
		alpha:     0.7213 / (1 + 1.079/float64(m)),
		buf:       make([]uint8, m),
	}
}

func NewFilterBuffer(buf []byte, p uint) (*LogLogBeta, error) {
	m := uint32(1 << p)
	if len(buf) != int(m) {
		return nil, fmt.Errorf("loglogbeta: invalid buffer size %d for precision %d", len(buf), p)
	}
	return &LogLogBeta{
		precision: p,
		m:         m,
		max:       64 - p,
		maxX:      math.MaxUint64 >> (64 - p),
		alpha:     0.7213 / (1 + 1.079/float64(m)),
		buf:       buf[:],
	}, nil
}

func (llb *LogLogBeta) P() uint {
	return llb.precision
}

// AddHash takes in a "hashed" value (bring your own hashing)
func (llb *LogLogBeta) AddHash(x uint64) {
	k := x >> uint(llb.max)
	val := uint8(bits.LeadingZeros64((x<<llb.precision)^llb.maxX)) + 1
	if llb.buf[k] < val {
		llb.buf[k] = val
	}
}

// Add inserts a value into the sketch
func (llb *LogLogBeta) Add(value []byte) {
	// llb.AddHash(metro.Hash64(value, 1337))
	llb.AddHash(xxhash.Sum64(value))
	// llb.AddHash(murmur3.Sum64WithSeed(value, 1337))
}

// Cardinality returns the number of unique elements added to the sketch
func (llb *LogLogBeta) Cardinality() uint64 {
	sum, ez := regSumAndZeros(llb.buf[:])
	m := float64(llb.m)
	return uint64(llb.alpha * m * (m - ez) / (beta(ez) + sum))
}

// Merge takes another LogLogBeta and combines it with llb one, making llb the union of both.
func (llb *LogLogBeta) Merge(other *LogLogBeta) {
	if llb.precision != other.precision {
		return
	}
	for i, v := range llb.buf {
		if v < other.buf[i] {
			llb.buf[i] = other.buf[i]
		}
	}
}

func (llb *LogLogBeta) Bytes() []byte {
	return llb.buf[:]
}
