// Copyright (c) 2022-2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package bloom

// This package implements a custom bloom filter implementation loosely based on
// Will Fitzgerald's bloom & bitset packages. It uses a vectorized zero-allocation
// xxhash32 implementation, limits the filter size to powers of 2 and fixes the
// number of hash functions to 4. The empirical false positive rate of this filter
// is pow(1 - exp(-4 / (m / n)), 4). A good way is to dimension the filter based
// on set cardinality while applying a scaling factor (multiply by 8 because
// NewFilter() counts in bits). This way factor directly controls filter size
// in bytes per value:
//
// factor   p          p(%)      false positive rate
// -------------------------------------------------
// 1        0.023968   2.4%      1 in 42
// 2        0.002394   0.2%      1 in 418
// 3        0.000555   0.05%     1 in 1,800
// 4        0.000190   0.02%     1 in 5,246
// 5        0.000082   0.008%    1 in 12,194
//
// see https://hur.st/bloomfilter

import (
	"bytes"
	"fmt"
	"io"
	"math"
	"unsafe"

	"blockwatch.cc/knoxdb/pkg/util"
	"github.com/zeebo/xxh3"
)

type containsFunc func(*Filter, uint32, uint32) bool
type addFunc func(*Filter, uint32, uint32)

// Filter represents a bloom filter.
type Filter struct {
	k        uint32
	mask     uint32
	buf      []byte
	bits     []byte
	add      addFunc
	contains containsFunc
}

// NewFilter returns a new instance of Filter using m bits.
// If m is not a power of two then it is rounded to the next highest power of 2.
func NewFilter(m int) *Filter {
	m = pow2(m)
	buf := make([]byte, 1+m>>3)
	buf[0] = 4
	return &Filter{
		k:        4,
		buf:      buf,
		bits:     buf[1:],
		mask:     uint32(m - 1),
		add:      addUnroll4,
		contains: containsUnroll4,
	}
}

func NewFilterEstimate(n int, p float64) *Filter {
	m, k := Estimate(uint64(n), p)
	m = pow2(m)
	buf := make([]byte, 1+m>>3)
	buf[0] = byte(k)
	af, cf := addGeneric, containsGeneric
	if k == 4 {
		af, cf = addUnroll4, containsUnroll4
	}
	return &Filter{
		k:        uint32(k),
		buf:      buf,
		bits:     buf[1:],
		mask:     uint32(m - 1),
		add:      af,
		contains: cf,
	}
}

// NewFilterBuffer returns a new instance of a filter using a backing buffer.
// The buffer length MUST be a power of 2.
func NewFilterBuffer(buf []byte) (*Filter, error) {
	l := len(buf) - 1
	m := pow2(l * 8)
	if m != l*8 {
		return nil, fmt.Errorf("bloom: buffer bit count must be a power of two: %d/%d", l*8, m)
	}
	af, cf := addGeneric, containsGeneric
	if buf[0] == 4 {
		af, cf = addUnroll4, containsUnroll4
	}
	return &Filter{
		k:        uint32(buf[0]),
		buf:      buf,
		bits:     buf[1:],
		mask:     uint32(m - 1),
		add:      af,
		contains: cf,
	}, nil
}

// Len returns the number of bits used in the filter.
func (f *Filter) Len() uint { return uint(len(f.bits)) }

// K returns the number of hash functions used in the filter.
func (f *Filter) K() uint32 { return f.k }

// Bytes returns the underlying backing slice.
func (f *Filter) Bytes() []byte { return f.buf }

// Reset all bits in the filter.
func (f *Filter) Reset() {
	clear(f.bits)
}

// Clone returns a copy of f.
func (f *Filter) Clone() *Filter {
	buf := bytes.Clone(f.buf)
	return &Filter{
		k:        f.k,
		buf:      buf,
		bits:     buf[1:],
		mask:     f.mask,
		add:      f.add,
		contains: f.contains,
	}
}

// location returns the ith hashed location using two hash values.
func (f *Filter) location(h0, h1 uint32, i uint32) uint32 {
	return (h0 + h1*i) & f.mask
}

// contains4 is a loop unrolled version for k=4
func containsUnroll4(f *Filter, h0, h1 uint32) bool {
	if f.bits[(h0&f.mask)>>3]&(1<<(h0&7)) == 0 {
		return false
	}
	h0 += h1
	if f.bits[(h0&f.mask)>>3]&(1<<(h0&7)) == 0 {
		return false
	}
	h0 += h1
	if f.bits[(h0&f.mask)>>3]&(1<<(h0&7)) == 0 {
		return false
	}
	h0 += h1
	return f.bits[(h0&f.mask)>>3]&(1<<(h0&7)) != 0
}

func containsGeneric(f *Filter, h0, h1 uint32) bool {
	for i := uint32(0); i < f.k; i++ {
		loc := f.location(h0, h1, i)
		if f.bits[loc>>3]&(1<<(loc&7)) == 0 {
			return false
		}
	}
	return true
}

func addUnroll4(f *Filter, h0, h1 uint32) {
	f.bits[(h0&f.mask)>>3] |= 1 << (h0 & 7)
	h0 += h1
	f.bits[(h0&f.mask)>>3] |= 1 << (h0 & 7)
	h0 += h1
	f.bits[(h0&f.mask)>>3] |= 1 << (h0 & 7)
	h0 += h1
	f.bits[(h0&f.mask)>>3] |= 1 << (h0 & 7)
}

func addGeneric(f *Filter, h0, h1 uint32) {
	for i := uint32(0); i < f.k; i++ {
		loc := f.location(h0, h1, i)
		f.bits[loc>>3] |= 1 << (loc & 7)
	}
}

// Contains returns true if the filter possible contains the
// encoded (pre-hashed) value. This implements the common
// interface filter.Filter used by query matchers.
func (f *Filter) Contains(h uint64) bool {
	return f.contains(f, uint32(h), uint32(h>>32))
}

// ContainsAny returns true if the filter contains any hash value in l.
// Returns false if the filter definitely does not contain any hash in l.
// func (f *Filter) ContainsAny(l []filter.HashValue) bool {
func (f *Filter) ContainsAny(l []uint64) bool {
	for _, h := range l {
		if f.contains(f, uint32(h), uint32(h>>32)) {
			return true
		}
	}
	return false
}

// Add inserts data to the filter.
func (f *Filter) Add(v []byte) {
	h := xxh3.Hash(v)
	f.add(f, uint32(h), uint32(h>>32))
}

// AddMany inserts multiple data points to the filter.
func (f *Filter) AddMany(src [][]byte) {
	for _, v := range src {
		h := xxh3.Hash(v)
		f.add(f, uint32(h), uint32(h>>32))
	}
}

// AddManyUint8 inserts multiple data points to the filter.
func (f *Filter) AddManyUint8(src []byte) {
	for _, v := range src {
		h := xxh3.Hash((*[1]byte)(unsafe.Pointer(&v))[:])
		f.add(f, uint32(h), uint32(h>>32))
	}
}

// AddManyUint16 inserts multiple data points to the filter.
func (f *Filter) AddManyUint16(src []uint16) {
	for _, v := range src {
		h := xxh3.Hash((*[2]byte)(unsafe.Pointer(&v))[:])
		f.add(f, uint32(h), uint32(h>>32))
	}
}

// AddManyUint32 inserts multiple data points to the filter.
func (f *Filter) AddManyUint32(src []uint32) {
	for _, v := range src {
		h := xxh3.Hash((*[4]byte)(unsafe.Pointer(&v))[:])
		f.add(f, uint32(h), uint32(h>>32))
	}
}

// AddManyUint64 inserts multiple data points to the filter.
func (f *Filter) AddManyUint64(src []uint64) {
	for _, v := range src {
		h := xxh3.Hash((*[8]byte)(unsafe.Pointer(&v))[:])
		f.add(f, uint32(h), uint32(h>>32))
	}
}

// AddManyFloat64 inserts multiple data points to the filter.
func (f *Filter) AddManyFloat64(src []float64) {
	f.AddManyUint64(util.ReinterpretSlice[float64, uint64](src))
}

// AddManyFloat32 inserts multiple data points to the filter.
func (f *Filter) AddManyFloat32(src []float32) {
	f.AddManyUint32(util.ReinterpretSlice[float32, uint32](src))
}

// Merge performs an in-place union of other into f.
// Returns an error if m or k of the filters differs.
func (f *Filter) Merge(other *Filter) error {
	if other == nil {
		return nil
	}

	// Ensure m & k fields match.
	if len(f.bits) != len(other.bits) {
		return fmt.Errorf("bloom.Merge(): m mismatch: %d <> %d", len(f.bits), len(other.bits))
	} else if f.k != other.k {
		return fmt.Errorf("bloom.Merge(): k mismatch: %d <> %d", f.k, other.k)
	}

	// Perform union.
	var n int
	for n < len(f.bits) {
		*(*uint64)(unsafe.Pointer(&f.bits[n])) |= *(*uint64)(unsafe.Pointer(&other.bits[n]))
		n += 8
	}
	for i := n; i < len(f.bits); i++ {
		f.bits[i] |= other.bits[i]
	}
	return nil
}

// Estimate returns an estimated bit count and hash count given the element count
// and false positive rate.
// TODO: adjust formula to fixed k = 4
func Estimate(n uint64, p float64) (m int, k int) {
	m = int(math.Ceil(-1 * float64(n) * math.Log(p) / math.Pow(math.Log(2), 2)))
	k = int(math.Ceil(math.Log(2) * float64(m) / float64(n)))
	return m, k
}

// pow2 returns the number that is the next highest power of 2.
// Returns v if it is a power of 2.
func pow2(v int) int {
	for i := 8; i < 1<<30; i *= 2 {
		if i >= v {
			return i
		}
	}
	panic("unreachable")
}

func (f *Filter) MarshalBinary() ([]byte, error) {
	return f.buf, nil
}

func (f *Filter) UnmarshalBinary(buf []byte) error {
	if len(buf) == 0 {
		return io.ErrShortBuffer
	}
	l := len(buf) - 1
	m := pow2(l * 8)
	if m != l*8 {
		return fmt.Errorf("bloom: buffer bit count must be a power of two: %d/%d", l*8, m)
	}
	f.k = uint32(buf[0])
	f.buf = bytes.Clone(buf)
	f.bits = f.buf[1:]
	f.mask = uint32(m - 1)
	if f.k == 4 {
		f.add = addUnroll4
		f.contains = containsUnroll4
	} else {
		f.add = addGeneric
		f.contains = containsGeneric
	}
	return nil
}
