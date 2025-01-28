// Copyright (c) 2022-2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package bloom

// This package implements a custom bloom filter implementation loosely based on
// Will Fitzgerald's bloom & bitset packages. It uses a vectorized zero-allocation
// xxhash32 implementation, limits the filter size to powers of 2 and fixes the
// number of hash functions to 4. The empirical false positive rate of this filter is
//
// - 2% for m = set cardinality * 2
// - 0.2% for m = set cardinality * 3
// - 0.02% for m = set cardinality * 4
//
// etc.

import (
	"bytes"
	"fmt"
	"io"
	"math"

	"blockwatch.cc/knoxdb/internal/hash"
)

type containsFunc func(*Filter, hash.HashValue) bool
type addFunc func(*Filter, hash.HashValue)

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
func (f *Filter) location(h hash.HashValue, i uint32) uint32 {
	return (h[0] + h[1]*i) & f.mask
}

// contains4 is a loop unrolled version for k=4
func containsUnroll4(f *Filter, h hash.HashValue) bool {
	a, b := h[0], h[1]
	if f.bits[(a&f.mask)>>3]&(1<<(a&7)) == 0 {
		return false
	}
	a += b
	if f.bits[(a&f.mask)>>3]&(1<<(a&7)) == 0 {
		return false
	}
	a += b
	if f.bits[(a&f.mask)>>3]&(1<<(a&7)) == 0 {
		return false
	}
	a += b
	return f.bits[(a&f.mask)>>3]&(1<<(a&7)) != 0
}

func containsGeneric(f *Filter, h hash.HashValue) bool {
	for i := uint32(0); i < f.k; i++ {
		loc := f.location(h, i)
		if f.bits[loc>>3]&(1<<(loc&7)) == 0 {
			return false
		}
	}
	return true
}

func addUnroll4(f *Filter, h hash.HashValue) {
	a, b := h[0], h[1]
	f.bits[(a&f.mask)>>3] |= 1 << (a & 7)
	a += b
	f.bits[(a&f.mask)>>3] |= 1 << (a & 7)
	a += b
	f.bits[(a&f.mask)>>3] |= 1 << (a & 7)
	a += b
	f.bits[(a&f.mask)>>3] |= 1 << (a & 7)
}

func addGeneric(f *Filter, h hash.HashValue) {
	for i := uint32(0); i < f.k; i++ {
		loc := f.location(h, i)
		f.bits[loc>>3] |= 1 << (loc & 7)
	}
}

func (f *Filter) AddHash(h hash.HashValue) {
	f.add(f, h)
}

// ContainsHash returns true if the filter contains hash value h.
// Returns false if the filter definitely does not contain h.
func (f *Filter) ContainsHash(h hash.HashValue) bool {
	return f.contains(f, h)
}

// Add inserts data to the filter.
func (f *Filter) Add(v []byte) {
	f.add(f, hash.Hash(v))
}

// AddMany inserts multiple data points to the filter.
func (f *Filter) AddMany(l [][]byte) {
	for _, v := range l {
		f.add(f, hash.Hash(v))
	}
}

// AddManyUint8 inserts multiple data points to the filter.
func (f *Filter) AddManyUint8(data []byte) {
	for _, v := range data {
		f.add(f, hash.Hash([]byte{v}))
	}
}

// AddManyUint16 inserts multiple data points to the filter.
func (f *Filter) AddManyUint16(data []uint16) {
	for _, v := range data {
		f.add(f, hash.HashUint16(v))
	}
}

// AddManyInt16 inserts multiple data points to the filter.
func (f *Filter) AddManyInt16(data []int16) {
	for _, v := range data {
		f.add(f, hash.HashInt16(v))
	}
}

// AddManyUint32 inserts multiple data points to the filter.
func (f *Filter) AddManyUint32(data []uint32) {
	filterAddManyUint32(f, data)
}

// AddManyInt32 inserts multiple data points to the filter.
func (f *Filter) AddManyInt32(data []int32) {
	filterAddManyInt32(f, data)
}

// AddManyUint64 inserts multiple data points to the filter.
func (f *Filter) AddManyUint64(data []uint64) {
	filterAddManyUint64(f, data)
}

// AddManyInt64 inserts multiple data points to the filter.
func (f *Filter) AddManyInt64(data []int64) {
	filterAddManyInt64(f, data)
}

// AddManyFloat64 inserts multiple data points to the filter.
func (f *Filter) AddManyFloat64(data []float64) {
	for _, v := range data {
		f.add(f, hash.HashFloat64(v))
	}
}

// AddManyFloat32 inserts multiple data points to the filter.
func (f *Filter) AddManyFloat32(data []float32) {
	for _, v := range data {
		f.add(f, hash.HashFloat32(v))
	}
}

// Contains returns true if the filter possibly contains v.
// Returns false if the filter definitely does not contain v.
func (f *Filter) Contains(v []byte) bool {
	return f.contains(f, hash.Hash(v))
}

// ContainsUint16 returns true if the filter possibly contains v.
// Returns false if the filter definitely does not contain v.
func (f *Filter) ContainsUint16(v uint16) bool {
	return f.contains(f, hash.HashUint16(v))
}

// ContainsInt16 returns true if the filter possibly contains v.
// Returns false if the filter definitely does not contain v.
func (f *Filter) ContainsInt16(v int16) bool {
	return f.contains(f, hash.HashInt16(v))
}

// ContainsInt32 returns true if the filter possibly contains v.
// Returns false if the filter definitely does not contain v.
func (f *Filter) ContainsInt32(v int32) bool {
	return f.contains(f, hash.HashInt32(v))
}

// ContainsUint32 returns true if the filter possibly contains v.
// Returns false if the filter definitely does not contain v.
func (f *Filter) ContainsUint32(v uint32) bool {
	return f.contains(f, hash.HashUint32(v))
}

// ContainsUint64 returns true if the filter possibly contains v.
// Returns false if the filter definitely does not contain v.
func (f *Filter) ContainsUint64(v uint64) bool {
	return f.contains(f, hash.HashUint64(v))
}

// ContainsInt64 returns true if the filter possibly contains v.
// Returns false if the filter definitely does not contain v.
func (f *Filter) ContainsInt64(v int64) bool {
	return f.contains(f, hash.HashInt64(v))
}

// ContainsFloat64 returns true if the filter possibly contains v.
// Returns false if the filter definitely does not contain v.
func (f *Filter) ContainsFloat64(v float64) bool {
	return f.contains(f, hash.HashFloat64(v))
}

// ContainsFloat32 returns true if the filter possibly contains v.
// Returns false if the filter definitely does not contain v.
func (f *Filter) ContainsFloat32(v float32) bool {
	return f.contains(f, hash.HashFloat32(v))
}

// ContainsAnyHash returns true if the filter contains any hash value in l.
// Returns false if the filter definitely does not contain any hash in l.
func (f *Filter) ContainsAnyHash(l []hash.HashValue) bool {
	for _, h := range l {
		if f.contains(f, h) {
			return true
		}
	}
	return false
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
	filterMerge(f.bits, other.bits)
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
