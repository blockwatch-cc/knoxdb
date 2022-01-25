// Copyright (c) 2020-2021 Blockwatch Data Inc.
// Original: InfluxData
//
package bloomVec

// NOTE:
// This package implements a limited bloom filter implementation loosely based on
// Will Fitzgerald's bloom & bitset packages. It uses a zero-allocation xxhash
// implementation.
//
// This also optimizes the filter by always using a bitset size with a power of 2.

import (
	"encoding/binary"
	"fmt"
	"math"

	"blockwatch.cc/knoxdb/hash/xxHash32"
	"blockwatch.cc/knoxdb/hash/xxhashVec"
)

const xxHash32Seed = 1312

// Filter represents a bloom filter.
type Filter struct {
	k    uint32
	mask uint32
	b    []byte
}

// NewFilter returns a new instance of Filter using m bits and k hash functions.
// If m is not a power of two then it is rounded to the next highest power of 2.
func NewFilter(m int) *Filter {
	m = pow2(m)
	return &Filter{k: 4, b: make([]byte, m>>3), mask: uint32(m - 1)}
}

// NewFilterBuffer returns a new instance of a filter using a backing buffer.
// The buffer length MUST be a power of 2.
func NewFilterBuffer(buf []byte) (*Filter, error) {
	m := pow2(len(buf) * 8)
	if m != len(buf)*8 {
		return nil, fmt.Errorf("bloom.Filter: buffer bit count must be a power of two: %d/%d", len(buf)*8, m)
	}
	return &Filter{k: 4, b: buf, mask: uint32(m - 1)}, nil
}

// Len returns the number of bits used in the filter.
func (f *Filter) Len() uint { return uint(len(f.b)) }

// K returns the number of hash functions used in the filter.
func (f *Filter) K() uint32 { return f.k }

// Bytes returns the underlying backing slice.
func (f *Filter) Bytes() []byte { return f.b }

// Reset all bits in the filter.
func (f *Filter) Reset() {
	f.b[0] = 0
	for bp := 1; bp < len(f.b); bp *= 2 {
		copy(f.b[bp:], f.b[:bp])
	}
}

// Clone returns a copy of f.
func (f *Filter) Clone() *Filter {
	other := &Filter{k: f.k, b: make([]byte, len(f.b)), mask: f.mask}
	copy(other.b, f.b)
	return other
}

// Add inserts data to the filter.
func (f *Filter) Add(v []byte) {
	h := hash(v, xxHash32Seed)
	for i := uint32(0); i < f.k; i++ {
		loc := f.location(h, i)
		f.b[loc>>3] |= 1 << (loc & 7)
	}
}

// AddMany inserts multiple data points to the filter.
func (f *Filter) AddMany(l [][]byte) {
	for _, v := range l {
		h := hash(v, xxHash32Seed)
		for i := uint32(0); i < f.k; i++ {
			loc := f.location(h, i)
			f.b[loc>>3] |= 1 << (loc & 7)
		}
	}
}

// AddMany inserts multiple data points to the filter.
func (f *Filter) AddManyUint16(data []uint16) {
	for _, v := range data {
		h := HashUint16(v)
		for i := uint32(0); i < f.k; i++ {
			loc := f.location(h, i)
			f.b[loc>>3] |= 1 << (loc & 7)
		}
	}
}

// AddMany inserts multiple data points to the filter.
func (f *Filter) AddManyInt16(data []int16) {
	for _, v := range data {
		h := HashInt16(v)
		for i := uint32(0); i < f.k; i++ {
			loc := f.location(h, i)
			f.b[loc>>3] |= 1 << (loc & 7)
		}
	}
}

// AddMany inserts multiple data points to the filter.
func (f *Filter) AddManyUint32(data []uint32) {
	filterAddManyUint32(f, data, xxHash32Seed)
}

// AddMany inserts multiple data points to the filter.
func (f *Filter) AddManyInt32(data []int32) {
	filterAddManyInt32(f, data, xxHash32Seed)
}

// AddMany inserts multiple data points to the filter.
func (f *Filter) AddManyUint64(data []uint64) {
	filterAddManyUint64(f, data, xxHash32Seed)
}

// AddMany inserts multiple data points to the filter.
func (f *Filter) AddManyInt64(data []int64) {
	filterAddManyInt64(f, data, xxHash32Seed)
}

// AddMany inserts multiple data points to the filter.
func (f *Filter) AddManyFloat64(data []float64) {
	for _, v := range data {
		h := HashFloat64(v)
		for i := uint32(0); i < f.k; i++ {
			loc := f.location(h, i)
			f.b[loc>>3] |= 1 << (loc & 7)
		}
	}
}

// AddMany inserts multiple data points to the filter.
func (f *Filter) AddManyFloat32(data []float32) {
	for _, v := range data {
		h := HashFloat32(v)
		for i := uint32(0); i < f.k; i++ {
			loc := f.location(h, i)
			f.b[loc>>3] |= 1 << (loc & 7)
		}
	}
}

// Contains returns true if the filter possibly contains v.
// Returns false if the filter definitely does not contain v.
func (f *Filter) Contains(v []byte) bool {
	h := hash(v, xxHash32Seed)
	for i := uint32(0); i < f.k; i++ {
		loc := f.location(h, i)
		if f.b[loc>>3]&(1<<(loc&7)) == 0 {
			return false
		}
	}
	return true
}

// Contains returns true if the filter possibly contains v.
// Returns false if the filter definitely does not contain v.
func (f *Filter) ContainsUint16(v uint16) bool {
	var buf [2]byte
	binary.LittleEndian.PutUint16(buf[:], v)
	h := hash(buf[:], xxHash32Seed)
	for i := uint32(0); i < f.k; i++ {
		loc := f.location(h, i)
		if f.b[loc>>3]&(1<<(loc&7)) == 0 {
			return false
		}
	}
	return true
}

// Contains returns true if the filter possibly contains v.
// Returns false if the filter definitely does not contain v.
func (f *Filter) ContainsInt16(v int16) bool {
	var buf [2]byte
	binary.LittleEndian.PutUint16(buf[:], uint16(v))
	h := hash(buf[:], xxHash32Seed)
	for i := uint32(0); i < f.k; i++ {
		loc := f.location(h, i)
		if f.b[loc>>3]&(1<<(loc&7)) == 0 {
			return false
		}
	}
	return true
}

// Contains returns true if the filter possibly contains v.
// Returns false if the filter definitely does not contain v.
func (f *Filter) ContainsInt32(v int32) bool {
	var h [2]uint32
	h[0] = xxhashVec.XXHash32Int32(v, xxHash32Seed)
	h[1] = xxhashVec.XXHash32Int32(v, 0)
	for i := uint32(0); i < f.k; i++ {
		loc := f.location(h, i)
		if f.b[loc>>3]&(1<<(loc&7)) == 0 {
			return false
		}
	}
	return true
}

// Contains returns true if the filter possibly contains v.
// Returns false if the filter definitely does not contain v.
func (f *Filter) ContainsUint32(v uint32) bool {
	var h [2]uint32
	h[0] = xxhashVec.XXHash32Uint32(v, xxHash32Seed)
	h[1] = xxhashVec.XXHash32Uint32(v, 0)
	for i := uint32(0); i < f.k; i++ {
		loc := f.location(h, i)
		if f.b[loc>>3]&(1<<(loc&7)) == 0 {
			return false
		}
	}
	return true
}

// Contains returns true if the filter possibly contains v.
// Returns false if the filter definitely does not contain v.
func (f *Filter) ContainsUint64(v uint64) bool {
	var h [2]uint32
	h[0] = xxhashVec.XXHash32Uint64(v, xxHash32Seed)
	h[1] = xxhashVec.XXHash32Uint64(v, 0)
	for i := uint32(0); i < f.k; i++ {
		loc := f.location(h, i)
		if f.b[loc>>3]&(1<<(loc&7)) == 0 {
			return false
		}
	}
	return true
}

// Contains returns true if the filter possibly contains v.
// Returns false if the filter definitely does not contain v.
func (f *Filter) ContainsInt64(v int64) bool {
	var h [2]uint32
	h[0] = xxhashVec.XXHash32Int64(v, xxHash32Seed)
	h[1] = xxhashVec.XXHash32Int64(v, 0)
	for i := uint32(0); i < f.k; i++ {
		loc := f.location(h, i)
		if f.b[loc>>3]&(1<<(loc&7)) == 0 {
			return false
		}
	}
	return true
}

// Contains returns true if the filter possibly contains v.
// Returns false if the filter definitely does not contain v.
func (f *Filter) ContainsFloat64(v float64) bool {
	u := math.Float64bits(v)
	var h [2]uint32
	h[0] = xxhashVec.XXHash32Uint64(u, xxHash32Seed)
	h[1] = xxhashVec.XXHash32Uint64(u, 0)
	for i := uint32(0); i < f.k; i++ {
		loc := f.location(h, i)
		if f.b[loc>>3]&(1<<(loc&7)) == 0 {
			return false
		}
	}
	return true
}

// Contains returns true if the filter possibly contains v.
// Returns false if the filter definitely does not contain v.
func (f *Filter) ContainsFloat32(v float32) bool {
	u := math.Float32bits(v)
	var h [2]uint32
	h[0] = xxhashVec.XXHash32Uint32(u, xxHash32Seed)
	h[1] = xxhashVec.XXHash32Uint32(u, 0)
	for i := uint32(0); i < f.k; i++ {
		loc := f.location(h, i)
		if f.b[loc>>3]&(1<<(loc&7)) == 0 {
			return false
		}
	}
	return true
}

// ContainsHash returns true if the filter contains hash value h.
// Returns false if the filter definitely does not contain h.
func (f *Filter) ContainsHash(h [2]uint32) bool {
	for i := uint32(0); i < f.k; i++ {
		loc := f.location(h, i)
		if f.b[loc>>3]&(1<<(loc&7)) == 0 {
			return false
		}
	}
	return true
}

// ContainsAnyHash returns true if the filter contains any hash value in l.
// Returns false if the filter definitely does not contain any hash in l.
func (f *Filter) ContainsAnyHash(l [][2]uint32) bool {
hash_scan:
	for _, h := range l {
		for i := uint32(0); i < f.k; i++ {
			loc := f.location(h, i)
			if f.b[loc>>3]&(1<<(loc&7)) == 0 {
				continue hash_scan
			}
		}
		return true
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
	if len(f.b) != len(other.b) {
		return fmt.Errorf("bloom.Filter.Merge(): m mismatch: %d <> %d", len(f.b), len(other.b))
	} else if f.k != other.k {
		return fmt.Errorf("bloom.Filter.Merge(): k mismatch: %d <> %d", f.b, other.b)
	}
	filterMerge(f.b, other.b)
	return nil
}

// location returns the ith hashed location using two hash values.
func (f *Filter) location(h [2]uint32, i uint32) uint32 {
	return (h[0] + h[1]*i) & f.mask
}

// Estimate returns an estimated bit count and hash count given the element count and false positive rate.
func Estimate(n uint64, p float64) (m uint64, k uint64) {
	m = uint64(math.Ceil(-1 * float64(n) * math.Log(p) / math.Pow(math.Log(2), 2)))
	k = uint64(math.Ceil(math.Log(2) * float64(m) / float64(n)))
	return m, k
}

func hash(data []byte, seed uint32) [2]uint32 {
	return [2]uint32{xxHash32.Checksum(data, seed), xxHash32.Checksum(data, 0)}
}

func Hash(data []byte) [2]uint32 {
	return hash(data, xxHash32Seed)
}

func HashUint16(v uint16) [2]uint32 {
	var buf [2]byte
	binary.LittleEndian.PutUint16(buf[:], v)
	return [2]uint32{
		xxHash32.Checksum(buf[:], xxHash32Seed),
		xxHash32.Checksum(buf[:], 0),
	}
}

func HashInt16(v int16) [2]uint32 {
	var buf [2]byte
	binary.LittleEndian.PutUint16(buf[:], uint16(v))
	return [2]uint32{
		xxHash32.Checksum(buf[:], xxHash32Seed),
		xxHash32.Checksum(buf[:], 0),
	}
}

func HashUint32(v uint32) [2]uint32 {
	return [2]uint32{
		xxhashVec.XXHash32Uint32(v, xxHash32Seed),
		xxhashVec.XXHash32Uint32(v, 0),
	}
}

func HashInt32(v int32) [2]uint32 {
	return [2]uint32{
		xxhashVec.XXHash32Int32(v, xxHash32Seed),
		xxhashVec.XXHash32Int32(v, 0),
	}
}

func HashUint64(v uint64) [2]uint32 {
	return [2]uint32{
		xxhashVec.XXHash32Uint64(v, xxHash32Seed),
		xxhashVec.XXHash32Uint64(v, 0),
	}
}

func HashInt64(v int64) [2]uint32 {
	return [2]uint32{
		xxhashVec.XXHash32Int64(v, xxHash32Seed),
		xxhashVec.XXHash32Int64(v, 0),
	}
}

func HashFloat64(v float64) [2]uint32 {
	u := math.Float64bits(v)
	return [2]uint32{
		xxhashVec.XXHash32Uint64(u, xxHash32Seed),
		xxhashVec.XXHash32Uint64(u, 0),
	}
}

func HashFloat32(v float32) [2]uint32 {
	u := math.Float32bits(v)
	return [2]uint32{
		xxhashVec.XXHash32Uint32(u, xxHash32Seed),
		xxhashVec.XXHash32Uint32(u, 0),
	}
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
