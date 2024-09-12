// Copyright (c) 2022 Blockwatch Data Inc.
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
	"encoding/binary"
	"fmt"
	"math"

	// "blockwatch.cc/knoxdb/internal/filter/bloom"
	"blockwatch.cc/knoxdb/internal/hash/xxHash32"
	"blockwatch.cc/knoxdb/internal/hash/xxhashVec"
	"blockwatch.cc/knoxdb/pkg/num"
	"blockwatch.cc/knoxdb/pkg/util"
)

const xxHash32Seed = 1312

// Filter represents a bloom filter.
type Filter struct {
	k    uint32
	mask uint32
	b    []byte
}

// NewFilter returns a new instance of Filter using m bits.
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
		return nil, fmt.Errorf("bloom: buffer bit count must be a power of two: %d/%d", len(buf)*8, m)
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

// AddManyUint8 inserts multiple data points to the filter.
func (f *Filter) AddManyUint8(data []byte) {
	for _, v := range data {
		h := hash([]byte{v}, xxHash32Seed)
		for i := uint32(0); i < f.k; i++ {
			loc := f.location(h, i)
			f.b[loc>>3] |= 1 << (loc & 7)
		}
	}
}

// AddManyUint16 inserts multiple data points to the filter.
func (f *Filter) AddManyUint16(data []uint16) {
	for _, v := range data {
		h := HashUint16(v)
		for i := uint32(0); i < f.k; i++ {
			loc := f.location(h, i)
			f.b[loc>>3] |= 1 << (loc & 7)
		}
	}
}

// AddManyInt16 inserts multiple data points to the filter.
func (f *Filter) AddManyInt16(data []int16) {
	for _, v := range data {
		h := HashInt16(v)
		for i := uint32(0); i < f.k; i++ {
			loc := f.location(h, i)
			f.b[loc>>3] |= 1 << (loc & 7)
		}
	}
}

// AddManyUint32 inserts multiple data points to the filter.
func (f *Filter) AddManyUint32(data []uint32) {
	filterAddManyUint32(f, data, xxHash32Seed)
}

// AddManyInt32 inserts multiple data points to the filter.
func (f *Filter) AddManyInt32(data []int32) {
	filterAddManyInt32(f, data, xxHash32Seed)
}

// AddManyUint64 inserts multiple data points to the filter.
func (f *Filter) AddManyUint64(data []uint64) {
	filterAddManyUint64(f, data, xxHash32Seed)
}

// AddManyInt64 inserts multiple data points to the filter.
func (f *Filter) AddManyInt64(data []int64) {
	filterAddManyInt64(f, data, xxHash32Seed)
}

// AddManyFloat64 inserts multiple data points to the filter.
func (f *Filter) AddManyFloat64(data []float64) {
	for _, v := range data {
		h := HashFloat64(v)
		for i := uint32(0); i < f.k; i++ {
			loc := f.location(h, i)
			f.b[loc>>3] |= 1 << (loc & 7)
		}
	}
}

// AddManyFloat32 inserts multiple data points to the filter.
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

// ContainsUint16 returns true if the filter possibly contains v.
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

// ContainsInt16 returns true if the filter possibly contains v.
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

// ContainsInt32 returns true if the filter possibly contains v.
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

// ContainsUint32 returns true if the filter possibly contains v.
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

// ContainsUint64 returns true if the filter possibly contains v.
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

// ContainsInt64 returns true if the filter possibly contains v.
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

// ContainsFloat64 returns true if the filter possibly contains v.
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

// ContainsFloat32 returns true if the filter possibly contains v.
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
		return fmt.Errorf("bloom.Merge(): m mismatch: %d <> %d", len(f.b), len(other.b))
	} else if f.k != other.k {
		return fmt.Errorf("bloom.Merge(): k mismatch: %d <> %d", f.b, other.b)
	}
	filterMerge(f.b, other.b)
	return nil
}

// location returns the ith hashed location using two hash values.
func (f *Filter) location(h [2]uint32, i uint32) uint32 {
	return (h[0] + h[1]*i) & f.mask
}

// Estimate returns an estimated bit count and hash count given the element count
// and false positive rate.
// TODO: adjust formula to fixed k = 4
func Estimate(n uint64, p float64) (m int, k int) {
	m = int(math.Ceil(-1 * float64(n) * math.Log(p) / math.Pow(math.Log(2), 2)))
	k = int(math.Ceil(math.Log(2) * float64(m) / float64(n)))
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

func HashAny(val any) [2]uint32 {
	if val == nil {
		return [2]uint32{}
	}
	switch v := val.(type) {
	case []byte:
		return Hash(v)
	case string:
		return Hash(util.UnsafeGetBytes(v))
	case uint:
		return HashUint64(uint64(v))
	case uint64:
		return HashUint64(v)
	case uint32:
		return HashUint32(v)
	case uint16:
		return HashUint16(v)
	case uint8:
		return Hash([]byte{v})
	case int:
		return HashInt64(int64(v))
	case int64:
		return HashInt64(v)
	case int32:
		return HashInt32(v)
	case int16:
		return HashInt16(v)
	case int8:
		return Hash([]byte{uint8(v)})
	case float64:
		return HashFloat64(v)
	case float32:
		return HashFloat32(v)
	case bool:
		if v {
			return Hash([]byte{1})
		} else {
			return Hash([]byte{0})
		}
	case num.Int256:
		buf := v.Bytes32()
		return Hash(buf[:])
	case num.Int128:
		buf := v.Bytes16()
		return Hash(buf[:])
	default:
		return [2]uint32{}
	}
}

func HashAnySlice(val any) [][2]uint32 {
	if val == nil {
		return nil
	}
	var res [][2]uint32
	switch v := val.(type) {
	case [][]byte:
		res = make([][2]uint32, len(v))
		for i := range res {
			res[i] = Hash(v[i])
		}
	case []string:
		res = make([][2]uint32, len(v))
		for i := range res {
			res[i] = Hash(util.UnsafeGetBytes(v[i]))
		}
	case []uint:
		res = make([][2]uint32, len(v))
		for i := range res {
			res[i] = HashUint64(uint64(v[i]))
		}
	case []uint64:
		res = make([][2]uint32, len(v))
		for i := range res {
			res[i] = HashUint64(v[i])
		}
	case []uint32:
		res = make([][2]uint32, len(v))
		for i := range res {
			res[i] = HashUint32(v[i])
		}
	case []uint16:
		res = make([][2]uint32, len(v))
		for i := range res {
			res[i] = HashUint16(v[i])
		}
	case []uint8:
		res = make([][2]uint32, len(v))
		for i := range res {
			res[i] = Hash([]byte{v[i]})
		}
	case []int:
		res = make([][2]uint32, len(v))
		for i := range res {
			res[i] = HashInt64(int64(v[i]))
		}
	case []int64:
		res = make([][2]uint32, len(v))
		for i := range res {
			res[i] = HashInt64(v[i])
		}
	case []int32:
		res = make([][2]uint32, len(v))
		for i := range res {
			res[i] = HashInt32(v[i])
		}
	case []int16:
		res = make([][2]uint32, len(v))
		for i := range res {
			res[i] = HashInt16(v[i])
		}
	case []int8:
		res = make([][2]uint32, len(v))
		for i := range res {
			res[i] = Hash([]byte{uint8(v[i])})
		}
	case []float64:
		res = make([][2]uint32, len(v))
		for i := range res {
			res[i] = HashFloat64(v[i])
		}
	case []float32:
		res = make([][2]uint32, len(v))
		for i := range res {
			res[i] = HashFloat32(v[i])
		}
	case []bool:
		res = make([][2]uint32, len(v))
		h0, h1 := Hash([]byte{0}), Hash([]byte{1})
		for i := range res {
			if v[i] {
				res[i] = h0
			} else {
				res[i] = h1
			}
		}
	case []num.Int256:
		res = make([][2]uint32, len(v))
		for i := range res {
			buf := v[i].Bytes32()
			res[i] = Hash(buf[:])
		}
	case []num.Int128:
		res = make([][2]uint32, len(v))
		for i := range res {
			buf := v[i].Bytes16()
			res[i] = Hash(buf[:])
		}
	}
	return res
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
