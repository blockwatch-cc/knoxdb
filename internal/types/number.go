// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package types

import (
	"iter"
	"math"
	"math/bits"

	"blockwatch.cc/knoxdb/internal/bitset"
)

// we use only types with strict cross-platform width
type Integer interface {
	int64 | int32 | int16 | int8 | uint64 | uint32 | uint16 | uint8
}

type Signed interface {
	int64 | int32 | int16 | int8
}

type Unsigned interface {
	uint64 | uint32 | uint16 | uint8
}

type Float interface {
	float64 | float32
}

type Number interface {
	Integer | Float
}

type Bitset = bitset.Bitset

type NumberMatcher[T Number] interface {
	MatchEqual(val T, bits, mask *Bitset)
	MatchNotEqual(val T, bits, mask *Bitset)
	MatchLess(val T, bits, mask *Bitset)
	MatchLessEqual(val T, bits, mask *Bitset)
	MatchGreater(val T, bits, mask *Bitset)
	MatchGreaterEqual(val T, bits, mask *Bitset)
	MatchBetween(a, b T, bits, mask *Bitset)
	MatchInSet(s any, bits, mask *Bitset)
	MatchNotInSet(s any, bits, mask *Bitset)
}

type NumberWriter[T Number] interface {
	Append(T)
	Set(int, T)
	Delete(int, int)
}

// NumberReader defines uniform access interface to read
// vector data from materialized and compressed vectors.
type NumberReader[T Number] interface {
	// Len returns the total number of elements in this container.
	Len() int

	// Size returns the total size of the vector including metadata.
	// Use it to estimate heap size and required buffer sizes
	// before storing a compressed vector.
	Size() int

	// Get returns an element at position n or zero when out of bounds.
	Get(int) T

	// Slice returns a raw Go slice directly referencing the internal
	// storage representation of the vector. It is only available on
	// materialized vectors.
	Slice() []T

	// Iterator returns a Go style iterator to walk the container
	// with a for range loop.
	Iterator() iter.Seq2[int, T]

	// Chunks returns a chunked iterator which enables efficient
	// batch processing of vector data.
	Chunks() NumberIterator[T]

	// AppendTo appends selected vector elements to a dst
	// returns an updated slice header reflecting the new length.
	// A selection vector sel controls which elements to append.
	// When nil, all source data is appended. To avoid reallocation
	// of dst, users must carefully capacity and selection vector.
	AppendTo(dst []T, sel []uint32) []T

	// MinMax returns vector minimum and maximum. It is only available
	// on materialized vectors.
	MinMax() (T, T)

	// Cmp compares two vector elements at positions i and j. It returns
	// -1 if `v[i] < v[j]`, 0 if `v[i] == v[j]` and 1 if `v[i] > v[j]`.
	Cmp(i, j int) int

	// more iterators?
	// Range(i, j int) iter.Seq2[int, T]
	// Select(sel []uint32) iter.Seq2[int, T]
}

// Iterators allow efficient seqential and random access to
// materialized and compressed vector data. For compressed vectors
// iterators may use an internal buffer to keep a chunk of decoded
// values in L1 cache which minimizes costs of linear and (small range)
// random access.
//
// Use NextChunk for linear walks, Get for point access and
// Seek or SkipChunk for jumping.
type NumberIterator[T Number] interface {
	// Returns the total number of elements in this vector.
	Len() int

	// Returns an element at position n or zero when out of bounds.
	// Implicitly seeks and decodes the chunk containing n.
	Get(int) T

	// Seeks to position n rounded by CHUNK_SIZE and decodes
	// the relevant chunk. Compatible with NextChunk and Get.
	Seek(int) bool

	// Decodes and returns the next chunk at CHUNK_SIZE boundaries
	// and the number of valid elements in the chunk. Past EOF
	// returns nil and zero n.
	NextChunk() (*[CHUNK_SIZE]T, int)

	// Skips a chunk efficiently without decoding data and returns
	// the number of elements skipped or zero when at EOF. Users may
	// call skip repeatedly before requesting data from NextChunk.
	SkipChunk() int

	// Close releases pointers and allows for efficient re-use
	// of iterators. Users are encouraged to call Close after use
	// to reduce allocations and GC overhead.
	Close()
}

type NumberAccessor[T Number] interface {
	NumberReader[T]
	NumberWriter[T]
	Matcher() NumberMatcher[T]
	Close()
}

func IsSigned[T Number]() bool {
	// Check if -1 is less than 0 in the type T
	// For signed types, this is true (e.g., -1 < 0)
	// For unsigned types, -1 wraps to MaxValue (e.g., 0xFF...FF), so it's false
	return T(0)-T(1) < T(0)
}

func IsInteger[T Number]() bool {
	switch any(T(0)).(type) {
	case float64:
		return false
	case float32:
		return false
	default:
		return true
	}
}

func Log2Range[T Integer](minv, maxv T) int {
	if IsSigned[T]() {
		return bits.Len64(uint64(int64(maxv) - int64(minv)))
	} else {
		return bits.Len64(uint64(maxv - minv))
	}
}

func MinVal[T Number]() T {
	switch any(T(0)).(type) {
	case int64:
		return any(int64(math.MinInt64)).(T)
	case int32:
		return any(int32(math.MinInt32)).(T)
	case int16:
		return any(int16(math.MinInt16)).(T)
	case int8:
		return any(int8(math.MinInt8)).(T)
	case uint64:
		return 0
	case uint32:
		return 0
	case uint16:
		return 0
	case uint8:
		return 0
	case float32:
		return any(float32(-math.MaxFloat32)).(T)
	case float64:
		return any(float64(-math.MaxFloat64)).(T)
	default:
		return 0
	}
}

func MaxVal[T Number]() T {
	switch any(T(0)).(type) {
	case int64:
		return any(int64(math.MaxInt64)).(T)
	case int32:
		return any(int32(math.MaxInt32)).(T)
	case int16:
		return any(int16(math.MaxInt16)).(T)
	case int8:
		return any(int8(math.MaxInt8)).(T)
	case uint64:
		return any(uint64(math.MaxUint64)).(T)
	case uint32:
		return any(uint32(math.MaxUint32)).(T)
	case uint16:
		return any(uint16(math.MaxUint16)).(T)
	case uint8:
		return any(uint8(math.MaxUint8)).(T)
	case float32:
		return any(float32(math.MaxFloat32)).(T)
	case float64:
		return any(float64(math.MaxFloat64)).(T)
	default:
		return 0
	}
}

func Cast[T Integer](val any) (t T, ok bool) {
	ok = true
	switch v := val.(type) {
	case int:
		t = T(v)
	case int64:
		t = T(v)
	case int32:
		t = T(v)
	case int16:
		t = T(v)
	case int8:
		t = T(v)
	case uint:
		t = T(v)
	case uint64:
		t = T(v)
	case uint32:
		t = T(v)
	case uint16:
		t = T(v)
	case uint8:
		t = T(v)
	default:
		ok = false
	}
	return
}
