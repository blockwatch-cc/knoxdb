// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package types

import (
	"iter"

	"blockwatch.cc/knoxdb/internal/bitset"
)

// we use only types with strict cross-platform width
type Signed interface {
	int64 | int32 | int16 | int8
}

type Unsigned interface {
	uint64 | uint32 | uint16 | uint8
}

type Integer interface {
	Signed | Unsigned
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
	Append(T) int
	Reserve(int)
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

	// Values returns sequence for all index-value pairs in the container.
	All() iter.Seq2[int, T]

	// Values returns sequence for all values in the container.
	Values() iter.Seq[T]

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
// Use Next for linear walks, Get for point access and
// Seek or Skip for jumping.
type NumberIterator[T Number] interface {
	// Returns the total number of elements in this vector.
	Len() int

	// Returns an element at position n or zero when out of bounds.
	// Implicitly seeks and decodes the chunk containing n.
	Value(int) T

	// Seeks to position n rounded by CHUNK_SIZE and decodes
	// the relevant chunk. Compatible with Next and Get.
	Seek(int) bool

	// Decodes and returns the next chunk at CHUNK_SIZE boundaries
	// and the number of valid elements in the chunk. Past EOF
	// returns nil and zero n.
	Next() (*[CHUNK_SIZE]T, int)

	// Skips a chunk efficiently without decoding data and returns
	// the number of elements skipped or zero when at EOF. Users may
	// call skip repeatedly before requesting data from Next.
	Skip() int

	// Close releases pointers and allows for efficient re-use
	// of iterators. Users are encouraged to call Close after use
	// to reduce allocations and GC overhead.
	Close()

	// All iterates over all index/value pairs in the array.
	All() iter.Seq2[int, T]

	// Values iterates over all values in the array in order.
	Values() iter.Seq[T]
}

type NumberAccessor[T Number] interface {
	NumberReader[T]
	NumberWriter[T]
	Matcher() NumberMatcher[T]
	Close()
}
