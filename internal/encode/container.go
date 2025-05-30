// Copyright (c) 2025 Blockwatch Data Inc.
// Author: abdul@blockwatch.cc

package encode

import (
	"errors"

	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/num"
)

var (
	ErrInvalidType = errors.New("invalid container type")
)

// ContainerType defines the encoding type
type ContainerType byte

const (
	TInvalid ContainerType = iota

	// integer containers
	TIntConstant   // 1
	TIntDelta      // 2
	TIntRunEnd     // 3
	TIntBitpacked  // 4
	TIntDictionary // 5
	TIntSimple8    // 6
	TIntRaw        // 7
	TInt128        // 8
	TInt256        // 9

	// float containers
	TFloatConstant   // 10
	TFloatRunEnd     // 11
	TFloatDictionary // 12
	TFloatAlp        // 13
	TFloatAlpRd      // 14
	TFloatRaw        // 15

	// string containers
	TStringConstant   // 16
	TStringFixed      // 17
	TStringCompact    // 18
	TStringDictionary // 19

	// bitmap containers
	TBitmapZero   // 20
	TBitmapOne    // 21
	TBitmapDense  // 22
	TBitmapSparse // 23

	// Note: always append new values at the end (type is used in storage headers)
)

var (
	cTypeNames    = "__const_delta_run_bp_dict_s8_raw_i128_i256_const_run_dict_alp_alprd_raw_const_fixed_compact_dict_zero_one_dense_sparse"
	cTypeNamesOfs = []int{0, 2, 8, 14, 18, 21, 26, 29, 33, 38, 43, 49, 53, 58, 62, 68, 72, 78, 84, 92, 97, 102, 106, 112, 119}
)

func (t ContainerType) String() string {
	return cTypeNames[cTypeNamesOfs[t] : cTypeNamesOfs[t+1]-1]
}

// NumberContainer defines a common interface for all encoding containers
// that embed native numeric vectors (signed & unsigned int, float). The
// purpose of these containers is to unify data access across compression
// schemes (e.g. dict, bitpack, delta, raw encodings) and perform filter/
// comparison operations on compressed vectors without first having to
// decompress the entire vector (i.e. most schemes use fusion kernels).
//
// Compression schemes in use are light-weight and require minimal CPU time
// to initialize after loading data from disk. All containers reference
// loaded data buffers, so their lifecycle must be synced with that of
// buffer pages containing the underlying data.
type NumberContainer[T types.Number] interface {
	// introspect
	Type() ContainerType // returns encoding type
	Info() string        // describes encoding and nested containers

	// encode and I/O
	Encode(ctx *Context[T], vals []T) NumberContainer[T]
	Store([]byte) []byte         // serializes into buf, returns updated buf
	Load([]byte) ([]byte, error) // deserializes from buf, returns updated buf

	// Common vector access interface
	//
	// Some functions are only available on materialized vectors. These are
	// not unavailable on encoded containers for semantic reasons (containers
	// are read-only) and performance reasosn (decoding full vectors would
	// defy the purpose of working on compressed data).
	//
	// Reader
	//   Len() int                     // returns vector length
	//   Size() int                    // encoded size, use to get buffer size before store
	//   Get(int) T                    // returns single value at position
	//   Slice() []T                   // (unavailable)
	//   Iterator() iter.Seq2[int, T]  // Go style vector iterator
	//   Chunks() NumberIterator[T]    // buffered chunk-based iterator
	//   AppendTo([]T, []uint32) []T   // decodes and appends all/selected values to dst
	//   MinMax() (T, T)               // (unavailable)
	//   Cmp(i, j int) int             // compares values at positions i and j
	// Writer
	//   Append(T)                     // (unavailable)
	//   Set(int, T)                   // (unavailable)
	//   Delete(int, int)              // (unavailable)
	// Accessor
	//   Matcher() NumberMatcher[T]    // returns a matcher object (self)
	//   Close()                       // free resources
	//
	types.NumberAccessor[T]
	types.NumberMatcher[T]
}

// Use to add noop functions required by the common interface to each container type.
type readOnlyContainer[T types.Number | []byte | num.Int128 | num.Int256] struct{}

// unsupported writer and accessor interfaces (used on materialized blocks only,
// but still part of the common interface)
func (_ *readOnlyContainer[T]) Set(_ int, _ T)           {}
func (_ *readOnlyContainer[T]) Delete(_, _ int)          {}
func (_ *readOnlyContainer[T]) Clear()                   {}
func (_ *readOnlyContainer[T]) Append(_ T)               {}
func (_ *readOnlyContainer[T]) Cap() (n int)             { return }
func (_ *readOnlyContainer[T]) Slice() (s []T)           { return }
func (_ *readOnlyContainer[T]) MinMax() (minv T, maxv T) { return }
func (_ *readOnlyContainer[T]) Min() (minv T)            { return }
func (_ *readOnlyContainer[T]) Max() (maxv T)            { return }
