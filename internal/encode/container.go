// Copyright (c) 2025 Blockwatch Data Inc.
// Author: abdul@blockwatch.cc

package encode

import (
	"errors"

	"blockwatch.cc/knoxdb/internal/types"
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

type NumberContainer[T types.Number] interface {
	// introspect
	Type() ContainerType // returns encoding type
	Len() int            // returns vector length
	Info() string        // describes encoding and nested containers

	// data access
	Get(int) T                          // returns single value at position
	AppendTo(dst []T, sel []uint32) []T // decodes and appends all/selected values
	Iterator() NumberIterator[T]        // buffered iterator

	// encode
	Encode(ctx *Context[T], vals []T) NumberContainer[T]

	// IO
	Size() int                   // helps dimension buffer before write
	Store([]byte) []byte         // serializes into buf, returns updated buf
	Load([]byte) ([]byte, error) // deserializes from buf, returns updated buf
	Close()                      // free resources

	// matchers
	types.NumberMatcher[T]
}
