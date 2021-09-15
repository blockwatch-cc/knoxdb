// Copyright (c) 2018-2021 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package dedup

import (
	"io"
	"reflect"

	"blockwatch.cc/knoxdb/vec"
)

type ByteArray interface {
	Len() int
	Cap() int
	Elem(int) []byte // index
	Set(int, []byte) // index, buf
	Append(...[]byte) ByteArray
	AppendFrom(ByteArray) ByteArray
	Insert(int, ...[]byte) ByteArray
	InsertFrom(int, ByteArray) ByteArray
	Copy(ByteArray, int, int, int) ByteArray // src, dstPos, srcPos, len (ReplaceFrom)
	Delete(int, int) ByteArray               // index, len
	Clear()                                  // zero elements and length
	Release()                                // recycle buffers
	Slice() [][]byte
	Subslice(int, int) [][]byte // start, end
	MinMax() ([]byte, []byte)

	MaxEncodedSize() int
	HeapSize() int
	WriteTo(io.Writer) (int, error)
	Decode([]byte) error

	Materialize() ByteArray // unpack to native [][]byte slice
	IsMaterialized() bool
	Optimize() ByteArray // analyzes and repacks into a single []byte buffer
	IsOptimized() bool

	// sort interface
	Less(int, int) bool
	Swap(int, int)

	// condition match interface
	MatchEqual(val []byte, bits, mask *vec.Bitset) *vec.Bitset
	MatchNotEqual(val []byte, bits, mask *vec.Bitset) *vec.Bitset
	MatchLessThan(val []byte, bits, mask *vec.Bitset) *vec.Bitset
	MatchLessThanEqual(val []byte, bits, mask *vec.Bitset) *vec.Bitset
	MatchGreaterThan(val []byte, bits, mask *vec.Bitset) *vec.Bitset
	MatchGreaterThanEqual(val []byte, bits, mask *vec.Bitset) *vec.Bitset
	MatchBetween(a, b []byte, bits, mask *vec.Bitset) *vec.Bitset
}

func NewByteArray(sz int) ByteArray {
	return newNativeByteArray(sz)
}

func NewByteArrayFromBytes(b [][]byte) ByteArray {
	return newNativeByteArrayFromBytes(b)
}

var (
	fixedByteArraySz   = int(reflect.TypeOf(FixedByteArray{}).Size())
	nativeByteArraySz  = int(reflect.TypeOf(NativeByteArray{}).Size())
	compactByteArraySz = int(reflect.TypeOf(CompactByteArray{}).Size())
	dictByteArraySz    = int(reflect.TypeOf(DictByteArray{}).Size())
)
