// Copyright (c) 2018-2021 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package dedup

import (
	"io"
	"reflect"

	"blockwatch.cc/knoxdb/internal/bitset"
)

type ByteArray interface {
	Len() int
	Cap() int
	Elem(int) []byte         // index
	Set(int, []byte)         // index, buf
	SetZeroCopy(int, []byte) // index, buf
	Append(...[]byte) ByteArray
	AppendZeroCopy(...[]byte) ByteArray
	AppendFrom(ByteArray) ByteArray
	Insert(int, ...[]byte) ByteArray
	InsertFrom(int, ByteArray) ByteArray
	Copy(ByteArray, int, int, int) ByteArray // src, dstPos, srcPos, len (ReplaceFrom)
	Delete(int, int) ByteArray               // index, len
	Grow(int) ByteArray                      // len
	Clear()                                  // zero elements and length
	Release()                                // recycle buffers
	Slice() [][]byte
	Subslice(int, int) [][]byte // start, end
	MinMax() ([]byte, []byte)
	Min() []byte
	Max() []byte

	MaxEncodedSize() int
	HeapSize() int
	WriteTo(io.Writer) (int64, error)
	ReadFrom(io.Reader) (int64, error)
	Decode([]byte) error

	Materialize() ByteArray // unpack to native [][]byte slice
	IsMaterialized() bool
	Optimize() ByteArray // analyzes and repacks into a single []byte buffer
	IsOptimized() bool

	ForEach(func(int, []byte))
	ForEachUnique(func(int, []byte))

	// sort interface
	Less(int, int) bool
	Swap(int, int)

	// debug
	Dump() string

	// condition match interface
	MatchEqual(val []byte, bits, mask *bitset.Bitset)
	MatchNotEqual(val []byte, bits, mask *bitset.Bitset)
	MatchLess(val []byte, bits, mask *bitset.Bitset)
	MatchLessEqual(val []byte, bits, mask *bitset.Bitset)
	MatchGreater(val []byte, bits, mask *bitset.Bitset)
	MatchGreaterEqual(val []byte, bits, mask *bitset.Bitset)
	MatchBetween(a, b []byte, bits, mask *bitset.Bitset)
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
