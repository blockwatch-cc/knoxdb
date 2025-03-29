// Copyright (c) 2022 Blockwatch Data Inc.
// Author: stefan@blockwatch.cc

package s8b

import (
	"sort"

	"blockwatch.cc/knoxdb/internal/encode/s8b/avx2"
	"blockwatch.cc/knoxdb/internal/encode/s8b/avx512"
	"blockwatch.cc/knoxdb/internal/encode/s8b/generic"
	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/util"
)

var (
	// Encoders
	EncodeLegacy = generic.EncodeLegacy

	EncodeUint64 = generic.Encode[uint64]
	EncodeInt64  = generic.Encode[int64]
	EncodeUint32 = generic.Encode[uint32]
	EncodeInt32  = generic.Encode[int32]
	EncodeUint16 = generic.Encode[uint16]
	EncodeInt16  = generic.Encode[int16]
	EncodeUint8  = generic.Encode[uint8]
	EncodeInt8   = generic.Encode[int8]

	// Decoders
	DecodeUint64 = generic.Decode[uint64]
	DecodeUint32 = generic.Decode[uint32]
	DecodeUint16 = generic.Decode[uint16]
	DecodeUint8  = generic.Decode[uint8]

	// Comparers
	Equal        = generic.Equal
	NotEqual     = generic.NotEqual
	Less         = generic.Less
	LessEqual    = generic.LessEqual
	Greater      = generic.Greater
	GreaterEqual = generic.GreaterEqual
	Between      = generic.Between

	// Helpers
	CountValues = generic.CountValues

	MaxValue   = uint64((1 << 60) - 1)
	MaxValue32 = uint64((1 << 30) - 1)
	MaxValue16 = uint64((1 << 15) - 1)
)

func init() {
	if util.UseAVX2 {
		DecodeUint64 = avx2.DecodeUint64
		DecodeUint32 = avx2.DecodeUint32
		DecodeUint16 = avx2.DecodeUint16
		DecodeUint8 = avx2.DecodeUint8
		CountValues = avx2.CountValues
	}
	if util.UseAVX512_F {
		DecodeUint64 = avx512.DecodeUint64
	}
}

func EstimateMaxSize[T types.Integer](srcLen int, minv, maxv T) int {
	rangeVal := uint64(maxv) - uint64(minv)
	if rangeVal == 0 { // All values equal after shift
		return srcLen/60 + packRemainder(srcLen%60)
	}

	// Find bits needed for rangeVal
	bitsPerValue := 1
	for rangeVal >= (1 << uint(bitsPerValue)) {
		bitsPerValue++
	}

	// Map to values per word
	var valuesPerWord int
	switch {
	case bitsPerValue <= 1:
		valuesPerWord = 60
	case bitsPerValue <= 2:
		valuesPerWord = 30
	case bitsPerValue <= 3:
		valuesPerWord = 20
	case bitsPerValue <= 4:
		valuesPerWord = 15
	case bitsPerValue <= 5:
		valuesPerWord = 12
	case bitsPerValue <= 6:
		valuesPerWord = 10
	case bitsPerValue <= 7:
		valuesPerWord = 8
	case bitsPerValue <= 8:
		valuesPerWord = 7
	case bitsPerValue <= 10:
		valuesPerWord = 6
	case bitsPerValue <= 12:
		valuesPerWord = 5
	case bitsPerValue <= 15:
		valuesPerWord = 4
	case bitsPerValue <= 20:
		valuesPerWord = 3
	case bitsPerValue <= 30:
		valuesPerWord = 2
	default:
		valuesPerWord = 1
	}

	return srcLen/valuesPerWord + packRemainder(srcLen%valuesPerWord)
}

func packRemainder(k int) (n int) {
	for k > 0 {
		switch {
		case k > 30:
			k -= 30
			n++
		case k > 20:
			k -= 20
			n++
		case k > 15:
			k -= 15
			n++
		case k > 12:
			k -= 12
			n++
		case k > 10:
			k -= 10
			n++
		case k > 8:
			k -= 8
			n++
		default:
			k = 0
			n++
		}
	}
	return
}

type Index interface {
	Len() int
	End() int
	Find(n int) int
}

type IndexImpl[T uint16 | uint32] struct {
	ends []T
}

func (idx IndexImpl[T]) Len() int {
	return len(idx.ends)
}

func (idx IndexImpl[T]) End() int {
	return int(idx.ends[len(idx.ends)-1])
}

func (idx IndexImpl[T]) Find(n int) int {
	i := sort.Search(len(idx.ends), func(i int) bool {
		return idx.ends[i] >= T(n)
	})
	return i
}

var selector = [16]int{240, 120, 60, 30, 20, 15, 12, 10, 8, 7, 6, 5, 4, 3, 2, 1}

func MakeIndex[T uint16 | uint32](src []byte, dst []T) Index {
	var (
		i int
		n T
	)

	if dst == nil {
		dst = make([]T, 0)
	}
	dst = dst[:0]

	for range len(src) / 64 {
		n += T(selector[src[i]>>4])
		dst = append(dst, n)
		i += 8
		n += T(selector[src[i]>>4])
		dst = append(dst, n)
		i += 8
		n += T(selector[src[i]>>4])
		dst = append(dst, n)
		i += 8
		n += T(selector[src[i]>>4])
		dst = append(dst, n)
		i += 8
		n += T(selector[src[i]>>4])
		dst = append(dst, n)
		i += 8
		n += T(selector[src[i]>>4])
		dst = append(dst, n)
		i += 8
		n += T(selector[src[i]>>4])
		dst = append(dst, n)
		i += 8
		n += T(selector[src[i]>>4])
		dst = append(dst, n)
		i += 8
	}

	for i < len(src) {
		n += T(selector[src[i]>>4])
		dst = append(dst, n)
		i += 8
	}

	return &IndexImpl[T]{ends: dst}
}

type Iterator[T types.Unsigned] struct {
	src []byte
	ofs int
	pos int
	n   int
	tmp [60]T
}

func NewIterator[T types.Unsigned](buf []byte) *Iterator[T] {
	return &Iterator[T]{
		src: buf,
	}
}

// it := s8b.NewIterator[uint64](c.Packed)
// var i int
// for {
// 	vals, err := it.Next()
// 	if err != nil {
// 		panic(err)
// 	}
// 	if len(vals) == 0 {
// 		break
// 	}
// 	for _, v := range vals {
// 		val := T(v) + c.For
// 		if set.Contains(uint64(val)) {
// 			bits.Set(i)
// 		}
// 		i++
// 	}
// }

func (it *Iterator[T]) Next() ([]T, error) {
	if it.ofs >= len(it.src) {
		return nil, nil
	}
	it.pos += it.n

	n, err := generic.DecodeWord(it.tmp[:], it.src[it.ofs:])
	if err != nil {
		return nil, err
	}
	it.ofs += 8
	it.n = n

	return it.tmp[:n], nil
}

// TODO: find a given value by position, amortize cost by caching the word or its decoded vals
// func (it *Iterator[T]) Seek(n int) (T, error) {
// 	if n>it.pos {
// 		it.pos = 0
// 		it.n = 0
// 		it.ofs = 0
// 	}
// }
