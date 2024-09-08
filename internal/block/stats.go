// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package block

import (
	"blockwatch.cc/knoxdb/internal/cmp"
	"blockwatch.cc/knoxdb/internal/filter/bloom"
	"blockwatch.cc/knoxdb/internal/filter/loglogbeta"
	"blockwatch.cc/knoxdb/internal/xroar"
	"blockwatch.cc/knoxdb/pkg/num"
	"blockwatch.cc/knoxdb/pkg/util"
	"golang.org/x/exp/slices"
)

func (b *Block) MinMax() (any, any) {
	switch b.typ {
	case BlockInt64, BlockTime:
		return util.MinMax(b.Int64().Slice()...)
	case BlockInt32:
		return util.MinMax(b.Int32().Slice()...)
	case BlockInt16:
		return util.MinMax(b.Int16().Slice()...)
	case BlockInt8:
		return util.MinMax(b.Int8().Slice()...)
	case BlockUint64:
		return util.MinMax(b.Uint64().Slice()...)
	case BlockUint32:
		return util.MinMax(b.Uint32().Slice()...)
	case BlockUint16:
		return util.MinMax(b.Uint16().Slice()...)
	case BlockUint8:
		return util.MinMax(b.Uint8().Slice()...)
	case BlockInt128:
		return b.Int128().MinMax()
	case BlockInt256:
		return b.Int256().MinMax()
	case BlockFloat64:
		return util.MinMax(b.Float64().Slice()...)
	case BlockFloat32:
		return util.MinMax(b.Float32().Slice()...)
	case BlockString:
		min, max := b.Bytes().MinMax()
		return string(min), string(max) // copy
	case BlockBytes:
		min, max := b.Bytes().MinMax()
		return slices.Clone(min), slices.Clone(max) // clone
	case BlockBool:
		bits := b.Bool()
		if bits.Len() > 0 && bits.Count() > 0 {
			return true, false
		}
		return false, false
	default:
		return nil, nil
	}
}

func (b *Block) FirstLast() (any, any) {
	switch b.typ {
	case BlockInt64, BlockTime:
		slice := b.Int64().Slice()
		if l := len(slice); l > 0 {
			return slice[0], slice[l-1]
		}
		return int64(0), int64(0)
	case BlockInt32:
		slice := b.Int32().Slice()
		if l := len(slice); l > 0 {
			return slice[0], slice[l-1]
		}
		return int32(0), int32(0)
	case BlockInt16:
		slice := b.Int16().Slice()
		if l := len(slice); l > 0 {
			return slice[0], slice[l-1]
		}
		return int16(0), int16(0)
	case BlockInt8:
		slice := b.Int8().Slice()
		if l := len(slice); l > 0 {
			return slice[0], slice[l-1]
		}
		return int8(0), int8(0)
	case BlockUint64:
		slice := b.Uint64().Slice()
		if l := len(slice); l > 0 {
			return slice[0], slice[l-1]
		}
		return uint64(0), uint64(0)
	case BlockUint32:
		slice := b.Uint32().Slice()
		if l := len(slice); l > 0 {
			return slice[0], slice[l-1]
		}
		return uint32(0), uint32(0)
	case BlockUint16:
		slice := b.Uint16().Slice()
		if l := len(slice); l > 0 {
			return slice[0], slice[l-1]
		}
		return uint16(0), uint16(0)
	case BlockUint8:
		slice := b.Uint8().Slice()
		if l := len(slice); l > 0 {
			return slice[0], slice[l-1]
		}
		return uint8(0), uint8(0)
	case BlockInt128:
		i128 := b.Int128()
		if l := i128.Len(); l > 0 {
			return i128.Elem(0), i128.Elem(l - 1)
		}
		return num.Int128{}, num.Int128{}
	case BlockInt256:
		i256 := b.Int256()
		if l := i256.Len(); l > 0 {
			return i256.Elem(0), i256.Elem(l - 1)
		}
		return num.Int256{}, num.Int256{}
	case BlockFloat64:
		slice := b.Float64().Slice()
		if l := len(slice); l > 0 {
			return slice[0], slice[l-1]
		}
		return float64(0), float64(0)
	case BlockFloat32:
		slice := b.Float32().Slice()
		if l := len(slice); l > 0 {
			return slice[0], slice[l-1]
		}
		return float32(0), float32(0)
	case BlockString:
		arr := b.Bytes()
		if l := arr.Len(); l > 0 {
			return string(arr.Elem(0)), string(arr.Elem(l - 1)) // copy
		}
		return "", ""
	case BlockBytes:
		arr := b.Bytes()
		if l := arr.Len(); l > 0 {
			return slices.Clone(arr.Elem(0)), slices.Clone(arr.Elem(l - 1)) // clone
		}
		return nil, nil
	case BlockBool:
		bits := b.Bool()
		if l := bits.Len(); l > 0 {
			return bits.IsSet(0), bits.IsSet(l - 1)
		}
		return false, false
	default:
		return nil, nil
	}
}

func (b *Block) EstimateCardinality(precision int) int {
	// shortcut for empty and very small blocks
	l := b.Len()
	switch l {
	case 0:
		return 0
	case 1:
		return 1
	case 2:
		minVal, maxVal := b.MinMax()
		if cmp.EQ(b.typ, minVal, maxVal) {
			return 1
		}
		return 2
	}

	// type-based estimation
	// - use loglogbeta for 256/128/64/32 bit numbers and bytes/strings
	// - use xroar bitmaps for 16/8 bit
	switch b.typ {
	case BlockInt64, BlockTime, BlockUint64, BlockFloat64:
		flt := loglogbeta.NewFilterWithPrecision(uint32(precision))
		flt.AddManyUint64(b.Uint64().Slice())
		return util.Min(l, int(flt.Cardinality()))

	case BlockInt32, BlockUint32, BlockFloat32:
		flt := loglogbeta.NewFilterWithPrecision(uint32(precision))
		flt.AddManyUint32(b.Uint32().Slice())
		return util.Min(l, int(flt.Cardinality()))

	case BlockInt16, BlockUint16:
		bits := xroar.NewBitmapWith(l)
		for _, v := range b.Uint16().Slice() {
			bits.Set(uint64(v))
		}
		return bits.GetCardinality()

	case BlockInt8, BlockUint8:
		bits := xroar.NewBitmapWith(l)
		for _, v := range b.Uint8().Slice() {
			bits.Set(uint64(v))
		}
		return bits.GetCardinality()

	case BlockInt256:
		i256 := b.Int256()
		flt := loglogbeta.NewFilterWithPrecision(uint32(precision))
		for i := 0; i < l; i++ {
			buf := i256.Elem(i).Bytes32()
			flt.Add(buf[:])
		}
		return util.Min(l, int(flt.Cardinality()))

	case BlockInt128:
		i128 := b.Int128()
		flt := loglogbeta.NewFilterWithPrecision(uint32(precision))
		for i := 0; i < l; i++ {
			buf := i128.Elem(i).Bytes16()
			flt.Add(buf[:])
		}
		return util.Min(l, int(flt.Cardinality()))

	case BlockBytes, BlockString:
		flt := loglogbeta.NewFilterWithPrecision(uint32(precision))
		b.Bytes().ForEachUnique(func(_ int, buf []byte) {
			flt.Add(buf)
		})
		return util.Min(l, int(flt.Cardinality()))

	case BlockBool:
		min, max := b.MinMax()
		if min == max {
			return 1
		}
		return 2

	default:
		return 0
	}
}

func (b *Block) BuildBloomFilter(cardinality, factor int) *bloom.Filter {
	if cardinality <= 0 || factor <= 0 {
		return nil
	}

	// dimension filter for cardinality and factor to control its false positive rate
	// (bloom expects size in bits)
	//
	// - 2% for m = set cardinality * 2
	// - 0.2% for m = set cardinality * 3
	// - 0.02% for m = set cardinality * 4
	flt := bloom.NewFilter(cardinality * factor * 8)

	switch b.typ {
	case BlockInt64, BlockTime, BlockUint64, BlockFloat64:
		// we write uint64 data in little endian order into the filter,
		// so all 8 byte numeric types look the same (float64 uses FloatBits == uint64)
		flt.AddManyUint64(b.Uint64().Slice())

	case BlockInt32, BlockUint32, BlockFloat32:
		// we write uint32 data in little endian order into the filter,
		// so all 4 byte numeric types look the same (float32 uses FloatBits == uint32)
		flt.AddManyUint32(b.Uint32().Slice())

	case BlockInt16, BlockUint16:
		// we write uint16 data in little endian order into the filter,
		// so all 2 byte numeric types look the
		flt.AddManyUint16(b.Uint16().Slice())

	case BlockInt8, BlockUint8:
		flt.AddManyUint8(b.Uint8().Slice())

	case BlockInt256:
		// write individual elements (no optimization exists)
		i256 := b.Int256()
		for i, l := 0, i256.Len(); i < l; i++ {
			buf := i256.Elem(i).Bytes32()
			flt.Add(buf[:])
		}

	case BlockInt128:
		// write individual elements (no optimization exists)
		i128 := b.Int128()
		for i, l := 0, i128.Len(); i < l; i++ {
			buf := i128.Elem(i).Bytes16()
			flt.Add(buf[:])
		}

	case BlockBytes, BlockString:
		// write only unique elements (post-dedup optimization this avoids
		// calculating hashes for duplicates)
		b.Bytes().ForEachUnique(func(_ int, buf []byte) {
			flt.Add(buf)
		})

	default:
		// BlockBool and unknown/future types have no filter
		return nil
	}
	return flt
}

func (b *Block) BuildBitsFilter(cardinality int) *xroar.Bitmap {
	if cardinality <= 0 {
		return nil
	}

	flt := xroar.NewBitmapWith(cardinality)

	switch b.typ {
	case BlockInt64, BlockTime, BlockUint64:
		for _, v := range b.Uint64().Slice() {
			flt.Set(v)
		}

	case BlockInt32, BlockUint32:
		for _, v := range b.Uint32().Slice() {
			flt.Set(uint64(v))
		}

	case BlockInt16, BlockUint16:
		for _, v := range b.Uint16().Slice() {
			flt.Set(uint64(v))
		}

	case BlockInt8, BlockUint8:
		for _, v := range b.Uint8().Slice() {
			flt.Set(uint64(v))
		}

	default:
		// unsupported
		// BlockInt256, BlockInt128, BlockBytes, BlockString, BlockBool
		// unknown/future types have no filter
		return nil
	}
	return flt
}
