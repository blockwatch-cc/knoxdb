// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package block

import (
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
	case BlockBytes:
		min, max := b.Bytes().MinMax()
		return slices.Clone(min), slices.Clone(max) // clone
	case BlockBool:
		bits := b.Bool()
		if bits.Len() > 0 && bits.Any() {
			return true, false
		}
		return false, false
	default:
		return nil, nil
	}
}

func (b *Block) Min() any {
	switch b.typ {
	case BlockInt64, BlockTime:
		return util.Min(b.Int64().Slice()...)
	case BlockInt32:
		return util.Min(b.Int32().Slice()...)
	case BlockInt16:
		return util.Min(b.Int16().Slice()...)
	case BlockInt8:
		return util.Min(b.Int8().Slice()...)
	case BlockUint64:
		return util.Min(b.Uint64().Slice()...)
	case BlockUint32:
		return util.Min(b.Uint32().Slice()...)
	case BlockUint16:
		return util.Min(b.Uint16().Slice()...)
	case BlockUint8:
		return util.Min(b.Uint8().Slice()...)
	case BlockInt128:
		return b.Int128().Min()
	case BlockInt256:
		return b.Int256().Min()
	case BlockFloat64:
		return util.Min(b.Float64().Slice()...)
	case BlockFloat32:
		return util.Min(b.Float32().Slice()...)
	case BlockBytes:
		return slices.Clone(b.Bytes().Min())
	case BlockBool:
		bits := b.Bool()
		return bits.Len() > 0 && bits.All()
	default:
		return nil
	}
}

func (b *Block) Max() any {
	switch b.typ {
	case BlockInt64, BlockTime:
		return util.Max(b.Int64().Slice()...)
	case BlockInt32:
		return util.Max(b.Int32().Slice()...)
	case BlockInt16:
		return util.Max(b.Int16().Slice()...)
	case BlockInt8:
		return util.Max(b.Int8().Slice()...)
	case BlockUint64:
		return util.Max(b.Uint64().Slice()...)
	case BlockUint32:
		return util.Max(b.Uint32().Slice()...)
	case BlockUint16:
		return util.Max(b.Uint16().Slice()...)
	case BlockUint8:
		return util.Max(b.Uint8().Slice()...)
	case BlockInt128:
		return b.Int128().Max()
	case BlockInt256:
		return b.Int256().Max()
	case BlockFloat64:
		return util.Max(b.Float64().Slice()...)
	case BlockFloat32:
		return util.Max(b.Float32().Slice()...)
	case BlockBytes:
		return slices.Clone(b.Bytes().Max())
	case BlockBool:
		return b.Bool().Any()
	default:
		return nil
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
