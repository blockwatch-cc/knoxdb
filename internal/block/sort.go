// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package block

import (
	"bytes"

	"blockwatch.cc/knoxdb/pkg/util"
)

func (b *Block) Less(i, j int) bool {
	switch b.typ {
	case BlockTime, BlockInt64:
		return b.Int64().Less(i, j)
	case BlockInt32:
		return b.Int32().Less(i, j)
	case BlockInt16:
		return b.Int16().Less(i, j)
	case BlockInt8:
		return b.Int8().Less(i, j)
	case BlockUint64:
		return b.Uint64().Less(i, j)
	case BlockUint32:
		return b.Uint32().Less(i, j)
	case BlockUint16:
		return b.Uint16().Less(i, j)
	case BlockUint8:
		return b.Uint8().Less(i, j)
	case BlockFloat64:
		return b.Float64().Less(i, j)
	case BlockFloat32:
		return b.Float32().Less(i, j)
	case BlockBool:
		bits := b.Bool()
		return !bits.IsSet(i) && bits.IsSet(j)
	case BlockBytes:
		dd := b.Bytes()
		return bytes.Compare(dd.Elem(i), dd.Elem(j)) < 0
	case BlockInt256:
		i256 := b.Int256()
		return i256.Elem(i).Lt(i256.Elem(j))
	case BlockInt128:
		i128 := b.Int128()
		return i128.Elem(i).Lt(i128.Elem(j))
	default:
		return false
	}
}

func (b *Block) Cmp(i, j int) int {
	switch b.typ {
	case BlockTime, BlockInt64:
		return b.Int64().Cmp(i, j)
	case BlockInt32:
		return b.Int32().Cmp(i, j)
	case BlockInt16:
		return b.Int16().Cmp(i, j)
	case BlockInt8:
		return b.Int8().Cmp(i, j)
	case BlockUint64:
		return b.Uint64().Cmp(i, j)
	case BlockUint32:
		return b.Uint32().Cmp(i, j)
	case BlockUint16:
		return b.Uint16().Cmp(i, j)
	case BlockUint8:
		return b.Uint8().Cmp(i, j)
	case BlockFloat64:
		return b.Float64().Cmp(i, j)
	case BlockFloat32:
		return b.Float32().Cmp(i, j)
	case BlockBool:
		bits := b.Bool()
		bi, bj := bits.IsSet(i), bits.IsSet(j)
		switch {
		case bi == bj:
			return 0
		case !bi && bj:
			return -1
		default:
			return 1
		}
	case BlockBytes:
		dd := b.Bytes()
		return bytes.Compare(dd.Elem(i), dd.Elem(j))
	case BlockInt256:
		i256 := b.Int256()
		return i256.Elem(i).Cmp(i256.Elem(j))
	case BlockInt128:
		i128 := b.Int128()
		return i128.Elem(i).Cmp(i128.Elem(j))
	default:
		return 0
	}
}

// case-insensitive
func (b *Block) Cmpi(i, j int) int {
	if b.typ == BlockBytes {
		dd := b.Bytes()
		return util.CmpCaseInsensitive(
			util.UnsafeGetString(dd.Elem(i)),
			util.UnsafeGetString(dd.Elem(j)),
		)
	} else {
		return b.Cmp(i, j)
	}
}

func (b *Block) Swap(i, j int) {
	switch b.typ {
	case BlockInt64, BlockTime:
		b.Int64().Swap(i, j)
	case BlockInt32:
		b.Int32().Swap(i, j)
	case BlockInt16:
		b.Int16().Swap(i, j)
	case BlockInt8:
		b.Int8().Swap(i, j)
	case BlockUint64:
		b.Uint64().Swap(i, j)
	case BlockUint32:
		b.Uint32().Swap(i, j)
	case BlockUint16:
		b.Uint16().Swap(i, j)
	case BlockUint8:
		b.Uint8().Swap(i, j)
	case BlockFloat64:
		b.Float64().Swap(i, j)
	case BlockFloat32:
		b.Float32().Swap(i, j)
	case BlockBytes:
		b.Bytes().Swap(i, j)
	case BlockBool:
		b.Bool().Swap(i, j)
	case BlockInt256:
		b.Int256().Swap(i, j)
	case BlockInt128:
		b.Int128().Swap(i, j)
	}
}
