// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package block

import (
	"bytes"

	"blockwatch.cc/knoxdb/pkg/util"
)

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
