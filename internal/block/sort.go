// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package block

import (
	"blockwatch.cc/knoxdb/pkg/util"
)

func (b *Block) Cmp(i, j int) int {
	switch b.typ {
	case BlockInt64:
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
		return b.Bool().Cmp(i, j)
	case BlockBytes:
		return b.Bytes().Cmp(i, j)
	case BlockInt256:
		return b.Int256().Cmp(i, j)
	case BlockInt128:
		return b.Int128().Cmp(i, j)
	default:
		return 0
	}
}

// case-insensitive
func (b *Block) Cmpi(i, j int) int {
	if b.typ == BlockBytes {
		dd := b.Bytes()
		return util.CmpCaseInsensitive(
			util.UnsafeGetString(dd.Get(i)),
			util.UnsafeGetString(dd.Get(j)),
		)
	} else {
		return b.Cmp(i, j)
	}
}
