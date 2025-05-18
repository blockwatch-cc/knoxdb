// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package block

import (
	"blockwatch.cc/knoxdb/internal/hash/xxhash"
	"blockwatch.cc/knoxdb/internal/hash/xxhash64"
)

func (b *Block) Hash() *Block {
	l := b.Len()
	h := New(BlockUint64, l)
	h.len = l

	switch b.typ {
	case BlockTime, BlockFloat64, BlockInt64, BlockUint64:
		xxhash.Vec64u64(b.Uint64().Slice(), h.Uint64().Slice())
	case BlockUint32, BlockInt32, BlockFloat32:
		xxhash.Vec64u32(b.Uint32().Slice(), h.Uint64().Slice())
	case BlockUint16, BlockInt16:
		xxhash.Vec64u16(b.Uint16().Slice(), h.Uint64().Slice())
	case BlockUint8, BlockInt8:
		xxhash.Vec64u8(b.Uint8().Slice(), h.Uint64().Slice())
	case BlockBool:
		zero, one := xxhash64.Sum64([]byte{0}), xxhash64.Sum64([]byte{1})
		bits := b.Bool()
		u64 := h.Uint64().Slice()
		for i := 0; i < l; i++ {
			if bits.Contains(i) {
				u64[i] = one
			} else {
				u64[i] = zero
			}
		}
	case BlockBytes:
		u64 := h.Uint64().Slice()
		bytes := b.Bytes()
		for i := 0; i < l; i++ {
			u64[i] = xxhash64.Sum64(bytes.Elem(i))
		}
	case BlockInt128:
		u64 := h.Uint64().Slice()
		i128 := b.Int128()
		for i := 0; i < l; i++ {
			u64[i] = xxhash64.Sum64(i128.Elem(i).Bytes())
		}
	case BlockInt256:
		u64 := h.Uint64().Slice()
		i256 := b.Int256()
		for i := 0; i < l; i++ {
			u64[i] = xxhash64.Sum64(i256.Elem(i).Bytes())
		}
	}
	return h
}
