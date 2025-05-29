// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package block

import (
	"blockwatch.cc/knoxdb/internal/hash/xxhash"
	"blockwatch.cc/knoxdb/internal/hash/xxhash64"
	"blockwatch.cc/knoxdb/pkg/slicex"
)

func (b *Block) Hash() *Block {
	l := b.Len()
	h := New(BlockUint64, l)
	h.len = uint32(l)

	switch b.typ {
	case BlockFloat64, BlockInt64, BlockUint64:
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
		switch {
		case bits.All():
			slicex.Fill(u64, one)
		case bits.None():
			slicex.Fill(u64, zero)
		default:
			slicex.Fill(u64, zero)
			for i := range bits.Iterator() {
				u64[i] = one
			}
		}
	case BlockBytes:
		u64 := h.Uint64().Slice()
		for i, v := range b.Bytes().Iterator() {
			u64[i] = xxhash64.Sum64(v)
		}
	case BlockInt128:
		u64 := h.Uint64().Slice()
		for i, v := range b.Int128().Iterator() {
			u64[i] = xxhash64.Sum64(v.Bytes())
		}
	case BlockInt256:
		u64 := h.Uint64().Slice()
		for i, v := range b.Int256().Iterator() {
			u64[i] = xxhash64.Sum64(v.Bytes())
		}
	}
	return h
}
