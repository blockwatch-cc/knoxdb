// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package block

import (
	"blockwatch.cc/knoxdb/internal/hash/xxhash"
	"blockwatch.cc/knoxdb/internal/hash/xxhashVec"
)

func (b *Block) Hash() *Block {
	l := b.Len()
	h := New(BlockUint64, l)
	h.len = l

	switch b.typ {
	case BlockTime, BlockFloat64, BlockInt64, BlockUint64:
		xxhashVec.XXHash64Uint64Slice(b.Uint64().Slice(), h.Uint64().Slice())
	case BlockUint32, BlockInt32, BlockFloat32:
		xxhashVec.XXHash64Uint32Slice(b.Uint32().Slice(), h.Uint64().Slice())
	case BlockUint16, BlockInt16:
		xxhashVec.XXHash64Uint16Slice(b.Uint16().Slice(), h.Uint64().Slice())
	case BlockUint8, BlockInt8:
		xxhashVec.XXHash64Uint8Slice(b.Uint8().Slice(), h.Uint64().Slice())
	case BlockBool:
		zero, one := xxhash.Sum64([]byte{0}), xxhash.Sum64([]byte{1})
		bits := b.Bool()
		u64 := h.Uint64().Slice()
		for i := 0; i < l; i++ {
			if bits.IsSet(i) {
				u64[i] = one
			} else {
				u64[i] = zero
			}
		}
	case BlockBytes:
		u64 := h.Uint64().Slice()
		bytes := b.Bytes()
		for i := 0; i < l; i++ {
			u64[i] = xxhash.Sum64(bytes.Elem(i))
		}
	case BlockInt128:
		u64 := h.Uint64().Slice()
		i128 := b.Int128()
		for i := 0; i < l; i++ {
			u64[i] = xxhash.Sum64(i128.Elem(i).Bytes())
		}
	case BlockInt256:
		u64 := h.Uint64().Slice()
		i256 := b.Int256()
		for i := 0; i < l; i++ {
			u64[i] = xxhash.Sum64(i256.Elem(i).Bytes())
		}
	}
	return h
}
