// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package index

import (
	"blockwatch.cc/knoxdb/internal/block"
	"blockwatch.cc/knoxdb/internal/hash"
	"blockwatch.cc/knoxdb/pkg/slicex"
)

// Produces hash values for all elements in the block.
func HashBlock(b *block.Block) *block.Block {
	l := b.Len()
	h := block.New(block.BlockUint64, l)
	h.Reserve(l)
	u64 := h.Uint64().Slice()

	switch b.Type() {
	case block.BlockFloat64, block.BlockInt64, block.BlockUint64:
		if b.IsMaterialized() {
			hash.Vec64(h.Uint64().Slice(), b.Uint64().Slice())
		} else {
			for i, v := range b.Uint64().All() {
				u64[i] = hash.Uint64(v)
			}
		}
	case block.BlockUint32, block.BlockInt32, block.BlockFloat32:
		if b.IsMaterialized() {
			hash.Vec32(h.Uint64().Slice(), b.Uint32().Slice())
		} else {
			for i, v := range b.Uint32().All() {
				u64[i] = hash.Uint32(v)
			}
		}
	case block.BlockUint16, block.BlockInt16:
		if b.IsMaterialized() {
			hash.Vec16(h.Uint64().Slice(), b.Uint16().Slice())
		} else {
			for i, v := range b.Uint16().All() {
				u64[i] = hash.Uint16(v)
			}
		}
	case block.BlockUint8, block.BlockInt8:
		if b.IsMaterialized() {
			hash.Vec8(h.Uint64().Slice(), b.Uint8().Slice())
		} else {
			for i, v := range b.Uint8().All() {
				u64[i] = hash.Uint8(v)
			}
		}
	case block.BlockBool:
		zero, one := hash.Hash([]byte{0}), hash.Hash([]byte{1})
		bits := b.Bool()
		switch {
		case bits.All():
			slicex.Fill(u64, one)
		case bits.None():
			slicex.Fill(u64, zero)
		default:
			slicex.Fill(u64, zero)
			for i := range bits.Ones() {
				u64[i] = one
			}
		}
	case block.BlockBytes:
		for i, v := range b.Bytes().All() {
			u64[i] = hash.Hash(v)
		}
	case block.BlockInt128:
		for i, v := range b.Int128().All() {
			u64[i] = hash.Hash(v.Bytes())
		}
	case block.BlockInt256:
		for i, v := range b.Int256().All() {
			u64[i] = hash.Hash(v.Bytes())
		}
	}
	return h
}
