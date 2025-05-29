// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package pack

import (
	"context"
	"fmt"
	"slices"

	"blockwatch.cc/knoxdb/internal/block"
	"blockwatch.cc/knoxdb/internal/store"
	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/num"
)

// The storage model stores serialized blocks in a storage bucket. Each block is
// directly addressable with a unique computable key (pack key + schema field id).
// This model allows loading of individual blocks from disk without touching deleted
// or unneeded data.
//
// Keys are encoded as order-preserving varints. Values start with single header
// byte that defines an optional outer entropy compression format followed by a
// type specific encoding header and data.
//

func cacheKey(packkey uint32, blockId uint16) uint64 {
	return uint64(blockId)<<32 | uint64(packkey)
}

// EncodeBlockKey produces a block key for use inside the table's data bucket.
// We use the key to cluster blocks of the same column into on-disk data pages
// to amortize load costs.
func EncodeBlockKey(packkey uint32, blockId uint16) []byte {
	var b [num.MaxVarintLen32 + num.MaxVarintLen16]byte
	return num.AppendUvarint(
		num.AppendUvarint(b[:0], uint64(blockId)),
		uint64(packkey),
	)
}

func DecodeBlockKey(buf []byte) (packkey uint32, blockId uint16) {
	v, n := num.Uvarint(buf)
	blockId = uint16(v)
	v, _ = num.Uvarint(buf[n:])
	packkey = uint32(v)
	return
}

// Loads missing blocks from cache
func (p *Package) LoadFromCache(bcache block.BlockCachePartition, fids []uint16) int {
	fields := p.schema.Exported()
	var n int
	bcache.Lock()
	for i, b := range p.blocks {
		// skip already loaded blocks
		if b != nil {
			continue
		}

		// skip excluded blocks, load full schema when fids is nil
		if fids != nil && !slices.Contains(fids, fields[i].Id) {
			continue
		}

		// try cache lookup, will inc refcount
		block, _ := bcache.GetLocked(cacheKey(p.key, fields[i].Id))
		if block != nil {
			p.blocks[i] = block
			n++
		}
	}
	bcache.Unlock()
	return n
}

func (p *Package) AddToCache(bcache block.BlockCachePartition) {
	fields := p.schema.Exported()
	bcache.Lock()
	for i, b := range p.blocks {
		if b == nil {
			continue
		}
		bcache.ContainsOrAddLocked(cacheKey(p.key, fields[i].Id), b)
	}
	bcache.Unlock()
}

func (p *Package) DropFromCache(bcache block.BlockCachePartition) {
	fields := p.schema.Exported()
	bcache.Lock()
	for i, b := range p.blocks {
		if b == nil {
			continue
		}
		bcache.RemoveLocked(cacheKey(p.key, fields[i].Id))
	}
	bcache.Unlock()
}

// Loads missing blocks from disk, blocks are read-only
func (p *Package) LoadFromDisk(ctx context.Context, bucket store.Bucket, fids []uint16, nRows int) (int, error) {
	if bucket == nil {
		return 0, store.ErrNoBucket
	}

	var n int
	for i, f := range p.schema.Exported() {
		// skip already loaded blocks
		if p.blocks[i] != nil {
			continue
		}

		// skip excluded blocks, load full schema when fids is nil
		if fids != nil && !slices.Contains(fids, f.Id) {
			continue
		}

		// skip inactive fields
		if f.Flags.Is(types.FieldFlagDeleted) {
			continue
		}

		// generate storage key for this block
		bkey := EncodeBlockKey(p.key, f.Id)

		// load block data
		buf := bucket.Get(bkey)
		if buf == nil {
			// when missing (new fields in old packs) keep block nil
			continue
		}
		n += len(buf)

		// decode block from buffer page
		b, err := block.Decode(f.Type.BlockType(), buf)
		if err != nil {
			return n, fmt.Errorf("loading block 0x%08x:%02d: %v", p.key, f.Id, err)
		}
		p.blocks[i] = b
	}

	// check if all non-nil blocks are equal length
	for i, b := range p.blocks {
		// skip excluded blocks
		if b == nil {
			continue
		}
		// if nrows is unknown to the caller, fall back to the first block's length
		if nRows == 0 {
			nRows = b.Len()
			continue
		}
		// all subsequent blocks must have same len
		if nRows != b.Len() {
			return n, fmt.Errorf("loading block 0x%08x:%02d len mismatch exp=%d have=%d",
				p.key, i, nRows, b.Len())
		}
	}

	// set pack len here
	p.nRows = nRows

	return n, nil
}

// store all dirty blocks
func (p *Package) StoreToDisk(ctx context.Context, bucket store.Bucket) (int, error) {
	if bucket == nil {
		return 0, store.ErrNoBucket
	}

	var n int
	for i, f := range p.schema.Exported() {
		// skip empty blocks, clean blocks and deleted fields
		b := p.blocks[i]
		if b == nil || !b.IsDirty() || f.Flags.Is(types.FieldFlagDeleted) {
			continue
		}

		// encode block
		buf, stats, err := b.Encode(f.Compress)
		if err != nil {
			return 0, err
		}

		// generate storage key for this block
		bkey := EncodeBlockKey(p.key, f.Id)

		// export block statistics
		if p.stats != nil {
			minv, maxv := stats.MinMax()
			stats.Close()
			p.stats.MinMax[i][0] = minv
			p.stats.MinMax[i][1] = maxv
			p.stats.DiffSize[i] = len(buf) - len(bucket.Get(bkey))
		}
		n += len(buf)

		// write to store (will keep a reference to buf until tx closes,
		// so we cannot free buf at this point, TODO: free in buffer manager)
		if err := bucket.Put(bkey, buf); err != nil {
			return n, fmt.Errorf("storing block 0x%08x:%02d: %v", p.key, f.Id, err)
		}
		p.blocks[i].SetClean()
	}

	return n, nil
}

// delete all blocks from storage
func (p *Package) RemoveFromDisk(ctx context.Context, bucket store.Bucket) error {
	if bucket == nil {
		return store.ErrNoBucket
	}

	for _, f := range p.schema.Exported() {
		// don't check if key exists
		if err := bucket.Delete(EncodeBlockKey(p.key, f.Id)); err != nil {
			return fmt.Errorf("removing block 0x%016x:%02d: %v", p.key, f.Id, err)
		}
	}

	return nil
}
