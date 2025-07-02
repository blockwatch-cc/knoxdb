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
// directly addressable with a unique computable key (pack id + field id + version).
// This model allows loading of individual blocks from disk without touching deleted
// or unneeded data. A version allows to keep multiple generations on storage.
// It is safe to truncate version to u16 or shorter since each pack version is only
// used for a short number of merge epochs.
//
// Keys are encoded as order-preserving varints. Values start with single header
// byte that defines an optional outer entropy compression format followed by a
// type specific encoding header and data.
//

func cacheKey(packkey, version uint32, blockId uint16) uint64 {
	return uint64(blockId)<<48 | uint64(version&0xFFFF)<<32 | uint64(packkey)
}

// EncodeBlockKey produces a block key for use inside the table's data bucket.
// The key clusters blocks of the same column into on-disk data pages which
// amortizes load costs.
func EncodeBlockKey(packkey, version uint32, blockId uint16) []byte {
	var b [num.MaxVarintLen32 + 2*num.MaxVarintLen16]byte
	buf := num.AppendUvarint(b[:0], uint64(blockId))
	buf = num.AppendUvarint(buf, uint64(packkey))
	buf = num.AppendUvarint(buf, uint64(version))
	return buf
}

func DecodeBlockKey(buf []byte) (packkey uint32, version uint32, blockId uint16) {
	v, n := num.Uvarint(buf)
	buf = buf[n:]
	blockId = uint16(v)
	v, n = num.Uvarint(buf)
	buf = buf[n:]
	packkey = uint32(v)
	v, _ = num.Uvarint(buf)
	version = uint32(v)
	return
}

// Loads missing blocks from cache
func (p *Package) LoadFromCache(bcache block.BlockCachePartition, fids []uint16) int {
	fields := p.schema.Exported()
	var n, nRows int
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
		block, _ := bcache.GetLocked(cacheKey(p.key, p.version, fields[i].Id))
		if block != nil {
			p.blocks[i] = block
			n++
			nRows = max(nRows, block.Len())
		}
	}
	bcache.Unlock()

	// check if all non-nil blocks are equal length
	for i, b := range p.blocks {
		// skip excluded blocks
		if b == nil {
			continue
		}
		// if nrows is unknown use the first block's length
		if nRows == 0 {
			nRows = b.Len()
			continue
		}
		// all blocks must have same len
		if nRows != b.Len() {
			panic(fmt.Errorf("cached block 0x%08x:%02d[v%d] len mismatch exp=%d have=%d",
				p.key, i, p.version, nRows, b.Len()))
		}
	}

	// set pack len here
	p.nRows = nRows

	return n
}

func (p *Package) AddToCache(bcache block.BlockCachePartition) {
	fields := p.schema.Exported()
	bcache.Lock()
	for i, b := range p.blocks {
		if b == nil {
			continue
		}
		bcache.ContainsOrAddLocked(cacheKey(p.key, p.version, fields[i].Id), b)
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
		bcache.RemoveLocked(cacheKey(p.key, p.version, fields[i].Id))
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
		bkey := EncodeBlockKey(p.key, p.version, f.Id)

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

// store all blocks
func (p *Package) StoreToDisk(ctx context.Context, bucket store.Bucket) (int, error) {
	if bucket == nil {
		return 0, store.ErrNoBucket
	}

	var n int
	for i, f := range p.schema.Exported() {
		// skip empty blocks, deleted fields; write all blocks for consistent version
		b := p.blocks[i]
		if b == nil || f.Flags.Is(types.FieldFlagDeleted) {
			continue
		}

		// encode block
		buf, stats, err := b.Encode(f.Compress)
		if err != nil {
			return 0, err
		}

		// if len(buf) > 20<<20 {
		// 	fmt.Printf("WARNING: suspiciously large block %s/%s/0x%08x:%02d[v%d] %d bytes\n",
		// 		p.schema.Name(), p.schema.Field(i).Name(), p.Key(), i, p.Version(), len(buf))
		// }

		// minv, maxv := stats.MinMax()
		// fmt.Printf("store block 0x%08x:%02d[v%d]: len=%d size=%d min=%v max=%v\n",
		// 	p.key, f.Id, p.version, b.Len(), len(buf), minv, maxv)

		// generate new and old storage keys for this block
		bkey := EncodeBlockKey(p.key, p.version, f.Id)
		okey := EncodeBlockKey(p.key, p.version-1, f.Id)

		// export block statistics
		if p.stats != nil {
			minv, maxv := stats.MinMax()
			p.stats.MinMax[i][0] = minv
			p.stats.MinMax[i][1] = maxv
			p.stats.Unique[i] = stats.Unique()
			p.stats.DiffSize[i] = len(buf) - len(bucket.Get(okey))
		}
		stats.Close()
		n += len(buf)

		// write to store (will keep a reference to buf until tx closes,
		// so we cannot free buf at this point, TODO: likely changes with buffer manager)
		if err := bucket.Put(bkey, buf); err != nil {
			return n, fmt.Errorf("storing block 0x%08x:%02d[v%d]: %v", p.key, f.Id, p.version, err)
		}
		p.blocks[i].SetClean()
	}

	return n, nil
}

// Deprecated, deletion is deferred to garbage collection
// delete all blocks from storage
// func (p *Package) RemoveFromDisk(ctx context.Context, bucket store.Bucket) error {
// 	if bucket == nil {
// 		return store.ErrNoBucket
// 	}

// 	for _, f := range p.schema.Exported() {
// 		// don't check if key exists
// 		if err := bucket.Delete(EncodeBlockKey(p.key, p.version, f.Id)); err != nil {
// 			return fmt.Errorf("removing block 0x%016x:%02d[v%d]: %v", p.key, f.Id, p.version, err)
// 		}
// 	}

// 	return nil
// }
