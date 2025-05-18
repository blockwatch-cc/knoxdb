// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package pack

import (
	"bytes"
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

// translate field type to block type
var blockTypes = types.BlockTypes

func cacheKey(packkey uint32, blockId uint16) uint64 {
	return uint64(packkey)<<32 | uint64(blockId)
}

func EncodeBlockKey(packkey uint32, blockId uint16) []byte {
	var b [num.MaxVarintLen32 + num.MaxVarintLen16]byte
	return num.AppendUvarint(
		num.AppendUvarint(b[:0], uint64(packkey)),
		uint64(blockId),
	)
}

func DecodeBlockKey(buf []byte) (packkey uint32, blockId uint16) {
	v, n := num.Uvarint(buf)
	packkey = uint32(v)
	v, _ = num.Uvarint(buf[n:])
	blockId = uint16(v)
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

		// TODO: alloc is part of new containers, no longer required here
		// alloc block (use actual storage size, arena will round up to power of 2)
		if p.blocks[i] == nil {
			sz := nRows
			if sz == 0 {
				sz = p.maxRows
			}
			p.blocks[i] = block.New(blockTypes[f.Type], sz)
		}

		// read block compression
		comp := types.FieldCompression(buf[0])
		buf = buf[1:]

		// decode block data
		var err error
		if comp > 0 {
			// decode block data with optional decompressor
			dec := NewDecompressor(bytes.NewBuffer(buf), comp)

			// TODO: readall and decode (deprecate ReadFrom)
			_, err = p.blocks[i].ReadFrom(dec)
			err2 := dec.Close()
			if err == nil {
				err = err2
			}

			// TODO: BufferManager: at this point we hold a copy of the decompressed
			// data referenced by a container and we can release any page locks

		} else {
			// TODO: BufferManager: here we reference data in pages and must hold
			// the lock until the block is released (move page lock release into
			// block.DeRef)

			// fast-path, decode from buffer
			err = p.blocks[i].Decode(buf)
		}
		if err != nil {
			return n, fmt.Errorf("loading block 0x%08x:%02d: %v", p.key, f.Id, err)
		}
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

	// TODO: re-use encoder analysis data here
	// analyze
	p.WithAnalysis()

	// optimize blocks before writing (dedup)
	// TODO: move this into an integrated analysis & encode pipeline
	// TODO: don't do this in-place so a writer pack can be reused
	p.Optimize()

	var n int
	for i, f := range p.schema.Exported() {
		// skip empty blocks, clean blocks and deleted fields
		if p.blocks[i] == nil || !p.blocks[i].IsDirty() || f.Flags.Is(types.FieldFlagDeleted) {
			continue
		}

		// encode block data using optional compressor into new allocated buffers
		// (this is necessary because the underlying store may not copy our data)
		buf := bytes.NewBuffer(make([]byte, 0, p.blocks[i].MaxStoredSize()))
		buf.WriteByte(byte(f.Compress))
		enc := NewCompressor(buf, f.Compress)

		// encode block
		_, err := p.blocks[i].WriteTo(enc)
		err2 := enc.Close()
		if err != nil {
			return 0, err
		}
		if err2 != nil {
			return 0, err2
		}

		// TODO: cluster keys on block id (so that pages contain data from the same column)

		// generate storage key for this block
		bkey := EncodeBlockKey(p.key, f.Id)

		// export block size statistics
		p.analyze.DiffSize[i] = buf.Len() - len(bucket.Get(bkey))
		n += buf.Len()

		// write to store
		if err := bucket.Put(bkey, buf.Bytes()); err != nil {
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
