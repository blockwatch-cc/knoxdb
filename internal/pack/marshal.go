// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package pack

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"slices"

	"blockwatch.cc/knoxdb/internal/block"
	"blockwatch.cc/knoxdb/internal/engine"
	"blockwatch.cc/knoxdb/internal/store"
	"blockwatch.cc/knoxdb/internal/types"
)

// The storage model stores serialized blocks in a storage bucket. Each block is
// directly addressable with a unique computable key (pack key + schema field id).
// A single byte header defines an outer compression format.
//
// This model allows loading of individual blocks from disk without touching deleted
// or unneeded data.
//
// Any new file format should support directly addressable blocks and data checksums.

var (
	LE = binary.LittleEndian // values
	BE = binary.BigEndian    // keys

	// translate field type to block type
	blockTypes = [...]block.BlockType{
		types.FieldTypeInvalid:    block.BlockUint8,
		types.FieldTypeDatetime:   block.BlockTime,
		types.FieldTypeBoolean:    block.BlockBool,
		types.FieldTypeString:     block.BlockString,
		types.FieldTypeBytes:      block.BlockBytes,
		types.FieldTypeInt8:       block.BlockInt8,
		types.FieldTypeInt16:      block.BlockInt16,
		types.FieldTypeInt32:      block.BlockInt32,
		types.FieldTypeInt64:      block.BlockInt64,
		types.FieldTypeInt128:     block.BlockInt128,
		types.FieldTypeInt256:     block.BlockInt256,
		types.FieldTypeUint8:      block.BlockUint8,
		types.FieldTypeUint16:     block.BlockUint16,
		types.FieldTypeUint32:     block.BlockUint32,
		types.FieldTypeUint64:     block.BlockUint64,
		types.FieldTypeDecimal32:  block.BlockInt32,
		types.FieldTypeDecimal64:  block.BlockInt64,
		types.FieldTypeDecimal128: block.BlockInt128,
		types.FieldTypeDecimal256: block.BlockInt256,
		types.FieldTypeFloat32:    block.BlockFloat32,
		types.FieldTypeFloat64:    block.BlockFloat64,
	}
)

func blockKey(packkey uint32, blockId uint16) uint64 {
	return uint64(packkey)<<32 | uint64(blockId)
}

func encodeBlockKey(packkey uint32, blockId uint16) []byte {
	var b [8]byte
	BE.PutUint32(b[:], packkey)
	BE.PutUint16(b[6:], blockId)
	return b[:]
}

func decodeBlockKey(buf []byte) (packkey uint32, blockId uint16) {
	packkey = BE.Uint32(buf)
	blockId = BE.Uint16(buf[6:])
	return
}

// Loads missing blocks from disk, blocks are read-only
func (p *Package) Load(ctx context.Context, tx store.Tx, useCache bool, cacheKey uint64, bucketKey []byte, fids []uint16, nRows int) (int, error) {
	bucket := tx.Bucket(bucketKey)
	if bucket == nil {
		return 0, fmt.Errorf("missing bucket %s", string(bucketKey))
	}

	// use block cache to lookup
	bcache := engine.GetTransaction(ctx).Engine().BlockCache()
	ckey := engine.CacheKeyType{cacheKey, 0}

	var n int
	for i, f := range p.schema.Fields() {
		// skip already loaded blocks
		if p.blocks[i] != nil {
			continue
		}

		// skip excluded blocks, load full schema when fids is nil
		if fids != nil && !slices.Contains(fids, f.Id()) {
			continue
		}

		// generate storage key for this block
		bkey := encodeBlockKey(p.key, f.Id())

		// try cache lookup first, will inc refcount
		ckey[1] = blockKey(p.key, f.Id())
		if block, ok := bcache.Get(ckey); ok {
			p.blocks[i] = block
			continue
		}

		// load block data
		buf := bucket.Get(bkey)
		if buf == nil {
			// when missing (new fields in old packs) set block to nil
			if p.blocks[i] != nil {
				p.blocks[i].DecRef()
				p.blocks[i] = nil
			}
			continue
		}
		n += len(buf)

		// alloc block (use actual storage size, arena will round up to power of 2)
		if p.blocks[i] == nil {
			p.blocks[i] = block.New(blockTypes[f.Type()], p.maxRows)
		}

		// read block compression
		comp := types.FieldCompression(buf[0])
		// fmt.Printf("Load block %d with comp %d\n", f.Id(), comp)
		// fmt.Printf("Data %d\n%s", len(buf), hex.Dump(buf))
		buf = buf[1:]

		// decode block data
		var err error
		if comp > 0 {
			// decode block data with optional decompressor
			dec := NewDecompressor(bytes.NewBuffer(buf), comp)
			_, err = p.blocks[i].ReadFrom(dec)
			err2 := dec.Close()
			if err == nil {
				err = err2
			}
		} else {
			// fast-path, decode from buffer
			err = p.blocks[i].Decode(buf)
		}
		if err != nil {
			return n, fmt.Errorf("loading block 0x%08x:%02d from bucket %s: %v",
				p.key, f.Id(), string(bucketKey), err)
		}

		// cache loaded block, will inc refcount
		if useCache {
			ckey[1] = blockKey(p.key, f.Id())
			bcache.Add(ckey, p.blocks[i])
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
			return n, fmt.Errorf("loading pack 0x%08x: block %02d len %d mismatch %d",
				p.key, i, nRows, b.Len())
		}
	}

	// set pack len here
	p.nRows = nRows

	return n, nil
}

// store all dirty blocks
func (p *Package) Store(ctx context.Context, tx store.Tx, cacheKey uint64, bucketKey []byte, fill float64) (int, error) {
	bucket := tx.Bucket(bucketKey)
	if bucket == nil {
		return 0, fmt.Errorf("missing bucket %s", string(bucketKey))
	}
	bucket.FillPercent(fill)

	// remove updated blocks from cache
	bcache := engine.GetTransaction(ctx).Engine().BlockCache()
	ckey := engine.CacheKeyType{cacheKey, 0}

	var n int
	for i, f := range p.schema.Fields() {
		// skip empty blocks, clean blocks and deleted fields
		if p.blocks[i] == nil || !p.blocks[i].IsDirty() || f.Is(types.FieldFlagDeleted) {
			continue
		}

		// encode block data using optional compressor into new allocated buffers
		// (this is necessary because the underlying store may not copy our data)
		buf := bytes.NewBuffer(make([]byte, 0, p.blocks[i].MaxStoredSize()))
		buf.WriteByte(byte(f.Compress()))
		enc := NewCompressor(buf, f.Compress())

		// encode block
		_, err := p.blocks[i].WriteTo(enc)
		err2 := enc.Close()
		if err != nil {
			return 0, err
		}
		if err2 != nil {
			return 0, err2
		}

		// TODO: howto export block size statistics
		// p.StoredSize[i] = buf.Len()
		n += buf.Len()

		// generate storage key for this block
		bkey := encodeBlockKey(p.key, f.Id())
		// fmt.Printf("Store block %d with comp %d\n", f.Id(), f.Compress())
		// fmt.Printf("Data %d\n%s", buf.Len(), hex.Dump(buf.Bytes()))

		// write to store
		if err := bucket.Put(bkey, buf.Bytes()); err != nil {
			return n, fmt.Errorf("storing block 0x%08x:%02d in bucket %s: %v",
				p.key, f.Id(), string(bucketKey), err)
		}
		p.blocks[i].SetClean()

		// drop cached blocks
		ckey[1] = blockKey(p.key, f.Id())
		bcache.Remove(ckey)
	}

	return n, nil
}

// delete all blocks from storage and cache
func (p *Package) Remove(ctx context.Context, tx store.Tx, cacheKey uint64, bucketKey []byte) error {
	bucket := tx.Bucket(bucketKey)
	if bucket == nil {
		return fmt.Errorf("missing bucket %s", string(bucketKey))
	}

	// remove updated blocks from cache
	bcache := engine.GetTransaction(ctx).Engine().BlockCache()
	ckey := engine.CacheKeyType{cacheKey, 0}

	for _, f := range p.schema.Fields() {
		// don't check if key exists
		bkey := encodeBlockKey(p.key, f.Id())
		if err := bucket.Delete(bkey); err != nil {
			return fmt.Errorf("removing block 0x%016x:%02d from bucket %s: %v",
				p.key, f.Id(), string(bucketKey), err)
		}

		// drop cached blocks
		ckey[1] = blockKey(p.key, f.Id())
		bcache.Remove(ckey)
	}
	return nil
}
