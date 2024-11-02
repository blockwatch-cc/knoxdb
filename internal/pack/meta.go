// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package pack

import (
	"bytes"
	"context"
	"fmt"
	"slices"

	"blockwatch.cc/knoxdb/internal/block"
	"blockwatch.cc/knoxdb/internal/engine"
	"blockwatch.cc/knoxdb/internal/store"
	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/assert"
	"blockwatch.cc/knoxdb/pkg/schema"
)

const (
	// meta block positions
	MetaRidPos  = iota // 0
	MetaRefPos         // 1
	MetaXminPos        // 2
	MetaXmaxPos        // 3
	MetaLivePos        // 4
)

type PackMeta [5]*block.Block

func NewMeta() *PackMeta {
	return &PackMeta{}
}

func (m *PackMeta) Alloc(sz int) {
	m[MetaRidPos] = block.New(block.BlockUint64, sz)
	m[MetaRefPos] = block.New(block.BlockUint64, sz)
	m[MetaXminPos] = block.New(block.BlockUint64, sz)
	m[MetaXmaxPos] = block.New(block.BlockUint64, sz)
	m[MetaLivePos] = block.New(block.BlockBool, sz)
}

func (m *PackMeta) Clone(sz int) *PackMeta {
	return &PackMeta{
		m[MetaRidPos].Clone(sz),
		m[MetaRefPos].Clone(sz),
		m[MetaXminPos].Clone(sz),
		m[MetaXmaxPos].Clone(sz),
		m[MetaLivePos].Clone(sz),
	}
}

func (p *Package) HasMeta() bool {
	return p.xmeta != nil
}

func (p *Package) Meta() PackMeta {
	return *p.xmeta
}

func (p *Package) ReadMeta(row int) (m schema.Meta) {
	if p.xmeta != nil {
		m.Rid = p.xmeta[MetaRidPos].Uint64().Get(row)
		m.Ref = p.xmeta[MetaRefPos].Uint64().Get(row)
		m.Xmin = p.xmeta[MetaXminPos].Uint64().Get(row)
		m.Xmax = p.xmeta[MetaXmaxPos].Uint64().Get(row)
		m.IsLive = p.xmeta[MetaLivePos].Bool().IsSet(row)
	}
	return
}

func (p *Package) AppendMeta(m schema.Meta) {
	if p.xmeta != nil {
		p.xmeta[MetaRidPos].Uint64().Append(m.Rid)
		p.xmeta[MetaRefPos].Uint64().Append(m.Ref)
		p.xmeta[MetaXminPos].Uint64().Append(m.Xmin)
		p.xmeta[MetaXmaxPos].Uint64().Append(m.Xmax)
		p.xmeta[MetaLivePos].Bool().Append(m.IsLive)
	}
	return
}

// Loads xmeta blocks from disk, blocks are read-only
func (p *Package) LoadMeta(ctx context.Context, bucket store.Bucket, useCache bool, cacheKey uint64, fids []uint16, nRows int) (int, error) {
	if bucket == nil {
		return 0, engine.ErrNoBucket
	}

	// use block cache to lookup
	bcache := engine.GetEngine(ctx).BlockCache()
	ckey := engine.CacheKeyType{cacheKey, 0}

	// alloc meta blocks if missing
	if p.xmeta == nil {
		p.xmeta = NewMeta()
	}

	var n int
	for i := range p.xmeta {
		// skip already loaded blocks
		if p.xmeta[i] != nil {
			continue
		}

		id := schema.MetaRid - uint16(i)

		// skip excluded blocks, load full schema when fids is nil
		if fids != nil && !slices.Contains(fids, id) {
			continue
		}

		// try cache lookup first, will inc refcount
		ckey[1] = blockKey(p.key, id)
		if block, ok := bcache.Get(ckey); ok {
			p.xmeta[i] = block
			continue
		}

		// generate storage key for this block
		bkey := EncodeBlockKey(p.key, id)

		// load block data
		buf := bucket.Get(bkey)
		if buf == nil {
			// when missing (new fields in old packs) set block to nil
			if p.xmeta[i] != nil {
				p.xmeta[i].DecRef()
				p.xmeta[i] = nil
			}
			continue
		}
		n += len(buf)

		// alloc block (use actual storage size, arena will round up to power of 2)
		if p.xmeta[i] == nil {
			sz := nRows
			if sz == 0 {
				sz = p.maxRows
			}
			p.xmeta[i] = block.New(block.BlockUint64, sz)
		}

		// skip unused block compression
		buf = buf[1:]

		// fast-path, decode from buffer
		if err := p.xmeta[i].Decode(buf); err != nil {
			return n, fmt.Errorf("loading xmeta block 0x%08x:%02d: %v", p.key, i, err)
		}

		// cache loaded block, will inc refcount
		if useCache {
			ckey[1] = blockKey(p.key, id)
			bcache.Add(ckey, p.xmeta[i])
		}
	}

	// check if all non-nil blocks are equal length
	for i, b := range p.xmeta {
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
			return n, fmt.Errorf("loading xmeta pack 0x%08x: block %02d len %d mismatch %d",
				p.key, i, nRows, b.Len())
		}
	}

	// set pack len here
	p.nRows = nRows

	return n, nil
}

// store all dirty xmeta blocks
func (p *Package) StoreMeta(ctx context.Context, bucket store.Bucket, cacheKey uint64, stats []int) (int, error) {
	if bucket == nil {
		return 0, engine.ErrNoBucket
	}

	// ensure stats length
	if stats != nil {
		assert.Always(len(stats) == len(p.blocks), "block stats len mismatch",
			"nstats", len(stats),
			"nblocks", len(p.blocks),
		)
	}

	// remove updated blocks from cache
	bcache := engine.GetEngine(ctx).BlockCache()
	ckey := engine.CacheKeyType{cacheKey, 0}

	var n int

	for i := range p.xmeta {
		// skip empty and clean blocks
		if p.xmeta[i] == nil || !p.xmeta[i].IsDirty() {
			if stats != nil {
				stats[i] = 0
			}
			continue
		}

		// encode block data using optional compressor into new allocated buffers
		// (this is necessary because the underlying store may not copy our data)
		buf := bytes.NewBuffer(make([]byte, 0, p.xmeta[i].MaxStoredSize()))
		buf.WriteByte(byte(types.FieldCompressNone))

		// encode block
		_, err := p.xmeta[i].WriteTo(buf)
		if err != nil {
			return 0, err
		}

		// howto export block size statistics
		if stats != nil {
			stats[i] = buf.Len()
		}
		n += buf.Len()

		// generate storage key for this block
		id := schema.MetaRid - uint16(i)
		bkey := EncodeBlockKey(p.key, id)

		// write to store
		if err := bucket.Put(bkey, buf.Bytes()); err != nil {
			return n, fmt.Errorf("storing xmeta block 0x%08x:%02d: %v",
				p.key, i, err)
		}
		p.xmeta[i].SetClean()

		// drop cached blocks
		ckey[1] = blockKey(p.key, id)
		bcache.Remove(ckey)
	}

	return n, nil
}

// delete all xmeta blocks from storage and cache
func (p *Package) RemoveMeta(ctx context.Context, bucket store.Bucket, cacheKey uint64) error {
	if bucket == nil {
		return engine.ErrNoBucket
	}

	// remove blocks from cache
	bcache := engine.GetEngine(ctx).BlockCache()
	ckey := engine.CacheKeyType{cacheKey, 0}

	for i := schema.MetaRid; i >= schema.MetaLive; i-- {
		// don't check if key exists
		bkey := EncodeBlockKey(p.key, i)
		if err := bucket.Delete(bkey); err != nil {
			return fmt.Errorf("removing xmeta block 0x%016x:%02d: %v", p.key, i, err)
		}

		// drop cached blocks
		ckey[1] = blockKey(p.key, i)
		bcache.Remove(ckey)
	}

	return nil
}
