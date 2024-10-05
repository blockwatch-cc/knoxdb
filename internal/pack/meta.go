// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package pack

import (
	"bytes"
	"context"
	"fmt"

	"blockwatch.cc/knoxdb/internal/block"
	"blockwatch.cc/knoxdb/internal/engine"
	"blockwatch.cc/knoxdb/internal/store"
	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/assert"
	"blockwatch.cc/knoxdb/pkg/schema"
)

const (
	// reserved metadata field ids
	RecordId   uint16 = 0xFFFF
	RecordRef  uint16 = 0xFFFE
	RecordXmin uint16 = 0xFFFD
	RecordXmax uint16 = 0xFFFC
	RecordDead uint16 = 0xFFFB
)

// Internal schema fields for transaction metadata
type Meta struct {
	Rid    uint64 `knox:"$rid,internal"`  // unique row id (not unique pk)
	Ref    uint64 `knox:"$ref,internal"`  // previous version, ref == rid on first insert
	Xmin   uint64 `knox:"$xmin,internal"` // txid where this row was created
	Xmax   uint64 `knox:"$xmax,internal"` // txid where this row was deleted
	IsDead bool   `knox:"$dead,internal"` // record is deleted
}

var MetaSchema = schema.MustSchemaOf(Meta{})

type PackMeta [5]*block.Block

func NewMeta() *PackMeta {
	return &PackMeta{}
}

func (m *PackMeta) Alloc(sz int) {
	m[0] = block.New(block.BlockUint64, sz)
	m[1] = block.New(block.BlockUint64, sz)
	m[2] = block.New(block.BlockUint64, sz)
	m[3] = block.New(block.BlockUint64, sz)
	m[4] = block.New(block.BlockBool, sz)
}

func (m *PackMeta) Clone(sz int) *PackMeta {
	return &PackMeta{
		m[0].Clone(sz),
		m[1].Clone(sz),
		m[2].Clone(sz),
		m[3].Clone(sz),
		m[4].Clone(sz),
	}
}

func (p *Package) HasMeta() bool {
	return p.xmeta != nil
}

func (p *Package) MetaBlocks() PackMeta {
	return *p.xmeta
}

func (p *Package) ReadMeta(row int) (m Meta) {
	if p.xmeta != nil {
		m.Rid = p.xmeta[0].Uint64().Get(row)
		m.Ref = p.xmeta[1].Uint64().Get(row)
		m.Xmin = p.xmeta[2].Uint64().Get(row)
		m.Xmax = p.xmeta[3].Uint64().Get(row)
		m.IsDead = p.xmeta[4].Bool().IsSet(row)
	}
	return
}

// Loads xmeta blocks from disk, blocks are read-only
func (p *Package) LoadMeta(ctx context.Context, tx store.Tx, useCache bool, cacheKey uint64, name string, nRows int) (int, error) {
	key := append([]byte(name), MetaKeySuffix...)
	bucket := tx.Bucket(key)
	if bucket == nil {
		return 0, fmt.Errorf("missing xmeta bucket %s", string(key))
	}

	// use block cache to lookup
	bcache := engine.GetTransaction(ctx).Engine().BlockCache()
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
		id := RecordId - uint16(i)

		// try cache lookup first, will inc refcount
		ckey[1] = blockKey(p.key, id)
		if block, ok := bcache.Get(ckey); ok {
			p.xmeta[i] = block
			continue
		}

		// generate storage key for this block
		bkey := encodeBlockKey(p.key, id)

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
			return n, fmt.Errorf("loading xmeta block 0x%08x:%02d from bucket %s: %v",
				p.key, i, string(key), err)
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
func (p *Package) StoreMeta(ctx context.Context, tx store.Tx, cacheKey uint64, name string, fill float64, stats []int) (int, error) {
	key := append([]byte(name), MetaKeySuffix...)
	bucket := tx.Bucket(key)
	if bucket == nil {
		return 0, fmt.Errorf("missing xmeta bucket %s", string(key))
	}
	bucket.FillPercent(fill)

	// ensure stats length
	if stats != nil {
		assert.Always(len(stats) == len(p.blocks), "block stats len mismatch",
			"nstats", len(stats),
			"nblocks", len(p.blocks),
		)
	}

	// remove updated blocks from cache
	bcache := engine.GetTransaction(ctx).Engine().BlockCache()
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
		id := RecordId - uint16(i)
		bkey := encodeBlockKey(p.key, id)

		// write to store
		if err := bucket.Put(bkey, buf.Bytes()); err != nil {
			return n, fmt.Errorf("storing xmeta block 0x%08x:%02d in bucket %s: %v",
				p.key, i, string(key), err)
		}
		p.xmeta[i].SetClean()

		// drop cached blocks
		ckey[1] = blockKey(p.key, id)
		bcache.Remove(ckey)
	}

	return n, nil
}

// delete all xmeta blocks from storage and cache
func (p *Package) RemoveMeta(ctx context.Context, tx store.Tx, cacheKey uint64, name string) error {
	key := append([]byte(name), MetaKeySuffix...)
	bucket := tx.Bucket(key)
	if bucket == nil {
		return fmt.Errorf("missing xmeta bucket %s", string(key))
	}

	// remove blocks from cache
	bcache := engine.GetTransaction(ctx).Engine().BlockCache()
	ckey := engine.CacheKeyType{cacheKey, 0}

	for i := RecordId; i >= RecordDead; i-- {
		// don't check if key exists
		bkey := encodeBlockKey(p.key, i)
		if err := bucket.Delete(bkey); err != nil {
			return fmt.Errorf("removing xmeta block 0x%016x:%02d from bucket %s: %v",
				p.key, i, string(key), err)
		}

		// drop cached blocks
		ckey[1] = blockKey(p.key, i)
		bcache.Remove(ckey)
	}

	return nil
}
