// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package pack

import (
	"context"
	"sync/atomic"

	"blockwatch.cc/knoxdb/internal/engine"
	"blockwatch.cc/knoxdb/internal/metadata"
	"blockwatch.cc/knoxdb/internal/pack"
	"blockwatch.cc/knoxdb/pkg/schema"
	"blockwatch.cc/knoxdb/util"
)

// Loads a shared pack for reading, uses block cache to lookup blocks.
// Stores loaded blocks unless useCache is false.
func (t *Table) loadSharedPack(ctx context.Context, id uint32, nrow int, useCache bool, s *schema.Schema) (*pack.Package, error) {
	// open read transaction (or reuse existing tx)
	tx, err := engine.GetTransaction(ctx).StoreTx(t.db, false)
	if err != nil {
		return nil, err
	}

	// list of fields to load
	fids := s.FieldIDs()

	// prepare a pack without block storage
	pkg := pack.New().
		WithKey(id).
		WithSchema(t.schema).
		WithMaxRows(util.NonZero(nrow, t.opts.PackSize))

	// load from table data bucket or cache using tableid as cache tag
	n, err := pkg.Load(ctx, tx, useCache, t.tableId, t.datakey, fids, nrow)
	if err != nil {
		pkg.Release()
		return nil, err
	}

	// count stats
	atomic.AddInt64(&t.stats.PacksLoaded, 1)
	atomic.AddInt64(&t.stats.BytesRead, int64(n))

	return pkg, nil
}

// Loads private copy of a pack for writing. Internally calls loadSharedPack
// and uses cached block copies, but clones cached blocks to make them private.
// Materializes blocks to native data representation to allow pack write methods
// to update/append data in place. Private packs may be written back to storage
// with storePack.
func (t *Table) loadWritablePack(ctx context.Context, id uint32, nrow int) (*pack.Package, error) {
	// load a shared pack with full schema columns and max capacity
	// using cached blocks if available, but do not pollute cache
	// when blocks are loaded from disk
	pkg, err := t.loadSharedPack(ctx, id, nrow, false, t.schema)
	if err != nil {
		return nil, err
	}

	// clone the shared pack (this produces materialized columns) into
	// a full capacity writable pack
	// in case we later allow encoded/optimized int vectors
	// we call materialize so that encodings are native
	clone := pkg.Clone(t.opts.PackSize).Materialize()

	// release the shared pack
	pkg.Release()

	return clone, nil
}

// Stores pack and updates metadata (statistics)
func (t *Table) storePack(ctx context.Context, pkg *pack.Package) (int, error) {
	// open write transaction (or reuse existing tx)
	tx, err := engine.GetTransaction(ctx).StoreTx(t.db, true)
	if err != nil {
		return 0, err
	}

	// remove zero length packs
	if pkg.Len() == 0 {
		// drop from metadata
		t.meta.Remove(pkg.Key())

		// store metadata changes
		m, err := t.meta.Store(ctx, tx, t.metakey, t.opts.PageFill)
		if err != nil {
			return 0, err
		}

		// remove from storage and block caches
		if err := pkg.Remove(ctx, tx, t.tableId, t.datakey); err != nil {
			return 0, err
		}

		// collect stats
		atomic.AddInt64(&t.stats.MetaBytesWritten, int64(m))
		atomic.StoreInt64(&t.stats.PacksCount, int64(t.meta.Len()))
		atomic.StoreInt64(&t.stats.MetaSize, int64(t.meta.HeapSize()))
		atomic.StoreInt64(&t.stats.TotalSize, int64(t.meta.TableSize()))

		return 0, nil
	}

	// update regular packs

	// optimize/dedup
	pkg.Optimize()

	// build block statistics first (block dirty flag is reset on save)
	fields := t.schema.Fields()
	meta, ok := t.meta.GetByKey(pkg.Key())
	if !ok {
		// create new metadata
		meta = &metadata.PackMetadata{
			Key:      pkg.Key(),
			SchemaId: pkg.Schema().Hash(),
			NValues:  pkg.Len(),
			Blocks:   make([]metadata.BlockMetadata, 0, t.schema.NumFields()),
			Dirty:    true,
		}

		for i, b := range pkg.Blocks() {
			meta.Blocks = append(meta.Blocks, metadata.NewBlockMetadata(b, &fields[i]))
		}
	} else {
		// update statistics for dirty blocks
		for i, b := range pkg.Blocks() {
			if !b.IsDirty() {
				continue
			}
			meta.Blocks[i] = metadata.NewBlockMetadata(b, &fields[i])
			meta.Dirty = true
		}
	}

	// write to disk
	n, err := pkg.Store(ctx, tx, t.tableId, t.datakey, t.opts.PageFill)
	if err != nil {
		return 0, err
	}
	meta.StoredSize = n

	// update and store statistics
	t.meta.AddOrUpdate(meta)
	m, err := t.meta.Store(ctx, tx, t.metakey, t.opts.PageFill)
	if err != nil {
		return n, err
	}

	atomic.AddInt64(&t.stats.PacksStored, 1)
	atomic.AddInt64(&t.stats.BytesWritten, int64(n))
	atomic.AddInt64(&t.stats.MetaBytesWritten, int64(m))
	atomic.StoreInt64(&t.stats.PacksCount, int64(t.meta.Len()))
	atomic.StoreInt64(&t.stats.MetaSize, int64(t.meta.HeapSize()))
	atomic.StoreInt64(&t.stats.TotalSize, int64(t.meta.TableSize()))

	return n + m, nil
}

// Splits a writebale pack into two same size packs and stores both. Source pack
// must be storted by pk before splitting. Potentially cached blocks from source
// pack are dropped when the pack is stored after shortening.
func (t *Table) splitPack(ctx context.Context, pkg *pack.Package) (int, error) {
	// prepare writeable pack with block storage
	pkg2 := pack.New().
		WithSchema(t.schema).
		WithMaxRows(t.opts.PackSize).
		Alloc()

	// move half of the data between packs
	half := pkg.Len() / 2
	if err := pkg2.AppendPack(pkg, half, pkg.Len()-half); err != nil {
		return 0, err
	}
	if err := pkg.Delete(half, pkg.Len()-half); err != nil {
		return 0, err
	}

	// store the source pack (this adds or updates its metadata)
	n, err := t.storePack(ctx, pkg)
	if err != nil {
		return 0, err
	}

	// set the new pack's key after storing pack 1, this avoids
	// using the same pack key when the source pack was not stored before
	pkg2.WithKey(t.meta.NextKey())

	// save the new pack
	m, err := t.storePack(ctx, pkg2)
	if err != nil {
		return 0, err
	}
	pkg2.Release()

	return n + m, nil
}
