// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package pack

import (
	"context"
	"sync/atomic"

	"blockwatch.cc/knoxdb/internal/engine"
	"blockwatch.cc/knoxdb/internal/metadata"
	"blockwatch.cc/knoxdb/internal/pack"
	"blockwatch.cc/knoxdb/pkg/util"
)

// Loads a shared pack for reading, uses block cache to lookup blocks.
// Stores loaded blocks unless useCache is false.
func (idx *Index) loadSharedPack(ctx context.Context, id uint32, nrow int, useCache bool) (*pack.Package, error) {
	// open read transaction (or reuse existing tx)
	tx, err := engine.GetTransaction(ctx).StoreTx(idx.db, false)
	if err != nil {
		return nil, err
	}

	// prepare a pack without block storage, we define its max size from
	// either nrows as passed by the caller (this may produce smaller packs
	// but is ok for read-only) and when missing use the configured max
	pkg := pack.New().
		WithKey(id).
		WithSchema(idx.schema).
		WithMaxRows(util.NonZero(nrow, idx.opts.PackSize))

	// load from table data bucket or cache using tableid as cache tag
	n, err := pkg.Load(ctx, tx, useCache, idx.indexId, idx.datakey, nil, nrow)
	if err != nil {
		pkg.Release()
		return nil, err
	}

	// count stats
	atomic.AddInt64(&idx.stats.BytesRead, int64(n))

	return pkg, nil
}

// Loads private copy of a pack for writing. Internally calls loadSharedPack
// and uses cached block copies, but clones cached blocks to make them private.
// Materializes blocks to native data representation to allow pack write methods
// to update/append data in place. Private packs may be written back to storage
// with storePack.
func (idx *Index) loadWritablePack(ctx context.Context, id uint32, nrow int) (*pack.Package, error) {
	// load a shared pack with full schema columns and regular capacity
	// (if known, otherwise max capacity, see above). Use cached blocks
	// if available, but do not pollute the cache when blocks have to be
	// loaded from disk.
	pkg, err := idx.loadSharedPack(ctx, id, nrow, false)
	if err != nil {
		return nil, err
	}

	// clone the shared pack into a full capacity writeable pack.
	// produce materialized column vectors (bytes/strings mostly)
	// in case we later allow encoded/optimized int vectors
	// we call materialize so that encodings are native
	clone := pkg.Clone(idx.opts.PackSize).Materialize()

	// release the shared pack again
	pkg.Release()

	return clone, nil
}

// Stores pack and updates metadata (statistics)
func (idx *Index) storePack(ctx context.Context, pkg *pack.Package) (int, error) {
	// open write transaction (or reuse existing tx)
	tx, err := engine.GetTransaction(ctx).StoreTx(idx.db, true)
	if err != nil {
		return 0, err
	}

	// remove zero length packs
	if pkg.Len() == 0 {
		// drop from metadata
		idx.meta.Remove(pkg.Key())

		// store metadata changes
		m, err := idx.meta.Store(ctx, tx, idx.metakey, idx.opts.PageFill)
		if err != nil {
			return 0, err
		}

		// remove from storage and block caches
		if err := pkg.Remove(ctx, tx, idx.indexId, idx.datakey); err != nil {
			return 0, err
		}

		// collect stats
		atomic.AddInt64(&idx.stats.MetaBytesWritten, int64(m))
		atomic.StoreInt64(&idx.stats.MetaSize, int64(idx.meta.HeapSize()))
		atomic.StoreInt64(&idx.stats.TotalSize, int64(idx.meta.TableSize()))

		return 0, nil
	}

	// update regular packs

	// optimize/dedup
	pkg.Optimize()

	// build block statistics first (block dirty flag is reset on save)
	fields := idx.schema.Fields()
	meta, ok := idx.meta.GetByKey(pkg.Key())
	if !ok {
		// create new metadata
		meta = &metadata.PackMetadata{
			Key:      pkg.Key(),
			SchemaId: pkg.Schema().Hash(),
			NValues:  pkg.Len(),
			Blocks:   make([]metadata.BlockMetadata, 0, idx.schema.NumFields()),
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
	n, err := pkg.Store(ctx, tx, idx.indexId, idx.datakey, idx.opts.PageFill)
	if err != nil {
		return 0, err
	}
	meta.StoredSize = n

	// update and store statistics
	idx.meta.AddOrUpdate(meta)
	m, err := idx.meta.Store(ctx, tx, idx.metakey, idx.opts.PageFill)
	if err != nil {
		return n, err
	}

	atomic.AddInt64(&idx.stats.BytesWritten, int64(n))
	atomic.AddInt64(&idx.stats.MetaBytesWritten, int64(m))
	atomic.StoreInt64(&idx.stats.MetaSize, int64(idx.meta.HeapSize()))
	atomic.StoreInt64(&idx.stats.TotalSize, int64(idx.meta.TableSize()))

	return n + m, nil
}

// Splits a writebale pack into two same size packs and stores both. Source pack
// must be storted by pk before splitting. Potentially cached blocks from source
// pack are dropped when the pack is stored after shortening.
func (idx *Index) splitPack(ctx context.Context, pkg *pack.Package) (int, error) {
	// prepare writeable pack with block storage
	pkg2 := pack.New().
		WithSchema(idx.schema).
		WithMaxRows(idx.opts.PackSize).
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
	n, err := idx.storePack(ctx, pkg)
	if err != nil {
		return 0, err
	}

	// set the new pack's key after storing pack 1, this avoids
	// using the same pack key when the source pack was not stored before
	pkg2.WithKey(idx.meta.NextKey())

	// save the new pack
	m, err := idx.storePack(ctx, pkg2)
	if err != nil {
		return 0, err
	}
	pkg2.Release()

	return n + m, nil
}
