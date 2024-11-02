// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package table

import (
	"context"
	"sync/atomic"

	"blockwatch.cc/knoxdb/internal/engine"
	"blockwatch.cc/knoxdb/internal/pack"
	"blockwatch.cc/knoxdb/internal/pack/stats"
	"blockwatch.cc/knoxdb/internal/store"
	"blockwatch.cc/knoxdb/pkg/schema"
	"blockwatch.cc/knoxdb/pkg/util"
)

func (t *Table) dataBucket(tx store.Tx) store.Bucket {
	key := append([]byte(t.schema.Name()), pack.DataKeySuffix...)
	b := tx.Bucket(key)
	if b != nil {
		b.FillPercent(t.opts.PageFill)
	}
	return b
}

func (t *Table) statsBucket(tx store.Tx) store.Bucket {
	key := append([]byte(t.schema.Name()), pack.StatsKeySuffix...)
	b := tx.Bucket(key)
	if b != nil {
		b.FillPercent(t.opts.PageFill)
	}
	return b
}

// Loads a shared pack for reading, uses block cache to lookup blocks.
// Stores loaded blocks unless useCache is false.
func (t *Table) loadSharedPack(ctx context.Context, id uint32, nrow int, useCache bool, s *schema.Schema) (*pack.Package, error) {
	// open read transaction (or reuse existing tx)
	tx, err := engine.GetTransaction(ctx).StoreTx(t.db, false)
	if err != nil {
		return nil, err
	}

	// list of fields to load
	fids := s.ActiveFieldIds()

	// prepare a pack without block storage
	pkg := pack.New().
		WithKey(id).
		WithSchema(t.schema).
		WithMaxRows(util.NonZero(nrow, t.opts.PackSize))

	// load from table data bucket or cache using tableid as cache tag
	n, err := pkg.Load(ctx, t.dataBucket(tx), useCache, t.tableId, fids, nrow)
	if err != nil {
		pkg.Release()
		return nil, err
	}

	// count metrics
	atomic.AddInt64(&t.metrics.PacksLoaded, 1)
	atomic.AddInt64(&t.metrics.BytesRead, int64(n))

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

// Stores pack and updates stats
func (t *Table) storePack(ctx context.Context, pkg *pack.Package) (int, error) {
	// open write transaction (or reuse existing tx)
	tx, err := engine.GetTransaction(ctx).StoreTx(t.db, true)
	if err != nil {
		return 0, err
	}

	// remove zero length packs
	if pkg.Len() == 0 {
		// drop from stats
		t.stats.Remove(pkg.Key())

		// store stats changes
		m, err := t.stats.Store(ctx, t.statsBucket(tx))
		if err != nil {
			return 0, err
		}

		// remove from storage and block caches
		if err := pkg.Remove(ctx, t.dataBucket(tx), t.tableId); err != nil {
			return 0, err
		}

		// collect stats
		atomic.AddInt64(&t.metrics.MetaBytesWritten, int64(m))
		atomic.StoreInt64(&t.metrics.PacksCount, int64(t.stats.Len()))
		atomic.StoreInt64(&t.metrics.MetaSize, int64(t.stats.HeapSize()))
		atomic.StoreInt64(&t.metrics.TotalSize, int64(t.stats.TableSize()))

		return 0, nil
	}

	// update regular packs

	// optimize/dedup
	pkg.Optimize()

	// build block statistics first (block dirty flag is reset on save)
	fields := t.schema.Fields()
	pstats, ok := t.stats.GetByKey(pkg.Key())
	if !ok {
		// create new stats
		pstats = &stats.PackStats{
			Key:      pkg.Key(),
			SchemaId: pkg.Schema().Hash(),
			NValues:  pkg.Len(),
			Blocks:   make([]stats.BlockStats, 0, t.schema.NumFields()),
			Dirty:    true,
		}

		for i, b := range pkg.Blocks() {
			pstats.Blocks = append(pstats.Blocks, stats.NewBlockStats(b, &fields[i]))
		}
	} else {
		// update statistics for dirty blocks
		for i, b := range pkg.Blocks() {
			if !b.IsDirty() {
				continue
			}
			pstats.Blocks[i] = stats.NewBlockStats(b, &fields[i])
			pstats.Dirty = true
		}
		pstats.NValues = pkg.Len()
	}

	// write to disk
	blockSizes := make([]int, len(pstats.Blocks))
	n, err := pkg.Store(ctx, t.dataBucket(tx), t.tableId, blockSizes)
	if err != nil {
		return 0, err
	}
	pstats.StoredSize = n
	for i := range pstats.Blocks {
		pstats.Blocks[i].StoredSize = blockSizes[i]
	}

	// update and store statistics
	t.stats.AddOrUpdate(pstats)
	m, err := t.stats.Store(ctx, t.statsBucket(tx))
	if err != nil {
		return n, err
	}

	atomic.AddInt64(&t.metrics.PacksStored, 1)
	atomic.AddInt64(&t.metrics.BytesWritten, int64(n))
	atomic.AddInt64(&t.metrics.MetaBytesWritten, int64(m))
	atomic.StoreInt64(&t.metrics.PacksCount, int64(t.stats.Len()))
	atomic.StoreInt64(&t.metrics.MetaSize, int64(t.stats.HeapSize()))
	atomic.StoreInt64(&t.metrics.TotalSize, int64(t.stats.TableSize()))

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

	// store the source pack (this adds or updates its stats)
	n, err := t.storePack(ctx, pkg)
	if err != nil {
		return 0, err
	}

	// set the new pack's key after storing pack 1, this avoids
	// using the same pack key when the source pack was not stored before
	pkg2.WithKey(t.stats.NextKey())

	// save the new pack
	m, err := t.storePack(ctx, pkg2)
	if err != nil {
		return 0, err
	}
	pkg2.Release()

	return n + m, nil
}
