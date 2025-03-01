// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package table

import (
	"context"
	"sync/atomic"

	"blockwatch.cc/knoxdb/internal/block"
	"blockwatch.cc/knoxdb/internal/engine"
	"blockwatch.cc/knoxdb/internal/pack"
	"blockwatch.cc/knoxdb/internal/store"
	"blockwatch.cc/knoxdb/pkg/schema"
	"blockwatch.cc/knoxdb/pkg/util"
)

func (t *Table) NewReader() engine.TableReader {
	return nil
}

func (t *Table) NewWriter() engine.TableWriter {
	return nil
}

func (t *Table) dataBucket(tx store.Tx) store.Bucket {
	key := append([]byte(t.schema.Name()), engine.DataKeySuffix...)
	b := tx.Bucket(key)
	if b != nil {
		b.FillPercent(t.opts.PageFill)
	}
	return b
}

// Loads a shared pack for reading, uses block cache to lookup blocks.
// Stores loaded blocks unless useCache is false.
func (t *Table) loadSharedPack(ctx context.Context, id uint32, nrow int, useCache bool, s *schema.Schema) (*pack.Package, error) {
	// prepare a pack without block storage
	pkg := pack.New().
		WithKey(id).
		WithSchema(t.schema).
		WithMaxRows(util.NonZero(nrow, t.opts.PackSize))

	// list of fields to load
	fids := s.ActiveFieldIds()

	// try load from cache using tableid as cache tag
	cache := block.NoCache
	if useCache {
		cache = engine.GetEngine(ctx).BlockCache(t.id)
		if pkg.LoadFromCache(cache, fids) == len(fids) {
			return pkg, nil
		}
	}

	// load from table data bucket
	err := t.db.View(func(tx store.Tx) error {
		n, err := pkg.LoadFromDisk(ctx, t.dataBucket(tx), fids, nrow)
		if err == nil {
			// count stats
			atomic.AddInt64(&t.metrics.PacksLoaded, 1)
			atomic.AddInt64(&t.metrics.BytesRead, int64(n))
		}
		return err
	})
	if err != nil {
		pkg.Release()
		return nil, err
	}

	// add loaded blocks to cache
	if useCache {
		pkg.AddToCache(cache)
	}

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
	// remove zero length packs
	if pkg.Len() == 0 {
		// drop from stats
		if err := t.stats.DeletePack(ctx, pkg); err != nil {
			return 0, err
		}

		// remove from storage
		err := t.db.Update(func(tx store.Tx) error {
			return pkg.RemoveFromDisk(ctx, t.dataBucket(tx))
		})

		// remove from cache
		pkg.DropFromCache(engine.GetEngine(ctx).BlockCache(t.id))

		return 0, err
	}

	// analyze, optimize, compress and write to disk
	var nBytes int
	err := t.db.Update(func(tx store.Tx) error {
		n, err := pkg.StoreToDisk(ctx, t.dataBucket(tx))
		if err == nil {
			nBytes = n
			atomic.AddInt64(&t.metrics.PacksStored, 1)
			atomic.AddInt64(&t.metrics.BytesWritten, int64(nBytes))
		}
		return err
	})
	if err != nil {
		return nBytes, err
	}

	// update statistics
	if pkg.Key() < t.stats.NextKey() {
		err = t.stats.UpdatePack(ctx, pkg)
	} else {
		err = t.stats.AddPack(ctx, pkg)
	}

	// remove from cache
	pkg.DropFromCache(engine.GetEngine(ctx).BlockCache(t.id))

	// cleanup temp statistics data
	pkg.FreeAnalysis()

	return nBytes, err
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
