// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package stats

import (
	"fmt"
	"time"

	"blockwatch.cc/knoxdb/internal/pack"
	"blockwatch.cc/knoxdb/internal/store"
	"blockwatch.cc/knoxdb/pkg/num"
)

// Versioned Copy-on-Write Storage and Garbage-Collection
//
// Journal-to-table merge runs concurrent with queries so we need a way to protect
// existing versions of on-disk table data and metadata from getting overwritten
// or deleted as long as readers still need them. We implement an epoch based
// design that ticks an epoch counter with each merged journal segment, ie. a
// journal segment represents one storage merge epoch. Note that epochs are
// increasing, but not necessarily strictly. Some journal segments may be
// skipped if they only contain aborted transactions or after a snapshot rollback
// has invalidated a set of segments.
//
// Existing on-disk data is read-only. TableReaders obtain a reference counted
// atomic pointer to the metadata index on creation (at one or multiple points
// during query execution) which locks the current epoch until the reference
// is dropped. This guarantees readers see a consistent view of all on-disk
// data and metadata.
//
// A TableWriter performing writes during the merge process stores new
// versions of objects on-disk. The writer maintains a private copy of the
// metadata index in memory and flushes changes on completion. Metadata is
// updated copy-on-write, i.e. only changed parts of the index are written,
// other parts are shared across epochs. All storage keys contain a unique
// version identifier as last part of their keys. This allows to store multiple
// versions of the same object in a bucket. Keys of replaced or deleted
// on-disk items (data blocks, metadata tree nodes) are stored into
// a tombstone list for later garbage collection.
//
// Once the writer finishes, it atomically replaces the old metadata index
// with the new index for the next epoch. When the last reader (or writer)
// finishes using an old index version it is freed from main memory. However,
// previous generations of on-disk data may still be required as some even
// older index epoch may still be alive.
//
//  epochs   -- e1 --> --- e2 --> --- e3 --> --- e4 -->
//  values     x,y        x',y      x',y'       x',y',z
//  tomb       --          x          y           --
//
//  R1       |---------------------------------->          sees [x,y]
//  R2                   |-->                              sees [x',y]
//  R3                             |-->                    sees [x',y']
//
//  Assume e1 writes [x,y], e2 replaces x with x' and e3 replaces y with y'.
//  Reader R2 and R3 are short lived, they start and finish within the lifespan
//  of epochs e2 and e3 and hence see the data as of these epochs. However
//  R1 is a long running transaction that starts at e1 and finishes during e4.
//  Even though e2 and e3 contain immediately reclaimable tombstones (no reader
//  holds a reference at the time the writer finishes merging) we cannot safely
//  reclaim x nor y because R1 still references them until later.
//
// The garbage collector uses a watermark to identify which epochs are
// reclaimable. The watermark is defined as the lowest epoch that is still
// in use. Tombstones for any earlier epoch are reclaimable. The tombstone
// list produced by epoch Y is collected and stored under the id of the direct
// predecessor epoch X. That way anything before the watermark is immediatly
// reclaimable instead of waiting one more epoch tick.
//
// Data layout on disk
//
// All numeric keys and bucket names are uvarint encoded (see num.Uvarint
// package) which is a big-endian sortable varint encoding. For data and
// meta packs we only store their key and version. Block ids are identified
// on init based on table/meta schemas.
//
// - versions
//   - bucket `{table}_epoch/{epoch}` (epoch is uvarint)
//   - nil value
// - tombstones
//   - `{table}_tomb/{epoch}/{kind}/{key-parts}`
//   - one bucket per epoch with buckets for each kind
//   - nil values
//   - nested keys prefixed with a 1 byte `kind` followed by encoded key parts
// - node tombstones [tombstone-kind:node-kind:id:key:version] can use as is
// - data pack tombstones [tombstone-kind:key:version] - rewrite, add block id
// - spack tombstones [tombstone-kind:key:version] - rewrite, add block id

// TODO
// - run GC at start of merge
// - run WAL gc in cron job (watermark based on catalog states, remove wal files)
// - test GC
// - table indexes
//   - add through index journal directly on merge
//   - insert tombstones on merge (write compressed index tombstone pack vectors)
//   - delay removal until epoch GC
// TODO: howto add index packs?
// - tighten table locks (maybe move into journal functions, don't hold lock too long)
//

// adds the current epoch to the list of live epochs on storage
func (idx *Index) addEpoch(tx store.Tx) error {
	return idx.epochBucket(tx).Put(num.EncodeUvarint(uint64(idx.epoch)), nil)
}

// removes the current epoch from the list of live epochs, which makes
// the on-disk tombstones from this epoch garbage collectable.
func (idx *Index) dropEpoch(tx store.Tx) error {
	return idx.epochBucket(tx).Delete(num.EncodeUvarint(uint64(idx.epoch)))
}

// Delete all reclaimable tombstones.
func (idx *Index) RunGC(tx store.Tx) error {
	// read watermark
	watermark := idx.getWatermark(tx)

	// identify epochs to drop
	drop := make([]uint32, 0)
	c := idx.tombBucket(tx).Cursor()
	for ok := c.First(); ok; ok = c.Next() {
		v, _ := num.Uvarint(c.Key())
		if uint32(v) >= watermark {
			break
		}
		drop = append(drop, uint32(v))
	}
	c.Close()

	// gc epochs
	for _, v := range drop {
		if err := idx.gcEpoch(tx, v); err != nil {
			idx.log.Errorf("gc: epoch %d: %v", v, err)
			return err
		}
	}

	return nil
}

// Drops all live epochs except the current. GCs tombstones of future
// epochs. Intended for startup/recovery.
func (idx *Index) CleanupEpochs(tx store.Tx) error {
	b := idx.epochBucket(tx)
	drop := make([]uint32, 0)

	// step 1: collect keys (its not safe to mutate inside a cursor)
	c := b.Cursor()
	for ok := c.First(); ok; ok = c.Next() {
		v, _ := num.Uvarint(c.Key())
		if uint32(v) == idx.epoch {
			continue
		}
		drop = append(drop, uint32(v))
	}
	c.Close()

	idx.log.Debugf("table[%s]: cleanup %d epochs", idx.schema.Name(), len(drop))

	// step 2: drop epoch keys
	for _, v := range drop {
		if err := b.Delete(num.EncodeUvarint(uint64(v))); err != nil {
			idx.log.Error(err)
		}

		// GC future tombstones (cleanup after crash)
		if v >= idx.epoch {
			idx.log.Debugf("table[%s]: gc broken future epoch %d", idx.schema.Name(), v)
			if err := idx.gcEpoch(tx, v); err != nil {
				idx.log.Error(err)
			}
		}
	}

	// set clean (gc is optional)
	idx.clean = true

	// step 3: run regular GC for old epochs
	return idx.RunGC(tx)
}

// Checks if cleanup is required, ie. future epochs exist or GC should run.
// Can run in read-only tx.
func (idx *Index) NeedCleanup(tx store.Tx) bool {
	b := idx.epochBucket(tx)
	c := b.Cursor()
	defer c.Close()
	for ok := c.First(); ok; ok = c.Next() {
		v, _ := num.Uvarint(c.Key())
		if uint32(v) != idx.epoch {
			return true
		}
	}
	return false
}

// getWatermark returns the minimum epoch that is still in use. If no on-disk
// epoch is found or the on-disk epoch is higher than the current epoch
// (i.e. from a previous crash or bug) this returns the current index epoch.
func (idx *Index) getWatermark(tx store.Tx) (ver uint32) {
	ver = idx.epoch
	c := idx.epochBucket(tx).Cursor()
	if c.First() {
		v, _ := num.Uvarint(c.Key())
		ver = min(ver, uint32(v))
	}
	c.Close()
	return
}

// numEpochs counts how many versions are active.
func (idx *Index) numEpochs(tx store.Tx) int {
	return idx.epochBucket(tx).Stats().KeyN
}

func (idx *Index) gcEpoch(tx store.Tx, epoch uint32) error {
	// resolve the epoch bucket
	ekey := num.EncodeUvarint(uint64(epoch))
	ebucket := idx.tombBucket(tx).Bucket(ekey)
	if ebucket == nil {
		return store.ErrNoBucket
	}
	idx.log.Debugf("GC epoch %d", epoch)

	var (
		start        = time.Now()
		nTableBlocks int
		nFilters     int
		nStatsBlocks int
		nTreeNodes   int
	)

	// process table data packs
	if b := ebucket.Bucket([]byte{TOMB_KIND_TABLE_PACK}); b != nil {
		dbucket := idx.tableBucket(tx)
		fbucket := idx.filterBucket(tx)
		rbucket := idx.rangeBucket(tx)
		c := b.Cursor()
		defer c.Close()
		for ok := c.First(); ok; ok = c.Next() {
			// key is pack-id + version
			key := c.Key()
			pk, n := num.Uvarint(key)
			pv, _ := num.Uvarint(key[n:])

			// drop blocks
			for _, id := range idx.tomb.activeFields {
				err := dbucket.Delete(pack.EncodeBlockKey(uint32(pk), uint32(pv), id))
				if err != nil {
					return fmt.Errorf("delete pack key 0x%08x:%02d[v%d]: %v", pk, id, pv, err)
				}
				nTableBlocks++
			}

			// drop filters
			for _, id := range idx.tomb.filteredFields {
				err := fbucket.Delete(encodeFilterKey(uint32(pk), uint32(pv), id))
				if err != nil {
					return fmt.Errorf("delete filter key 0x%08x:%02d[v%d]: %v", pk, id, pv, err)
				}
				nFilters++
			}

			// drop range filters
			for _, id := range idx.tomb.rangeFields {
				err := rbucket.Delete(encodeFilterKey(uint32(pk), uint32(pv), id))
				if err != nil {
					return fmt.Errorf("delete range key 0x%08x:%02d[v%d]: %v", pk, id, pv, err)
				}
				nFilters++
			}
		}
	}

	// process spacks
	if b := ebucket.Bucket([]byte{TOMB_KIND_STATS_PACK}); b != nil {
		sbucket := idx.statsBucket(tx)
		c := b.Cursor()
		defer c.Close()
		for ok := c.First(); ok; ok = c.Next() {
			// key is pack-id + version
			key := c.Key()
			pk, n := num.Uvarint(key)
			pv, _ := num.Uvarint(key[n:])

			// drop blocks (id is u16(pos + 1))
			for id := range idx.tomb.nSpackFields {
				err := sbucket.Delete(pack.EncodeBlockKey(uint32(pk), uint32(pv), uint16(id+1)))
				if err != nil {
					return fmt.Errorf("delete spack key 0x%08x:%02d[v%d]: %v", pk, id, pv, err)
				}
				nStatsBlocks++
			}
		}
	}

	// process tree nodes
	if b := ebucket.Bucket([]byte{TOMB_KIND_STATS_NODE}); b != nil {
		tbucket := idx.treeBucket(tx)
		c := b.Cursor()
		defer c.Close()
		for ok := c.First(); ok; ok = c.Next() {
			// use key as is
			err := tbucket.Delete(c.Key())
			if err != nil {
				return fmt.Errorf("delete tree node %x: %v", c.Key(), err)
			}
			nTreeNodes++
		}
	}

	idx.log.Debugf("GC[%s] epoch %d: reclaimed table=%d filter=%d stats=%d tree=%d in %s",
		idx.schema.Name(), epoch, nTableBlocks, nFilters, nStatsBlocks, nTreeNodes,
		time.Since(start),
	)

	// after successful GC, drop this epoch's bucket from the tomb
	return idx.tombBucket(tx).DeleteBucket(ekey)
}
