// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package table

import (
	"context"
	"sync/atomic"

	"blockwatch.cc/knoxdb/internal/arena"
	"blockwatch.cc/knoxdb/internal/bitset"
	"blockwatch.cc/knoxdb/internal/block"
	"blockwatch.cc/knoxdb/internal/engine"
	"blockwatch.cc/knoxdb/internal/pack"
	"blockwatch.cc/knoxdb/internal/pack/match"
	"blockwatch.cc/knoxdb/internal/pack/stats"
	"blockwatch.cc/knoxdb/internal/query"
	"blockwatch.cc/knoxdb/internal/store"
	"blockwatch.cc/knoxdb/pkg/schema"
	"blockwatch.cc/knoxdb/pkg/util"
)

// TableReader
//
// A physical table scan access operator (source) that supports filter push-down
// and special mask feature for lookup/exclusion. Skips data packs based on data
// statistics like zone-maps (min/max) and optional filters (bloom, fuse, bit,
// range) and returns data packs with an associated selection vector.
//
// Supported query modes
// - query: call `WithQuery()` to install a filter condition and scan direction
// - lookup: call `WithMask()` with row-id filter to load specific packs
// - point: call `Read()` to load a data pack by key
//
//
// Mask feature
//
// Table scans using a mask run either in exclusion (together with a query) or
// inclusion mode (lookup without query).
//
// - ReadModeIncludeMask used without a query to lookup packs for rids in mask
// - ReadModeExcludeMask used with a query to exclude rids from selection vectors
//
// In exclusion mode, a query defines filter conditions which based on statistics
// are used to identify potential candidate packs. Once loaded and query filters
// select matching vector positions and the exclude mask is used as additional
// criteria for de-selecting records that would otherwise match. This mode is used
// to hide rows that are replaced by updates and deletions inside the table journal.
//
// In inclusion mode, the mask provides a list of row_ids to lookup without additional
// query filters. Recall that table packs are sorted by row id and row id pack vectors
// don't overlap. We use this invariant to efficiently jump to the next candidate pack
// through quick lookups in our statistics index (FindPk). This mode is used by merge
// operations which strip deleted rows from stored packs.

var _ engine.TableReader = (*Reader)(nil)

type Reader struct {
	table     *Table                    // table back-reference
	stats     *stats.Index              // active of statistics index
	it        *stats.Iterator           // statistics iterator
	query     *query.QueryPlan          // related query plan
	rx        int                       // row id vector position
	reqFields []uint16                  // query field ids for matching
	resFields []uint16                  // result field ids (all when nil)
	mask      []uint64                  // masked row ids (sorted)
	pack      *pack.Package             // current package
	hits      []uint32                  // selection vector
	bits      *bitset.Bitset            // selection bitset
	bcache    block.BlockCachePartition // block cache reference
	mode      engine.ReadMode           // exclude or include masked row ids
	useCache  bool                      // use cache
}

func (t *Table) NewReader() engine.TableReader {
	rx := t.schema.RowIdIndex()
	return &Reader{
		table: t,
		stats: t.stats.Load().(*stats.Index),
		rx:    rx,
		query: &query.QueryPlan{
			Filters: makeRxFilter(rx),
			Log:     t.log,
		},
		reqFields: []uint16{uint16(rx), uint16(rx + 2), uint16(rx + 3)},
		hits:      arena.AllocUint32(t.opts.PackSize),
		bits:      bitset.New(t.opts.PackSize),
		useCache:  true,
	}
}

func (r *Reader) WithQuery(p engine.QueryPlan) engine.TableReader {
	r.query = p.(*query.QueryPlan)
	r.useCache = !r.query.Flags.IsNoCache()
	r.reqFields = r.query.RequestSchema.ActiveFieldIds()
	r.resFields = r.query.ResultSchema.ActiveFieldIds()
	return r
}

func (r *Reader) WithFields(fids []uint16) engine.TableReader {
	r.resFields = fids
	return r
}

func (r *Reader) WithMask(mask []uint64, mode engine.ReadMode) engine.TableReader {
	r.mask = mask
	r.mode = mode
	return r
}

func (r *Reader) Schema() *schema.Schema {
	return r.table.schema
}

func (r *Reader) Reset() {
	if r.pack != nil {
		r.pack.Release()
		r.pack = nil
	}
	if r.it != nil {
		r.it.Close()
		r.it = nil
	}
	r.query = &query.QueryPlan{
		Filters: makeRxFilter(r.rx),
		Log:     r.table.log,
	}
	r.reqFields = []uint16{uint16(r.rx), uint16(r.rx + 2), uint16(r.rx + 3)}
	r.resFields = nil
	r.mask = nil
	r.bcache = nil
	r.mode = 0
	r.useCache = false
}

func (r *Reader) Close() {
	if r.pack != nil {
		r.pack.Release()
		r.pack = nil
	}
	if r.it != nil {
		r.it.Close()
		r.it = nil
	}
	r.table = nil
	r.stats = nil
	r.query = nil
	r.reqFields = nil
	r.resFields = nil
	r.mask = nil
	arena.Free(r.hits[:0])
	r.hits = nil
	r.bits.Close()
	r.bits = nil
	r.bcache = nil
	r.useCache = false
	r.mode = 0
}

func (r *Reader) Next(ctx context.Context) (*pack.Package, error) {
	// release last pack
	if r.pack != nil {
		r.pack.Release()
		r.pack = nil
	}

	// init cache on first call
	if r.bcache == nil {
		if r.useCache {
			r.bcache = engine.GetEngine(ctx).BlockCache(r.table.id)
		} else {
			r.bcache = block.NoCache
		}
	}

	// find and load the next pack based on operation mode
	if r.mode == engine.ReadModeIncludeMask {
		return r.nextLookupMatch(ctx)
	}
	return r.nextQueryMatch(ctx)
}

func (r *Reader) nextLookupMatch(ctx context.Context) (*pack.Package, error) {
	// no more matches
	if len(r.mask) == 0 {
		return nil, nil
	}

	// find next potential pack match in statistics index
	// (may use a backend read tx to load stats)
	var ok bool
	r.it, ok = r.stats.FindPk(ctx, r.mask[0])
	if !ok {
		r.mask = r.mask[:0]
		return nil, nil
	}

	// obtain max row id and remove mask entries within this pack
	_, rmax := r.it.MinMaxPk()
	for len(r.mask) > 0 && r.mask[0] <= rmax.(uint64) {
		r.mask = r.mask[1:]
	}

	// load pack from it
	err := r.loadPack(ctx, r.it.Key(), r.it.NValues(), r.resFields)

	// close it
	r.it.Close()
	r.it = nil

	// return pack without selection vector
	return r.pack, err
}

func (r *Reader) nextQueryMatch(ctx context.Context) (*pack.Package, error) {
	// fixed marker value to identify excluded entries in the selection vector
	const DROP_MARKER uint32 = 0xffffffff

	// Loop until a pack with query matches is found.
	//
	// - first check for potential matches against the table's statistics index
	//   (zone maps, filters)
	// - load blocks vectors required to perform a full vector match
	// - run a vector match, apply snapshot isolation and optional exclude mask
	// - continue with next potential pack when no match was found
	// - if match was found, load remaining block vectors
	// - add selection vector to pack and return
	//
	for {
		// find next potential pack match in statistics index, scans in query order
		// (may use a backend read tx to load stats)
		var ok bool
		if r.it == nil {
			// init on first use
			r.it, ok = r.stats.Query(ctx, r.query.Filters, r.query.Order)
		} else {
			ok = r.it.Next()
		}

		// no more matches, return end condition (nil pack and nil error)
		if !ok {
			return nil, nil
		}

		// load match columns only
		if err := r.loadPack(ctx, r.it.Key(), r.it.NValues(), r.reqFields); err != nil {
			return nil, err
		}

		// find actual matches (zero bits befor checking a pack)
		match.MatchTree(r.query.Filters, r.pack, r.it, r.bits.Zero())
		r.query.Stats.Count(PACKS_SCHEDULED_KEY, 1)

		// handle false positive metadata matches
		if r.bits.None() {
			r.pack.Release()
			r.pack = nil
			continue
		}

		// handle real matches
		r.hits = r.bits.Indexes(r.hits)
		r.query.Stats.Count(PACKS_SCANNED_KEY, 1)

		// constrain hits
		var needCleanup bool

		// apply exclusion mask
		if r.mode == engine.ReadModeExcludeMask {
			_, rmax := r.it.MinMaxPk()
			for i, pos := range r.hits {
				// read next row id
				rid := r.pack.Pk(int(pos))

				// drop non existent mask values (unlikely)
				for len(r.mask) > 0 && r.mask[0] < rid {
					r.mask = r.mask[1:]
				}

				// stop when mask is exhausted or next mask value is outside this pack
				if len(r.mask) == 0 || r.mask[0] > rmax.(uint64) {
					break
				}

				// on match, mark selection for removal and advance mask
				if rid == r.mask[0] {
					r.hits[i] = DROP_MARKER
					r.mask = r.mask[1:]
					needCleanup = true
				}
			}
		}

		// Apply snapshot isolation (only necessary when this pack's data
		// was written by transactions that overlap with the current snapshot.
		//
		// This may seem unlikely because new data is written to journals first
		// and only merged when all txn in a journal segment have ended.
		// However, long running readers may observe merged data from write txn
		// that started after the read txn (xid > snap.xmax) or were active
		// when the reader started (xid in snap.xact).
		//
		// Note we do not check for future writer activity (>snap.xmax) here.
		// Instead we extend the query filter during plan compile. The benefit
		// is that safe snapshots (xact = 0) need no visibility check here.
		//
		if !r.query.Snap.Safe {
			// hide future values from concurrent txn based on rec.$xmin
			x, y := r.it.MinMax(r.rx + 2)
			if r.query.Snap.Xmin >= x.(uint64) && r.query.Snap.Xmin <= y.(uint64) {
				for i, pos := range r.hits {
					if !r.query.Snap.IsVisible(r.pack.Xmin(int(pos))) {
						r.hits[i] = DROP_MARKER
						needCleanup = true
					}
				}
			}

			// hide deleted rows based on rec.$xmax
			x, y = r.it.MinMax(r.rx + 3)
			if r.query.Snap.Xmax >= x.(uint64) && r.query.Snap.Xmax <= y.(uint64) {
				for i, pos := range r.hits {
					if !r.query.Snap.IsVisible(r.pack.Xmax(int(pos))) {
						r.hits[i] = DROP_MARKER
						needCleanup = true
					}
				}
			}

		}

		// remove excluded hits
		if needCleanup {
			var k int
			for i, l := 0, len(r.hits); i < l; i++ {
				if r.hits[i] != DROP_MARKER {
					r.hits[k] = r.hits[i]
					k++
				}
			}
			r.hits = r.hits[:k]

			// skip pack when no more hits remain
			if len(r.hits) == 0 {
				r.pack.Release()
				r.pack = nil
				continue
			}
		}

		// load remaining columns here
		if err := r.loadPack(ctx, r.it.Key(), r.it.NValues(), r.resFields); err != nil {
			return nil, err
		}

		r.query.Log.Debugf("read: %s pack=%08x with %d/%d matches",
			r.table.schema.Name(), r.it.Key(), len(r.hits), r.it.NValues())

		// set pack selection vector
		r.pack.WithSelection(r.hits)

		return r.pack, nil
	}
}

func makeRxFilter(rx int) *query.FilterTreeNode {
	return &query.FilterTreeNode{
		Children: []*query.FilterTreeNode{
			{
				Filter: &query.Filter{
					Name:    "$rid",
					Type:    query.BlockUint64,
					Mode:    query.FilterModeTrue,
					Index:   uint16(rx),
					Value:   nil,
					Matcher: query.NoopMatcher,
				},
			},
		},
	}
}

func (r *Reader) Read(ctx context.Context, key uint32) (*pack.Package, error) {
	err := r.loadPack(ctx, key, 0, nil)
	return r.pack, err
}

func (r *Reader) loadPack(ctx context.Context, key uint32, nval int, fids []uint16) error {
	r.query.Log.Debugf("read: %s loading pack=%08x rows=%d", r.table.schema.Name(), key, nval)

	// prepare an empty pack without block storage
	if r.pack == nil {
		r.pack = pack.New().
			WithKey(key).
			WithSchema(r.table.schema).
			WithMaxRows(util.NonZero(nval, r.table.opts.PackSize))
	}

	// try load from cache using tableid as cache tag
	if r.useCache {
		// count number of expected blocks
		nBlocks := len(fids)
		if fids == nil {
			nBlocks = r.table.schema.NumActiveFields()
		}

		// stop early when all requested blocks are found
		if r.pack.LoadFromCache(r.bcache, fids) == nBlocks {
			return nil
		}
	}

	// load from table data bucket in short-lived read tx
	err := r.table.db.View(func(tx store.Tx) error {
		n, err := r.pack.LoadFromDisk(ctx, r.table.dataBucket(tx), fids, nval)
		if err == nil {
			// count stats
			atomic.AddInt64(&r.table.metrics.PacksLoaded, 1)
			atomic.AddInt64(&r.table.metrics.BytesRead, int64(n))
		}
		return err
	})
	if err != nil {
		r.pack.Release()
		r.pack = nil
		return err
	}

	// add loaded blocks to cache
	if r.useCache {
		r.pack.AddToCache(r.bcache)
	}
	return nil
}
