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
	"blockwatch.cc/knoxdb/internal/operator/filter"
	"blockwatch.cc/knoxdb/internal/pack"
	"blockwatch.cc/knoxdb/internal/pack/stats"
	"blockwatch.cc/knoxdb/internal/query"
	"blockwatch.cc/knoxdb/internal/store"
	"blockwatch.cc/knoxdb/internal/xroar"
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
// are used to identify potential candidate packs. Once loaded and after query filters
// have selected matching vector positions, the exclude mask is used as additional
// criteria for de-selecting records that would otherwise match. This hides rows
// that are replaced by updates and deletions in the table journal.
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
	mask      *xroar.Bitmap             // masked row ids (sorted)
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
		reqFields: []uint16{schema.MetaRid, schema.MetaXmin, schema.MetaXmin},
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

func (r *Reader) WithMask(mask *xroar.Bitmap, mode engine.ReadMode) engine.TableReader {
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
	r.reqFields = []uint16{schema.MetaRid, schema.MetaXmin, schema.MetaXmin}
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
	if r.mask.Count() == 0 {
		return nil, nil
	}

	// find next potential pack match in statistics index
	// (may use a backend read tx to load stats)
	var ok bool
	r.it, ok = r.stats.FindRid(ctx, r.mask.Min())
	if !ok {
		r.mask.Reset()
		return nil, nil
	}

	// obtain max row id and remove mask entries within this pack
	// assumes table is sorted by rid (not applicable to history tables)
	rmin, rmax := r.it.MinMaxRid()
	r.mask.UnsetRange(rmin, rmax+1)

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

		// find actual matches (zero bits before checking a pack)
		filter.MatchTree(r.query.Filters, r.pack, r.it, r.bits.Zero())
		r.query.Stats.Count(PACKS_SCHEDULED_KEY, 1)

		// handle false positive metadata matches
		if r.bits.None() {
			r.pack.Release()
			r.pack = nil
			continue
		}

		// handle real matches
		r.query.Stats.Count(PACKS_SCANNED_KEY, 1)

		// apply exclusion mask, do not assume forward scan order,
		// we may also walk backwards!
		if r.mode == engine.ReadModeExcludeMask {
			rmin, rmax := r.it.MinMaxRid()
			if r.mask.ContainsRange(rmin, rmax) {
				rids := r.pack.RowIds()

				// TODO: use chunk iterator
				for i := range r.bits.Iterator() {
					// read next row id
					rid := rids.Get(i)

					// reset matched bit and remove rid from mask
					if r.mask.Contains(rid) {
						r.bits.Unset(i)
						r.mask.Unset(rid)
					}

					// TODO: measure if this is faster than checking all matches
					// and removing a range

					// stop early when next mask value is outside this pack
					if r.mask.Min() > rmax {
						break
					}
				}
				// r.mask.UnsetRange(rmin, rmax+1)
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
			xmins := r.pack.Xmins()
			if r.query.Snap.Xmin >= x.(uint64) && r.query.Snap.Xmin <= y.(uint64) {
				for i := range r.bits.Iterator() {
					if !r.query.Snap.IsVisible(xmins.Get(i)) {
						r.bits.Unset(i)
					}
				}
			}

			// hide deleted rows based on rec.$xmax
			x, y = r.it.MinMax(r.rx + 3)
			if r.query.Snap.Xmax >= x.(uint64) && r.query.Snap.Xmax <= y.(uint64) {
				xmaxs := r.pack.Xmaxs()
				for i := range r.bits.Iterator() {
					if !r.query.Snap.IsVisible(xmaxs.Get(i)) {
						r.bits.Unset(i)
					}
				}
			}
		}

		// check if there is a result match left
		if r.bits.None() {
			r.pack.Release()
			r.pack = nil
			continue
		}

		// load remaining columns here
		if err := r.loadPack(ctx, r.it.Key(), r.it.NValues(), r.resFields); err != nil {
			return nil, err
		}

		r.query.Log.Debugf("read: %s pack=%08x with %d/%d matches",
			r.table.schema.Name(), r.it.Key(), len(r.hits), r.it.NValues())

		// set pack selection vector
		r.pack.WithSelection(r.bits.Indexes(r.hits))

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
					Index:   rx,
					Id:      schema.MetaRid,
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
