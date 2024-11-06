// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package table

import (
	"context"
	"fmt"
	"sync/atomic"

	"blockwatch.cc/knoxdb/internal/bitset"
	"blockwatch.cc/knoxdb/internal/engine"
	"blockwatch.cc/knoxdb/internal/pack"
	"blockwatch.cc/knoxdb/internal/query"
	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/bitmap"
)

func (t *Table) Query(ctx context.Context, q engine.QueryPlan) (engine.QueryResult, error) {
	plan, ok := q.(*query.QueryPlan)
	if !ok {
		return nil, fmt.Errorf("invalid query plan type %T", q)
	}

	// obtain shared table lock
	err := engine.GetTransaction(ctx).RLock(ctx, t.tableId)
	if err != nil {
		return nil, err
	}

	if err := plan.QueryIndexes(ctx); err != nil {
		return nil, err
	}

	// prepare result
	res := NewResult(
		pack.New().
			WithKey(pack.ResultKeyId).
			WithMaxRows(int(plan.Limit)).
			WithSchema(plan.ResultSchema).
			Alloc(),
	)

	// protect journal access
	t.mu.RLock()
	defer t.mu.RUnlock()
	atomic.AddInt64(&t.metrics.QueryCalls, 1)

	// execute query
	switch plan.Order {
	case types.OrderDesc, types.OrderDescCaseInsensitive:
		err = t.doQueryDesc(ctx, plan, res)
	default:
		err = t.doQueryAsc(ctx, plan, res)
	}
	if err != nil {
		res.Close()
		return nil, err
	}

	return res, nil
}

func (t *Table) Stream(ctx context.Context, q engine.QueryPlan, fn func(engine.QueryRow) error) error {
	plan, ok := q.(*query.QueryPlan)
	if !ok {
		return fmt.Errorf("invalid query plan type %T", q)
	}

	// obtain shared table lock
	err := engine.GetTransaction(ctx).RLock(ctx, t.tableId)
	if err != nil {
		return err
	}

	if err := plan.QueryIndexes(ctx); err != nil {
		return err
	}

	// prepare result
	res := NewStreamResult(fn)
	defer res.Close()

	// protect journal access
	t.mu.RLock()
	defer t.mu.RUnlock()
	atomic.AddInt64(&t.metrics.StreamCalls, 1)

	// execute query
	switch plan.Order {
	case types.OrderDesc, types.OrderDescCaseInsensitive:
		err = t.doQueryDesc(ctx, plan, res)
	default:
		err = t.doQueryAsc(ctx, plan, res)
	}
	if err != nil && err != engine.EndStream {
		return err
	}

	return nil
}

func (t *Table) Count(ctx context.Context, q engine.QueryPlan) (uint64, error) {
	// unpack query plan
	plan, ok := q.(*query.QueryPlan)
	if !ok {
		return 0, fmt.Errorf("invalid query plan type %T", q)
	}

	// obtain shared table lock
	err := engine.GetTransaction(ctx).RLock(ctx, t.tableId)
	if err != nil {
		return 0, err
	}

	if err := plan.QueryIndexes(ctx); err != nil {
		return 0, err
	}

	// amend query plan to only output pk field
	rs, err := t.schema.SelectFieldIds(t.schema.PkId())
	if err != nil {
		return 0, err
	}
	plan.ResultSchema = rs.WithName("count")

	// use count result
	res := NewCountResult()

	// protect journal read access
	t.mu.RLock()
	defer t.mu.RUnlock()
	atomic.AddInt64(&t.metrics.StreamCalls, 1)

	// run the query
	err = t.doQueryAsc(ctx, plan, res)
	if err != nil {
		return 0, err
	}

	return res.Count(), nil
}

func (t *Table) Delete(ctx context.Context, q engine.QueryPlan) (uint64, error) {
	// unpack query plan
	plan, ok := q.(*query.QueryPlan)
	if !ok {
		return 0, fmt.Errorf("invalid query plan type %T", q)
	}

	// obtain shared table lock
	err := engine.GetTransaction(ctx).RLock(ctx, t.tableId)
	if err != nil {
		return 0, err
	}

	if err := plan.QueryIndexes(ctx); err != nil {
		return 0, err
	}

	// amend query plan to only output pk field
	rs, err := t.schema.SelectFieldIds(t.schema.PkId())
	if err != nil {
		return 0, err
	}
	plan.ResultSchema = rs.WithName("delete")

	// start a storage write transaction, prevents a conflicting
	// read-only tx from getting opened in doQuery (tx is reused there)
	_, err = engine.GetTransaction(ctx).StoreTx(t.db, true)
	if err != nil {
		return 0, err
	}
	atomic.AddInt64(&t.metrics.DeleteCalls, 1)

	// execute the query to find all matching pks
	bits := bitmap.New()
	res := NewStreamResult(func(row engine.QueryRow) error {
		bits.Set(row.(*Row).Uint64(t.px))
		return nil
	})

	// protect journal write access (query is read only, but later update will write)
	t.mu.Lock()
	defer t.mu.Unlock()

	// upgrade tx for writing and register touched table for later commit
	engine.GetTransaction(ctx).Touch(t.tableId)

	// run the query
	err = t.doQueryAsc(ctx, plan, res)
	if err != nil {
		return 0, err
	}

	// mark as deleted
	n := t.journal.DeleteBatch(bits)

	// write journal data to disk before we continue
	if t.journal.IsFull() {
		// check context cancelation
		if err := ctx.Err(); err != nil {
			return n, err
		}

		// flush pack data to storage, will open storage write transaction
		// TODO: write a new layer pack (fast) and merge in background
		if err := t.mergeJournal(ctx); err != nil {
			return n, err
		}
	}

	// TODO: we may accept non existing pks for deletion, should we update
	// table active row count here or wait until next flush (may be wrong
	// anyways until flush)
	if n > 0 {
		atomic.AddInt64(&t.metrics.DeletedTuples, int64(n))
	}

	return n, nil
}

func (t *Table) doQueryAsc(ctx context.Context, plan *query.QueryPlan, res QueryResultConsumer) error {
	var (
		nRowsScanned, nRowsMatched uint32
		jbits                      *bitset.Bitset
	)

	// cleanup and log on exit
	defer func() {
		plan.Stats.Tick(query.SCAN_TIME_KEY)
		plan.Stats.Count(query.ROWS_SCANNED_KEY, int(nRowsScanned))
		plan.Stats.Count(query.ROWS_MATCHED_KEY, int(nRowsMatched))
		atomic.AddInt64(&t.metrics.QueriedTuples, int64(nRowsMatched))
		if jbits != nil {
			jbits.Close()
		}
	}()

	// FIXME: check if index result & journal query conflict (under new node.Bits)
	//
	// run journal query before index query to avoid side-effects of
	// added pk lookup condition (otherwise only indexed pks are found,
	// but not new pks that are only in journal)
	jbits = MatchTree(plan.Filters, t.journal.Data, nil)
	nRowsScanned += uint32(t.journal.Len())
	plan.Stats.Tick(JOURNAL_TIME_KEY)
	// plan.Log.Debugf("Table %s: %d journal results", t.name(), jbits.Count())

	// early return
	if jbits.Count() == 0 && plan.IsEmptyMatch() {
		return nil
	}

	// PACK SCAN
	// - based on filter tree which contains at this point
	//   - index matches (pks in bitsets attached to nodes)
	//   - non-indexed regular filter conditions
	// - scan iff
	//   (a) index match is non-empty or
	//   (b) no index exists
	if !plan.IsEmptyMatch() {
		// pack iterator manages selection, load and scan of packs
		it := NewForwardIterator(plan)
		defer it.Close()

	packloop:
		for {
			// check context
			if err := ctx.Err(); err != nil {
				return err
			}

			// load next pack with real matches
			pkg, hits, err := it.Next(ctx)
			if err != nil {
				return err
			}

			// finish when no more packs are found
			if pkg == nil {
				break
			}
			nRowsScanned += uint32(pkg.Len())

			for _, idx := range hits {
				index := int(idx)
				src := pkg

				// skip broken records (invalid pk)
				pk := pkg.Uint64(t.px, index)
				if pk == 0 {
					continue
				}

				// skip deleted records
				if t.journal.IsDeleted(pk) {
					continue
				}

				// use journal record if exists
				if j, _ := t.journal.PkIndex(pk, 0); j >= 0 {
					// cross-check record actually matches
					if !jbits.IsSet(j) {
						continue
					}

					// remove match bit (so we don't output this record twice)
					jbits.Clear(j)
					src = t.journal.Data
					index = j
				}

				// skip offset
				if plan.Offset > 0 {
					plan.Offset--
					continue
				}

				// emit record
				nRowsMatched++
				if err := res.Append(src, index, 1); err != nil {
					return err
				}

				// apply limit
				if plan.Limit > 0 && nRowsMatched >= plan.Limit {
					break packloop
				}
			}
		}
	}

	// finalize on limit
	if plan.Limit > 0 && nRowsMatched >= plan.Limit {
		return nil
	}

	// after all packs have been scanned, add remaining rows from journal, if any
	idxs, _ := t.journal.SortedIndexes(jbits)
	jpack := t.journal.Data
	// log.Debugf("Table %s: %d remaining journal rows", t.name, len(idxs))
	for _, idx := range idxs {
		// skip offset
		if plan.Offset > 0 {
			plan.Offset--
			continue
		}

		// emit record
		nRowsMatched++
		if err := res.Append(jpack, idx, 1); err != nil {
			return err
		}

		// apply limit
		if plan.Limit > 0 && nRowsMatched == plan.Limit {
			break
		}
	}

	return nil
}

func (t *Table) doQueryDesc(ctx context.Context, plan *query.QueryPlan, res QueryResultConsumer) error {
	var (
		nRowsScanned, nRowsMatched uint32
		jbits                      *bitset.Bitset
	)

	// cleanup and log on exit
	defer func() {
		plan.Stats.Tick(query.SCAN_TIME_KEY)
		plan.Stats.Count(query.ROWS_SCANNED_KEY, int(nRowsScanned))
		plan.Stats.Count(query.ROWS_MATCHED_KEY, int(nRowsMatched))
		atomic.AddInt64(&t.metrics.QueriedTuples, int64(nRowsMatched))
		if jbits != nil {
			jbits.Close()
		}
	}()

	// run journal query before index query to avoid side-effects of
	// added pk lookup condition (otherwise only indexed pks are found,
	// but not new pks that are only in journal)
	// reverse the bitfield order for descending walk
	jbits = MatchTree(plan.Filters, t.journal.Data, nil)
	nRowsScanned += uint32(t.journal.Len())

	// early return
	if jbits.Count() == 0 && plan.IsEmptyMatch() {
		return nil
	}

	// find max pk across all saved packs (we assume any journal entry greater than this max
	// is new and hasn't been saved before; this assumption breaks when user-defined pk
	// values are smaller, so a user must flush the journal before query)
	_, maxStoredPk := t.stats.GlobalMinMax()

	// before table scan, emit 'new' journal-only records (i.e. pk > max) in desc order
	// Note: deleted journal records are not present in this list
	idxs, pks := t.journal.SortedIndexesReversed(jbits)
	jpack := t.journal.Data
	for i, idx := range idxs {
		// skip already stored records (we'll get back to them later)
		if pks[i] <= maxStoredPk {
			continue
		}

		// skip offset
		if plan.Offset > 0 {
			plan.Offset--
			continue
		}

		// emit record
		nRowsMatched++
		if err := res.Append(jpack, idx, 1); err != nil {
			return err
		}

		// remove match bit (so we don't output this record twice)
		jbits.Clear(idx)

		// apply limit
		if plan.Limit > 0 && nRowsMatched == plan.Limit {
			break
		}
	}
	plan.Stats.Tick(JOURNAL_TIME_KEY)

	// finalize on limit
	if plan.Limit > 0 && nRowsMatched >= plan.Limit {
		return nil
	}

	// second return point (match was journal only)
	if plan.IsEmptyMatch() {
		return nil
	}

	// PACK SCAN (reverse-scan)
	// - based on filter tree which contains at this point
	//   - index matches (pks in bitsets attached to nodes)
	//   - non-indexed regular filter conditions
	// - scan iff
	// (a) index match returned any results or
	// (b) no index exists

	// pack iterator manages selection, load and scan of packs
	it := NewReverseIterator(plan)
	defer it.Close()

packloop:
	for {
		// check context
		if err := ctx.Err(); err != nil {
			return err
		}

		// load next pack with real matches
		pkg, hits, err := it.Next(ctx)
		if err != nil {
			return err
		}
		nRowsScanned += uint32(pkg.Len())

		// finish when no more packs are found
		if pkg == nil {
			break
		}

		// walk hits in reverse pk order
		for k := len(hits) - 1; k >= 0; k-- {
			index := int(hits[k])
			src := pkg

			// skip broken records (invalid pk)
			pk := pkg.Uint64(t.px, index)
			if pk == 0 {
				continue
			}

			// skip deleted records
			if t.journal.IsDeleted(pk) {
				continue
			}

			// use journal record if exists
			if j, _ := t.journal.PkIndex(pk, 0); j >= 0 {
				// cross-check if record actually matches
				if !jbits.IsSet(j) {
					continue
				}

				// remove match bit (so we don't output this record twice)
				jbits.Clear(j)
				src = t.journal.Data
				index = j
			}

			// skip offset
			if plan.Offset > 0 {
				plan.Offset--
				continue
			}

			// emit record
			nRowsMatched++
			if err := res.Append(src, index, 1); err != nil {
				return err
			}

			// apply limit
			if plan.Limit > 0 && nRowsMatched == plan.Limit {
				break packloop
			}
		}
	}

	return nil
}
