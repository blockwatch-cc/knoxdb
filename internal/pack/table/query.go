// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package table

import (
	"context"
	"fmt"
	"sync/atomic"

	"blockwatch.cc/knoxdb/internal/engine"
	"blockwatch.cc/knoxdb/internal/pack"
	"blockwatch.cc/knoxdb/internal/pack/stats"
	"blockwatch.cc/knoxdb/internal/query"
	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/internal/wal"
	"blockwatch.cc/knoxdb/pkg/num"
	"blockwatch.cc/knoxdb/pkg/schema"
)

var (
	// statistics keys
	PACKS_SCANNED_KEY   = "packs_scanned"
	PACKS_SCHEDULED_KEY = "packs_scheduled"
	JOURNAL_TIME_KEY    = "journal_time"
)

func (t *Table) Query(ctx context.Context, q engine.QueryPlan) (engine.QueryResult, error) {
	plan, ok := q.(*query.QueryPlan)
	if !ok {
		return nil, fmt.Errorf("invalid query plan type %T", q)
	}

	// obtain shared table lock
	err := engine.GetTransaction(ctx).RLock(ctx, t.id)
	if err != nil {
		return nil, err
	}

	// prepare result
	res := NewResult(
		pack.New().
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
	err := engine.GetTransaction(ctx).RLock(ctx, t.id)
	if err != nil {
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
	if err != nil && err != types.EndStream {
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
	err := engine.GetTransaction(ctx).RLock(ctx, t.id)
	if err != nil {
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

	// check table state
	if t.opts.ReadOnly {
		return 0, engine.ErrTableReadOnly
	}

	// obtain shared table lock
	tx := engine.GetTransaction(ctx)
	err := tx.RLock(ctx, t.id)
	if err != nil {
		return 0, err
	}

	// amend query plan to only output pk and rid fields
	rs, err := t.schema.SelectFieldIds(t.schema.PkId(), schema.MetaRid)
	if err != nil {
		return 0, err
	}
	plan.ResultSchema = rs.WithName("delete")

	// register table for commit/abort callbacks
	tx.Touch(t.id)

	// execute the query to find all matching pk/rid rows (db rows and journal rows)
	var (
		px   = t.schema.PkIndex()
		rx   = t.schema.RowIdIndex()
		jcap = t.journal.Capacity()
		msg  = make([]byte, 0, t.journal.MaxSize()*num.MaxVarintLen64)
		n    uint64
	)
	res := NewStreamResult(func(row engine.QueryRow) error {
		// collect pk/rid until journal segment is full
		msg = num.AppendUvarint(msg, row.(*Row).Uint64(rx))
		msg = num.AppendUvarint(msg, row.(*Row).Uint64(px))
		jcap--
		n++

		// write wal and journal
		if jcap == 0 {
			if tx.UseWal() {
				// write wal batch
				_, err := t.engine.Wal().Write(&wal.Record{
					Type:   wal.RecordTypeDelete,
					Tag:    types.ObjectTagTable,
					Entity: t.id,
					TxID:   tx.Id(),
					Data:   [][]byte{msg},
				})
				if err != nil {
					return err
				}
			}

			// protect journal write access
			t.mu.Lock()
			defer t.mu.Unlock()
			buf := msg
			for len(buf) > 0 {
				rid, n := num.Uvarint(buf)
				buf = buf[n:]
				pk, n := num.Uvarint(buf)
				buf = buf[n:]
				t.journal.Delete(tx.Id(), rid, pk)
				t.state.NRows--
			}

			// once a journal segment is full
			// - rotate segment, flush to disk
			// - flush other dirty segment's xmeta to disk
			// - write checkpoint record
			if t.journal.Active().IsFull() {
				err = t.flushJournal(ctx)
				if err != nil {
					return err
				}
			}

			// reset and continue
			msg = msg[:0]
			jcap = t.journal.Capacity()
		}
		return nil
	})

	// run the query
	err = t.doQueryAsc(ctx, plan, res)
	if err != nil {
		return n, err
	}

	// delete tail (matched list may not be empty here)
	if len(msg) > 0 {
		// protect journal write access
		t.mu.Lock()
		defer t.mu.Unlock()
		buf := msg
		for len(buf) > 0 {
			rid, n := num.Uvarint(buf)
			buf = buf[n:]
			pk, n := num.Uvarint(buf)
			buf = buf[n:]
			t.journal.Delete(tx.Id(), rid, pk)
			t.state.NRows--
		}

		// once a journal segment is full
		// - rotate segment, flush to disk
		// - flush other dirty segment's xmeta to disk
		// - write checkpoint record
		if t.journal.Active().IsFull() {
			err = t.flushJournal(ctx)
			if err != nil {
				return n, err
			}
		}
	}
	msg = nil

	atomic.AddInt64(&t.metrics.DeletedTuples, int64(n))
	atomic.AddInt64(&t.metrics.DeleteCalls, 1)

	return n, nil
}

func (t *Table) doQueryAsc(ctx context.Context, plan *query.QueryPlan, res QueryResultConsumer) error {
	var (
		nRowsScanned, nRowsMatched uint32
	)

	// cleanup and log on exit
	defer func() {
		plan.Stats.Tick(query.SCAN_TIME_KEY)
		plan.Stats.Count(query.ROWS_SCANNED_KEY, int(nRowsScanned))
		plan.Stats.Count(query.ROWS_MATCHED_KEY, int(nRowsMatched))
		atomic.AddInt64(&t.metrics.QueriedTuples, int64(nRowsMatched))
	}()

	// query journal first to apply mvcc isolation and avoid side-effects
	// of index pk condition(s), otherwise recent records that are only in
	// journal are missing
	jres := t.journal.Query(plan.Filters, plan.Snap)
	defer jres.Close()
	nRowsScanned += uint32(t.journal.Len())
	plan.Stats.Tick(JOURNAL_TIME_KEY)
	plan.Log.Debugf("%d journal results in %s", jres.Len(), plan.Stats.GetRuntime(JOURNAL_TIME_KEY))

	// run index query
	if err := plan.QueryIndexes(ctx); err != nil {
		return err
	}

	// early return
	if jres.IsEmpty() && plan.IsNoMatch() {
		return nil
	}

	// PACK SCAN
	if !plan.IsNoMatch() {
		// pack iterator manages selection, load and scan of packs including snapshot isolation
		// (note: long-running read queries may see data from future tx during table scans
		// when completed journal segments have been merged concurrently)
		r := t.NewReader().WithQuery(plan)
		defer r.Close()

	packloop:
		for {
			// check context
			if err := ctx.Err(); err != nil {
				return err
			}

			// load next pack with real matches
			pkg, err := r.Next(ctx)
			if err != nil {
				return err
			}

			// finish when no more packs are found
			if pkg == nil {
				break
			}
			nRowsScanned += uint32(pkg.Len())

			for _, idx := range pkg.Selected() {
				index := int(idx)
				src := pkg

				// skip broken records (invalid pk)
				pk := pkg.Pk(index)
				if pk == 0 {
					continue
				}

				// skip deleted records
				if jres.IsDeleted(pk) {
					continue
				}

				// use journal record if exists
				if idx, ok := jres.FindPk(pk); ok {
					// remove match bit (so we don't output this record twice)
					jres.UnsetMatch(pk)

					// use journal segment pack and offset to access result
					src, index = jres.GetRef(idx)
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
	err := jres.ForEach(func(pkg *pack.Package, idx int) error {
		// skip offset
		if plan.Offset > 0 {
			plan.Offset--
			return nil
		}

		// emit record
		nRowsMatched++
		if err := res.Append(pkg, idx, 1); err != nil {
			return err
		}

		// apply limit
		if plan.Limit > 0 && nRowsMatched == plan.Limit {
			return types.EndStream
		}
		return nil
	})
	if err != nil {
		return err
	}

	return nil
}

func (t *Table) doQueryDesc(ctx context.Context, plan *query.QueryPlan, res QueryResultConsumer) error {
	var (
		nRowsScanned, nRowsMatched uint32
		snap                       = engine.GetTransaction(ctx).Snapshot()
	)

	// cleanup and log on exit
	defer func() {
		plan.Stats.Tick(query.SCAN_TIME_KEY)
		plan.Stats.Count(query.ROWS_SCANNED_KEY, int(nRowsScanned))
		plan.Stats.Count(query.ROWS_MATCHED_KEY, int(nRowsMatched))
		atomic.AddInt64(&t.metrics.QueriedTuples, int64(nRowsMatched))
	}()

	// query journal first to apply mvcc isolation and avoid side-effects
	// of index pk condition(s), otherwise recent records that are only in
	// journal are missing
	jres := t.journal.Query(plan.Filters, snap)
	defer jres.Close()
	nRowsScanned += uint32(t.journal.Len())
	plan.Stats.Tick(JOURNAL_TIME_KEY)
	plan.Log.Debugf("%d journal results in %s", jres.Len(), plan.Stats.GetRuntime(JOURNAL_TIME_KEY))

	// run index query
	if err := plan.QueryIndexes(ctx); err != nil {
		return err
	}

	// early return
	if jres.IsEmpty() && plan.IsNoMatch() {
		return nil
	}

	// find max pk across all saved packs (we assume any journal entry greater than this max
	// is new and hasn't been saved before; this assumption holds true because we disallow
	// user-defined pk values
	maxStoredPk := t.stats.Load().(*stats.Index).GlobalMaxPk()

	// before table scan, emit 'new' journal-only records (i.e. pk > max) in desc order
	// Note: deleted journal records are not present in this list
	err := jres.ForEachReverse(func(pkg *pack.Package, idx int) error {
		// stop on first pk that is merged into table data
		pk := pkg.Pk(idx)
		if pk <= maxStoredPk {
			return types.EndStream
		}

		// skip offset
		if plan.Offset > 0 {
			plan.Offset--
			return nil
		}

		// emit record
		nRowsMatched++
		if err := res.Append(pkg, idx, 1); err != nil {
			return err
		}

		// remove match bit (so we don't output this record twice)
		jres.UnsetMatch(pk)

		// apply limit
		if plan.Limit > 0 && nRowsMatched == plan.Limit {
			return types.EndStream
		}
		return nil
	})
	if err != nil {
		return err
	}
	plan.Stats.Tick(JOURNAL_TIME_KEY)

	// finalize on limit
	if plan.Limit > 0 && nRowsMatched >= plan.Limit {
		return nil
	}

	// second return point (match was journal only)
	if plan.IsNoMatch() {
		return nil
	}

	// PACK SCAN (reverse-scan)
	// pack iterator manages selection, load and scan of packs including snapshot isolation
	// (note: long-running read queries may see data from future tx during table scans
	// when completed journal segments have been merged concurrently)
	r := t.NewReader().WithQuery(plan)
	defer r.Close()

packloop:
	for {
		// check context
		if err := ctx.Err(); err != nil {
			return err
		}

		// load next pack with real matches
		pkg, err := r.Next(ctx)
		if err != nil {
			return err
		}

		// finish when no more packs are found
		if pkg == nil {
			break
		}
		nRowsScanned += uint32(pkg.Len())

		// walk hits in reverse scan order
		hits := pkg.Selected()
		for k := len(hits) - 1; k >= 0; k-- {
			index := int(hits[k])
			src := pkg

			// skip broken records (invalid pk)
			pk := pkg.Pk(index)
			if pk == 0 {
				continue
			}

			// skip deleted records
			if jres.IsDeleted(pk) {
				continue
			}

			// use journal record if exists
			if idx, ok := jres.FindPk(pk); ok {
				// remove match bit (so we don't output this record twice)
				jres.UnsetMatch(pk)

				// use journal segment pack and offset to access result
				src, index = jres.GetRef(idx)
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
