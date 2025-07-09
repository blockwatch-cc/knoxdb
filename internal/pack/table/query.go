// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package table

import (
	"context"
	"fmt"
	"runtime/debug"
	"sync/atomic"

	"blockwatch.cc/knoxdb/internal/engine"
	"blockwatch.cc/knoxdb/internal/pack"
	"blockwatch.cc/knoxdb/internal/query"
	"blockwatch.cc/knoxdb/internal/types"
)

type QueryResultConsumer = engine.QueryResultConsumer

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
	err := engine.GetTx(ctx).RLock(ctx, t.id)
	if err != nil {
		return nil, err
	}

	// prepare result
	res := query.NewResult(
		pack.New().
			WithMaxRows(int(plan.Limit)).
			WithSchema(plan.ResultSchema).
			Alloc(),
	).
		WithLimit(plan.Limit).
		WithOffset(plan.Offset)

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
	err := engine.GetTx(ctx).RLock(ctx, t.id)
	if err != nil {
		return err
	}

	// prepare result
	res := query.NewStreamResult(fn).
		WithLimit(plan.Limit).
		WithOffset(plan.Offset)
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
	err := engine.GetTx(ctx).RLock(ctx, t.id)
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
	res := query.NewCountResult()

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

func (t *Table) doQueryAsc(ctx context.Context, plan *query.QueryPlan, res QueryResultConsumer) error {
	var (
		nRowsScanned, nRowsMatched int
		err                        error
	)

	// cleanup and log on exit
	defer func() {
		if e := recover(); e != nil {
			t.log.Error(e)
			debug.PrintStack()
			err = fmt.Errorf("query execution failed")
		}
		plan.Stats.Tick(query.SCAN_TIME_KEY)
		plan.Stats.Count(query.ROWS_SCANNED_KEY, nRowsScanned)
		plan.Stats.Count(query.ROWS_MATCHED_KEY, nRowsMatched)
		atomic.AddInt64(&t.metrics.QueriedTuples, int64(nRowsMatched))
	}()

	// init table reader to lock storage merge epoch
	r := t.NewReader()
	defer r.Close()

	// query journal before merging index result into query plan
	// (index IN condition would hide journal-only records)
	jres := t.journal.Query(plan, r.Epoch())
	defer jres.Close()
	nRowsScanned += t.journal.NumTuples()
	plan.Stats.Tick(JOURNAL_TIME_KEY)
	plan.Log.Debugf("%d journal results in %s", jres.Len(), plan.Stats.GetRuntime(JOURNAL_TIME_KEY))

	// l := operator.NewLogger(plan.Log.Logger().Writer(), 10)
	// l.Process(ctx, t.journal.Tip().Data())

	// run index query
	if err := plan.QueryIndexes(ctx); err != nil {
		return err
	}

	// early return
	if jres.IsEmpty() && plan.IsNoMatch() {
		plan.Log.Debugf("empty match")
		return nil
	}

	// PACK SCAN
	if !plan.IsNoMatch() {
		// init table reader with query filter and snapshot isolation info
		r.WithQuery(plan).WithMask(jres.TombMask(), engine.ReadModeExcludeMask)

		for {
			// check context
			if err = ctx.Err(); err != nil {
				return err
			}

			// load next pack with real matches
			var pkg *pack.Package
			pkg, err = r.Next(ctx)
			if err != nil {
				return err
			}

			// finish when no more packs are found
			if pkg == nil {
				break
			}
			nRowsScanned += pkg.Len()
			// plan.Log.Debugf("Found matching pack id %d", pkg.Key())

			nRowsMatched += pkg.NumSelected()
			if err = res.Append(ctx, pkg); err != nil {
				return err
			}
		}
	}

	// after all packs have been scanned, add remaining rows from journal, if any
	for pkg := range jres.Iterator() {
		plan.Log.Debugf("journal segment %d with %d matches", pkg.Key(), pkg.NumSelected())
		nRowsMatched += pkg.NumSelected()
		if err = res.Append(ctx, pkg); err != nil {
			return err
		}
	}
	return nil
}

func (t *Table) doQueryDesc(ctx context.Context, plan *query.QueryPlan, res QueryResultConsumer) error {
	var (
		nRowsScanned, nRowsMatched int
	)

	// cleanup and log on exit
	defer func() {
		plan.Stats.Tick(query.SCAN_TIME_KEY)
		plan.Stats.Count(query.ROWS_SCANNED_KEY, int(nRowsScanned))
		plan.Stats.Count(query.ROWS_MATCHED_KEY, int(nRowsMatched))
		atomic.AddInt64(&t.metrics.QueriedTuples, int64(nRowsMatched))
	}()

	// init table reader to lock storage merge epoch
	r := t.NewReader()
	defer r.Close()

	// query journal before merging index result into query plan
	// (index IN condition would hide journal-only records)
	jres := t.journal.Query(plan, r.Epoch())
	defer jres.Close()
	nRowsScanned += t.journal.Len()
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

	// before table scan, emit 'new' journal-only records in desc order
	for pkg := range jres.ReverseIterator() {
		nRowsMatched += pkg.NumSelected()
		if err := res.Append(ctx, pkg); err != nil {
			return err
		}
	}
	plan.Stats.Tick(JOURNAL_TIME_KEY)

	// second return point (match was journal only)
	if plan.IsNoMatch() {
		return nil
	}

	// PACK SCAN (reverse-scan)
	// init table reader with query filter and snapshot isolation info
	r.WithQuery(plan).WithMask(jres.TombMask(), engine.ReadModeExcludeMask)
	defer r.Close()

	// packloop:
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
		nRowsScanned += pkg.Len()

		// forward pack with matches
		nRowsMatched += pkg.NumSelected()
		if err := res.Append(ctx, pkg); err != nil {
			return err
		}
	}

	return nil
}
