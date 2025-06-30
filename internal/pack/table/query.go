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
	err := engine.GetTransaction(ctx).RLock(ctx, t.id)
	if err != nil {
		return nil, err
	}

	// prepare result
	res := query.NewResult(
		pack.New().
			WithMaxRows(int(plan.Limit)).
			WithSchema(plan.ResultSchema).
			Alloc(),
	)

	// TODO: manage offset/limit in result

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
	// TODO: manage offset/limit in result
	res := query.NewStreamResult(fn)
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
	// TODO: manage offset/limit in result
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

	// l := operator.NewLogOperator(plan.Log.Logger().Writer(), 10)
	// l.Process(ctx, t.journal.Tip().Data())

	// run index query
	if err := plan.QueryIndexes(ctx); err != nil {
		return err
	}

	// early return
	if jres.IsEmpty() && plan.IsNoMatch() {
		// plan.Log.Debugf("empty match")
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

			// TODO: vectorize pack & journal merge, no more need to preserve order
			// - fast path when journal result has no overlap with pack result
			// - remove pack selection for pks that are in the journal (or deleted)
			// - output all remaining matches (no switch between pack and journal)
			// - last, output all journal matches, segment by segment (keep per segment
			//   selection vector)
			//
			// TableReader already supports ExcludeMask which looks perfect for this
			// scenario too. Produce a RowId mask for all deleted records (by updates
			// or deletes), exclude during read. Then we need no more check for pk's here!!
			//
			// Move limit/offset into result, count there and skip what is appended, return
			// EndOfStream signal

			nRowsMatched += pkg.NumSelected()
			if err = res.Append(ctx, pkg); err != nil {
				return err
			}

			// for _, idx := range pkg.Selected() {
			// 	index := int(idx)
			// 	pk := pkg.Pk(index)

			// 	// skip broken records (invalid pk)
			// 	// if pk == 0 {
			// 	// 	continue
			// 	// }

			// 	// skip deleted records
			// 	if jres.IsDeleted(pk) {
			// 		continue
			// 	}

			// 	// use journal record if exists
			// 	src := pkg
			// 	if idx, ok := jres.FindPk(pk); ok {
			// 		// remove match bit (so we don't output this record twice)
			// 		jres.UnsetMatch(pk)

			// 		// use journal segment pack and offset to access result
			// 		src, index = jres.GetRef(idx)
			// 	}

			// 	// skip offset
			// 	if plan.Offset > 0 {
			// 		plan.Offset--
			// 		continue
			// 	}

			// 	// emit record
			// 	nRowsMatched++
			// 	if err := res.AppendRange(src, index, index+1); err != nil {
			// 		return err
			// 	}

			// 	// apply limit
			// 	if plan.Limit > 0 && nRowsMatched >= plan.Limit {
			// 		break packloop
			// 	}
			// }
		}
	}

	// finalize on limit
	// if plan.Limit > 0 && nRowsMatched >= plan.Limit {
	// 	return nil
	// }

	// after all packs have been scanned, add remaining rows from journal, if any
	for pkg := range jres.Iterator() {
		// skip offset
		// if plan.Offset > 0 {
		// 	plan.Offset--
		// 	return nil
		// }

		// emit
		plan.Log.Debugf("journal segment %d with %d matches", pkg.Key(), pkg.NumSelected())
		nRowsMatched += pkg.NumSelected()
		if err = res.Append(ctx, pkg); err != nil {
			return err
		}

		// // apply limit
		// if plan.Limit > 0 && nRowsMatched == plan.Limit {
		// 	return types.EndStream
		// }
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
		// // stop on first pk that is merged into table data
		// pk := pkg.Pk(idx)
		// if pk <= maxStoredPk {
		// 	return types.EndStream
		// }

		// // skip offset
		// if plan.Offset > 0 {
		// 	plan.Offset--
		// 	return nil
		// }

		// forward pack with matches
		nRowsMatched += pkg.NumSelected()
		if err := res.Append(ctx, pkg); err != nil {
			return err
		}

		// remove match bit (so we don't output this record twice)
		// jres.UnsetMatch(pk)

		// apply limit
		// if plan.Limit > 0 && nRowsMatched == plan.Limit {
		// 	return types.EndStream
		// }
		// return nil
	}
	plan.Stats.Tick(JOURNAL_TIME_KEY)

	// finalize on limit
	// if plan.Limit > 0 && nRowsMatched >= plan.Limit {
	// 	return nil
	// }

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

		// walk hits in reverse scan order
		// hits := pkg.Selected()
		// for k := len(hits) - 1; k >= 0; k-- {
		// 	index := int(hits[k])
		// 	src := pkg

		// 	// skip broken records (invalid pk)
		// 	pk := pkg.Pk(index)
		// 	if pk == 0 {
		// 		continue
		// 	}

		// 	// skip deleted records
		// 	if jres.IsDeleted(pk) {
		// 		continue
		// 	}

		// 	// use journal record if exists
		// 	if idx, ok := jres.FindPk(pk); ok {
		// 		// remove match bit (so we don't output this record twice)
		// 		jres.UnsetMatch(pk)

		// 		// use journal segment pack and offset to access result
		// 		src, index = jres.GetRef(idx)
		// 	}

		// 	// skip offset
		// 	if plan.Offset > 0 {
		// 		plan.Offset--
		// 		continue
		// 	}

		// 	// emit record
		// 	nRowsMatched++
		// 	if err := res.AppendRange(src, index, index+1); err != nil {
		// 		return err
		// 	}

		// 	// apply limit
		// 	if plan.Limit > 0 && nRowsMatched == plan.Limit {
		// 		break packloop
		// 	}
		// }
	}

	return nil
}
