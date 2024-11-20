// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package table

import (
	"context"
	"fmt"
	"sync/atomic"

	"blockwatch.cc/knoxdb/internal/engine"
	"blockwatch.cc/knoxdb/internal/query"
	"blockwatch.cc/knoxdb/internal/store"
	"blockwatch.cc/knoxdb/pkg/schema"
)

func (t *Table) Delete(ctx context.Context, q engine.QueryPlan) (uint64, error) {
	var (
		key                        [8]byte
		nRowsScanned, nRowsMatched uint32
	)

	plan, ok := q.(*query.QueryPlan)
	if !ok {
		return 0, fmt.Errorf("invalid query plan type %T", q)
	}

	// obtain shared table lock
	err := engine.GetTransaction(ctx).RLock(ctx, t.id)
	if err != nil {
		return 0, err
	}

	if err := plan.QueryIndexes(ctx); err != nil {
		return 0, err
	}

	// open write transaction
	tx, err := engine.GetTransaction(ctx).StoreTx(t.db, true)
	if err != nil {
		return 0, err
	}

	// cleanup on exit
	defer func() {
		plan.Stats.Tick(query.SCAN_TIME_KEY)
		plan.Stats.Count(query.ROWS_SCANNED_KEY, int(nRowsScanned))
		plan.Stats.Count(query.ROWS_MATCHED_KEY, int(nRowsMatched))
		atomic.AddInt64(&t.metrics.DeleteCalls, 1)
	}()

	bucket := tx.Bucket(t.key)
	if bucket == nil {
		return 0, engine.ErrNoBucket
	}

	// handle cases
	switch {
	case plan.Filters.IsNoMatch():
		// nothing to delete
		return 0, nil

	case plan.Filters.IsProcessed():
		// 1: full index query -> everything is resolved, walk bitset
		it := plan.Filters.Bits.Bitmap.NewIterator()
		for pk := it.Next(); pk > 0; pk = it.Next() {
			BE.PutUint64(key[:], pk)
			prev, err := t.delTx(tx, key[:])
			if err != nil {
				return 0, err
			}
			if prev == nil {
				continue
			}
			nRowsMatched++

			// update indexes
			for _, idx := range t.indexes {
				idx.(engine.IndexEngine).Del(ctx, prev)
			}
		}

	case !plan.Filters.OrKind && plan.Filters.Bits.IsValid():
		// 2: partial index query & root = AND: walk bitset but check each value
		it := plan.Filters.Bits.Bitmap.NewIterator()
		view := schema.NewView(t.schema)
		for id := it.Next(); id > 0; id = it.Next() {
			BE.PutUint64(key[:], id)
			buf := bucket.Get(key[:])
			if buf == nil {
				continue
			}

			// check conditions
			nRowsScanned++
			if !query.MatchTree(plan.Filters, view.Reset(buf)) {
				continue
			}

			// delete
			prev, err := t.delTx(tx, key[:])
			if err != nil {
				return 0, err
			}
			nRowsMatched++

			// update indexes
			for _, idx := range t.indexes {
				idx.(engine.IndexEngine).Del(ctx, prev)
			}
		}
	default:
		// 3: partial index query & root = OR: walk full table and check each value
		// 4: no index query: walk full table and check each value
		c := bucket.Cursor(store.ForwardCursor)
		view := schema.NewView(t.schema)
		for ok := c.First(); ok; ok = c.Next() {
			buf := c.Value()

			// check conditions
			nRowsScanned++
			if !query.MatchTree(plan.Filters, view.Reset(buf)) {
				continue
			}

			// delete
			prev, err := t.delTx(tx, key[:])
			if err != nil {
				return 0, err
			}
			nRowsMatched++

			// update indexes
			for _, idx := range t.indexes {
				idx.(engine.IndexEngine).Del(ctx, prev)
			}
		}
	}

	return uint64(nRowsMatched), nil
}
