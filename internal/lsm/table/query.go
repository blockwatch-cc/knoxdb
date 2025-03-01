// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package table

import (
	"bytes"
	"context"
	"fmt"
	"sync/atomic"

	"blockwatch.cc/knoxdb/internal/engine"
	"blockwatch.cc/knoxdb/internal/query"
	"blockwatch.cc/knoxdb/internal/store"
	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/bitmap"
	"blockwatch.cc/knoxdb/pkg/schema"
)

// Table Query Interface
// - requires main data bucket to be indexed by pk (uint64)
// - generate index scan ranges from query conditions
// - run index scans -> bitsets
// - merge bitsets along condition tree
// - resolve result from value bucket via final bitset
// - append row data to Result
// - result decoder can skip unused fields

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

	if err := plan.QueryIndexes(ctx); err != nil {
		return nil, err
	}

	res := NewResult(plan.ResultSchema, int(plan.Limit))

	err = t.doQuery(ctx, plan, res)
	if err != nil {
		return nil, err
	}

	return res, nil
}

func (t *Table) Stream(ctx context.Context, q engine.QueryPlan, fn func(engine.QueryRow) error) error {
	plan, ok := q.(*query.QueryPlan)
	if !ok {
		return fmt.Errorf("invalid query plan type %T", q)
	}

	if err := plan.QueryIndexes(ctx); err != nil {
		return err
	}

	res := NewStreamResult(plan.ResultSchema, fn)

	err := t.doQuery(ctx, plan, res)
	if err != nil && err != types.EndStream {
		return err
	}

	return nil
}

func (t *Table) doQuery(ctx context.Context, plan *query.QueryPlan, res QueryResultConsumer) error {
	var (
		bits                       bitmap.Bitmap
		key                        [8]byte
		nRowsScanned, nRowsMatched uint32
	)

	// open read transaction
	tx, err := engine.GetTransaction(ctx).StoreTx(t.db, false)
	if err != nil {
		return err
	}

	// cleanup and log on exit
	defer func() {
		plan.Stats.Tick(query.SCAN_TIME_KEY)
		plan.Stats.Count(query.ROWS_SCANNED_KEY, int(nRowsScanned))
		plan.Stats.Count(query.ROWS_MATCHED_KEY, int(nRowsMatched))
		atomic.AddInt64(&t.metrics.QueryCalls, 1)
		atomic.AddInt64(&t.metrics.QueriedTuples, int64(nRowsMatched))
		bits.Free()
	}()

	bucket := tx.Bucket(append([]byte(t.schema.Name()), engine.DataKeySuffix...))
	if bucket == nil {
		return store.ErrNoBucket
	}

	// prepare result converter (table schema -> result schema)
	conv := schema.NewConverter(t.schema, plan.ResultSchema, NE)

	// handle cases
	switch {
	case plan.Filters.IsAnyMatch():
		// No conds: walk entire table
		c := bucket.Cursor(store.ForwardCursor)
		defer c.Close()
		for ok := c.First(); ok; ok = c.Next() {
			// skip offset
			if plan.Offset > 0 {
				plan.Offset--
				continue
			}

			// convert result schema and store
			if err := res.Append(conv.Extract(c.Value()), t.isZeroCopy); err != nil {
				return err
			}

			// apply limit
			nRowsScanned++
			nRowsMatched++
			if plan.Limit > 0 && nRowsMatched >= plan.Limit {
				break
			}
		}

	case plan.Filters.IsProcessed():
		// 1: full index query -> everything is resolved, walk bitset
		it := plan.Filters.Bits.Bitmap.NewIterator()
		for id := it.Next(); id > 0; id = it.Next() {
			// skip offset
			if plan.Offset > 0 {
				plan.Offset--
				continue
			}
			BE.PutUint64(key[:], id)
			val := bucket.Get(key[:])
			if val == nil {
				// warn on indexed but missing pks
				plan.Log.Warnf("query %s: missing index scan PK %d on table %s", plan.Tag, id, t.name())
				continue
			}

			// convert result schema and store
			if err := res.Append(conv.Extract(val), t.isZeroCopy); err != nil {
				return err
			}

			// apply limit
			nRowsScanned++
			nRowsMatched++
			if plan.Limit > 0 && nRowsMatched >= plan.Limit {
				break
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
				// warn on indexed but missing pks
				plan.Log.Warnf("query %s: missing index scan PK %d on table %s", plan.Tag, id, t.name())
				continue
			}

			// check conditions
			nRowsScanned++
			if !query.MatchTree(plan.Filters, view.Reset(buf)) {
				continue
			}

			// skip offset
			if plan.Offset > 0 {
				plan.Offset--
				continue
			}

			// convert result schema and store
			if err := res.Append(conv.Extract(buf), t.isZeroCopy); err != nil {
				return err
			}

			// apply limit
			nRowsMatched++
			if plan.Limit > 0 && nRowsMatched >= plan.Limit {
				break
			}
		}
	default:
		// 3: partial index query & root = OR: walk full table and check each value
		// 4: no index query: walk full table and check each value
		c := bucket.Cursor(store.ForwardCursor)
		defer c.Close()
		view := schema.NewView(t.schema)

		// construct prefix scan from unprocessed pk condition(s) if any
		var first, last [8]byte
		from, to := PkRange(plan.Filters, t.schema)
		BE.PutUint64(first[:], from)
		BE.PutUint64(last[:], to)

		for ok := c.Seek(first[:]); ok && bytes.Compare(c.Key(), last[:]) <= 0; ok = c.Next() {
			buf := c.Value()

			// check conditions
			nRowsScanned++
			if !query.MatchTree(plan.Filters, view.Reset(buf)) {
				continue
			}

			// skip offset
			if plan.Offset > 0 {
				plan.Offset--
				continue
			}

			// convert result schema and store
			if err := res.Append(conv.Extract(buf), t.isZeroCopy); err != nil {
				return err
			}

			// apply limit
			nRowsMatched++
			if plan.Limit > 0 && nRowsMatched >= plan.Limit {
				break
			}
		}
	}

	return nil
}

func (t *Table) Count(ctx context.Context, q engine.QueryPlan) (uint64, error) {
	var (
		bits                       bitmap.Bitmap
		key                        [8]byte
		nRowsScanned, nRowsMatched uint32
	)

	plan, ok := q.(*query.QueryPlan)
	if !ok {
		return 0, fmt.Errorf("invalid query plan type %T", q)
	}

	if err := plan.QueryIndexes(ctx); err != nil {
		return 0, err
	}

	// open read transaction
	tx, err := engine.GetTransaction(ctx).StoreTx(t.db, false)
	if err != nil {
		return 0, err
	}

	// cleanup and log on exit
	defer func() {
		plan.Stats.Tick(query.SCAN_TIME_KEY)
		plan.Stats.Count(query.ROWS_SCANNED_KEY, int(nRowsScanned))
		plan.Stats.Count(query.ROWS_MATCHED_KEY, int(nRowsMatched))
		atomic.AddInt64(&t.metrics.QueryCalls, 1)
		atomic.AddInt64(&t.metrics.QueriedTuples, int64(nRowsMatched))
		bits.Free()
	}()

	bucket := tx.Bucket(append([]byte(t.schema.Name()), engine.DataKeySuffix...))
	if bucket == nil {
		return 0, store.ErrNoBucket
	}

	// handle cases
	switch {
	case plan.Filters.IsAnyMatch():
		// No conds: walk entire table
		c := bucket.Cursor(store.IndexCursor)
		defer c.Close()
		for ok := c.First(); ok; ok = c.Next() {
			nRowsMatched++
		}

	case plan.Filters.IsProcessed():
		// 1: full index query -> everything is resolved, count bitset
		nRowsMatched = uint32(plan.Filters.Bits.Count())

	case !plan.Filters.OrKind && plan.Filters.Bits.IsValid():
		// 2: partial index query & root = AND: walk bitset but check each value
		it := plan.Filters.Bits.Bitmap.NewIterator()
		view := schema.NewView(t.schema)
		for id := it.Next(); id > 0; id = it.Next() {
			BE.PutUint64(key[:], id)
			buf := bucket.Get(key[:])
			if buf == nil {
				// warn on indexed but missing pks
				plan.Log.Warnf("query %s: missing index scan PK %d on table %s", plan.Tag, id, t.name())
				continue
			}

			// check conditions
			nRowsScanned++
			if !query.MatchTree(plan.Filters, view.Reset(buf)) {
				continue
			}

			nRowsMatched++
		}

	default:
		// 3: partial index query & root = OR: walk full table and check each value
		// 4: no index query: walk full table and check each value
		c := bucket.Cursor(store.ForwardCursor)
		defer c.Close()
		view := schema.NewView(t.schema)
		for ok := c.First(); ok; ok = c.Next() {
			buf := c.Value()

			// check conditions
			nRowsScanned++
			if !query.MatchTree(plan.Filters, view.Reset(buf)) {
				continue
			}

			nRowsMatched++
		}
	}

	return uint64(nRowsMatched), nil
}

func (t *Table) Lookup(ctx context.Context, pks []uint64) (engine.QueryResult, error) {
	res := NewResult(t.schema, len(pks))
	err := t.doLookup(ctx, pks, res)
	if err != nil {
		return nil, err
	}
	return res, nil
}

func (t *Table) StreamLookup(ctx context.Context, pks []uint64, fn func(engine.QueryRow) error) error {
	res := NewStreamResult(t.schema, fn)
	err := t.doLookup(ctx, pks, res)
	if err != nil && err != types.EndStream {
		return err
	}
	return nil
}

func (t *Table) doLookup(ctx context.Context, pks []uint64, res QueryResultConsumer) error {
	var (
		key          [8]byte
		nRowsMatched uint32
	)

	// open read transaction
	tx, err := engine.GetTransaction(ctx).StoreTx(t.db, false)
	if err != nil {
		return err
	}

	// cleanup on exit
	defer func() {
		atomic.AddInt64(&t.metrics.QueryCalls, 1)
		atomic.AddInt64(&t.metrics.QueriedTuples, int64(nRowsMatched))
		tx.Rollback()
	}()

	bucket := tx.Bucket(append([]byte(t.schema.Name()), engine.DataKeySuffix...))
	if bucket == nil {
		return store.ErrNoBucket
	}

	for _, pk := range pks {
		if pk == 0 {
			continue
		}

		BE.PutUint64(key[:], pk)
		buf := bucket.Get(key[:])
		if buf == nil {
			continue
		}

		nRowsMatched++
		if err := res.Append(buf, t.isZeroCopy); err != nil {
			return err
		}
	}
	return nil
}
