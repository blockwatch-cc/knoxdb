// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package table

import (
	"context"
	"fmt"
	"sync/atomic"

	"blockwatch.cc/knoxdb/internal/engine"
	"blockwatch.cc/knoxdb/internal/pack"
	"blockwatch.cc/knoxdb/internal/pack/journal"
	"blockwatch.cc/knoxdb/internal/query"
	"blockwatch.cc/knoxdb/pkg/schema"
)

var _ engine.QueryResultConsumer = (*DeleteAdapter)(nil)

type DeleteAdapter struct {
	j *journal.Journal
	n int
}

func (d *DeleteAdapter) Len() int {
	return d.n
}

func (d *DeleteAdapter) Append(ctx context.Context, pkg *pack.Package) error {
	n, err := d.j.DeletePack(ctx, pkg)
	d.n += n
	return err
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

	// amend query plan to only output rid field
	rs, err := t.schema.SelectFieldIds(schema.MetaRid)
	if err != nil {
		return 0, err
	}
	plan.ResultSchema = rs.WithName("delete")

	// register state reset callback only once
	if !tx.Touched(t.id) {
		prevState := t.state
		tx.OnAbort(func(_ context.Context) error {
			t.state = prevState
			return nil
		})
	}

	// register table for commit/abort callbacks
	tx.Touch(t.id)

	// protect journal access
	t.mu.RLock()
	defer t.mu.RUnlock()

	// run the query, forward result to journal delete
	res := &DeleteAdapter{j: t.journal}
	if err = t.doQueryAsc(ctx, plan, res); err != nil {
		return 0, err
	}
	atomic.AddInt64(&t.metrics.DeletedTuples, int64(res.Len()))
	atomic.AddInt64(&t.metrics.DeleteCalls, 1)

	return uint64(res.Len()), nil
}
