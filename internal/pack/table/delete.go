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
	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/schema"
)

var _ engine.QueryResultConsumer = (*DeleteAdapter)(nil)

type DeleteAdapter struct {
	j      *journal.Journal
	n      int
	limit  int
	offset int
}

func (d *DeleteAdapter) Len() int {
	return d.n
}

func (d *DeleteAdapter) Append(ctx context.Context, src *pack.Package) error {
	// read selection info
	sel := src.Selected()
	nsel := src.NumSelected()

	// apply offset and limit to selection vector, generate selection vector if necessary
	if d.offset > 0 || d.limit > 0 {
		// skip offset records
		if d.offset > 0 {
			if d.offset > nsel {
				// skip the entire src pack
				d.offset -= nsel
				return nil
			}
			if sel != nil {
				// skip offset elements from existing selection vector
				sel = sel[d.offset:]
				nsel -= d.offset
			} else {
				// create selection vector for some tail portion of src
				sel = types.NewRange(d.offset, nsel).AsSelection()
				nsel = src.Len() - d.offset
			}
			d.offset = 0
		}

		// apply limit
		if d.limit > 0 {
			if nsel > d.limit {
				if sel != nil {
					// shorten selection vector
					sel = sel[:d.limit]
				} else {
					// create selection vector
					sel = types.NewRange(0, d.limit).AsSelection()
				}
			}
		}
	}

	src.WithSelection(sel)
	n, err := d.j.DeletePack(ctx, src)
	d.n += n

	// stop when limit is reached
	if d.limit > 0 && d.n >= d.limit {
		return types.EndStream
	}
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
	tx := engine.GetTx(ctx)
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
	t.mu.Lock()
	defer t.mu.Unlock()

	// run the query, forward result to journal delete
	res := &DeleteAdapter{
		j:      t.journal,
		limit:  int(plan.Limit),
		offset: int(plan.Offset),
	}
	if err = t.doQueryAsc(ctx, plan, res); err != nil && err != types.EndStream {
		t.log.Error(err)
		return 0, err
	}
	atomic.AddInt64(&t.metrics.DeletedTuples, int64(res.Len()))
	atomic.AddInt64(&t.metrics.DeleteCalls, 1)

	return uint64(res.Len()), nil
}
