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
	"blockwatch.cc/knoxdb/pkg/slicex"
)

// UpdateRows appends full wire-encoded update records to journal and WAL.
// In contrast to Update which uses a query filter to identify records and
// their current active row ids, UpdateRecord performs a lookup to identify
// rowids for primary keys found in the wire format batch. For this reason
// all records in a batch must have a valid primary key set. UpdateRecord
// does not identify changed fields and writes the full wire encoded record
// to WAL. The WAL message encoding is compatible with Update.
//
// [Records] -> [List Pks] -> [Index Lookup Rids] -> [Update Journal]
func (t *Table) UpdateRows(ctx context.Context, buf []byte) (uint64, error) {
	// Update (pk != 0)
	// - input is record format without metadata
	// - same pk can be updated multiple times in the same batch
	// - challenges
	//   1. lookup current row id for each updated pk (MVCC visibility)
	//   2. pks across records may be out of order
	//   3. updates may update earlier records in the same batch
	// - process
	//   1. collect unique list of pks
	//   2. lookup pk -> rid from index + journal (apply MVCC rules)
	//   3. append updates to journal, will assign new row ids in batch order
	//      and track multiple updates to the same pk

	// reject invalid messages
	if len(buf) == 0 {
		return 0, nil
	}
	if len(buf) < t.schema.WireSize() {
		return 0, engine.ErrShortMessage
	}

	// check table state
	if t.opts.ReadOnly {
		return 0, engine.ErrTableReadOnly
	}
	atomic.AddInt64(&t.metrics.UpdateCalls, 1)

	// ensure table has a pk index
	idx, ok := t.PkIndex()
	if !ok {
		return 0, engine.ErrNoPkIndex
	}

	// extract list of primary keys
	var (
		view = schema.NewView(t.schema)
		pks  = make([]uint64, 0, len(buf)/t.schema.WireSize()) // upper bound
	)

	// split buf into wire messages
	view, vbuf, _ := view.Cut(buf)
	for view.IsValid() {
		pk := view.GetPk()
		if pk == 0 {
			return 0, engine.ErrNoPk
		}
		pks = append(pks, pk)
		view, vbuf, _ = view.Cut(vbuf)
	}
	pks = slicex.Unique(pks)

	// obtain shared table lock
	tx := engine.GetTransaction(ctx)
	err := tx.RLock(ctx, t.id)
	if err != nil {
		return 0, err
	}

	// register table for commit/abort callbacks
	tx.Touch(t.id)

	// build a hash map for pk -> rid (assumes u64 primary keys)
	ridMap := make(map[uint64]uint64, len(pks))
	for _, v := range pks {
		ridMap[v] = 0 // seed with 0
	}

	// lookup from index
	if err := idx.Lookup(ctx, pks, ridMap); err != nil {
		return 0, err
	}

	// protect journal access
	t.mu.Lock()
	defer t.mu.Unlock()

	// fill in journal info (does a pk have a visible and more recent rid mapping?)
	t.journal.Lookup(ridMap, tx.Snapshot())

	// write updates to journal and WAL
	n, err := t.journal.UpdateRecords(ctx, buf, ridMap)
	if err != nil {
		return 0, err
	}
	atomic.AddInt64(&t.metrics.UpdatedTuples, int64(n))

	return uint64(n), nil
}

// TODO
// [TableReader+Filter] -> [Expression VM] -> Journal UpdatePack

var _ engine.QueryResultConsumer = (*Updater)(nil)

type Updater struct {
	// op *operator.UpdateOperator
	j *journal.Journal
	n int
}

func NewUpdater(q engine.QueryPlan, j *journal.Journal) *Updater {
	// TODO: setup update expr vm
	return &Updater{j: j}
}

func (x *Updater) Len() int {
	return x.n
}

func (x *Updater) Append(ctx context.Context, src *pack.Package) error {
	// run update expressions
	// dst, res := x.op.Process(ctx, src)
	// if res == operator.ResultError {
	// 	return x.op.Err()
	// }

	// forward changed pack to journal
	n, err := x.j.UpdatePack(ctx, src)
	x.n += n
	return err
}

func (t *Table) Update(ctx context.Context, q engine.QueryPlan) (uint64, error) {
	// unpack query plan
	plan, ok := q.(*query.QueryPlan)
	if !ok {
		return 0, fmt.Errorf("invalid query plan type %T", q)
	}

	// check table state
	if t.opts.ReadOnly {
		return 0, engine.ErrTableReadOnly
	}
	atomic.AddInt64(&t.metrics.UpdateCalls, 1)

	// obtain shared table lock
	tx := engine.GetTransaction(ctx)
	err := tx.RLock(ctx, t.id)
	if err != nil {
		return 0, err
	}

	// register table for commit/abort callbacks
	tx.Touch(t.id)

	// protect journal access
	t.mu.Lock()
	defer t.mu.Unlock()

	// run the query, forward result to journal update
	upd := NewUpdater(q, t.journal)
	if err = t.doQueryAsc(ctx, plan, upd); err != nil {
		return 0, err
	}
	atomic.AddInt64(&t.metrics.UpdatedTuples, int64(upd.Len()))

	return uint64(upd.Len()), nil
}
