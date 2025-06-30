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
)

// TODO: refactor update
// - Update[query]: [Query] -> [Apply Expr] -> [Update Journal]
// - UpsertRows: [Rows] -> [List Pks] -> [Index Lookup Rids] -> [full pack] -> [Update Journal]
//
// WAL Record encoding
// | changeset | rid1 | ref1 | wire1 | ref2 | wire2 | ... |
func (t *Table) UpdateRows(ctx context.Context, buf []byte) (uint64, error) {
	// Upsert = insert (pk = 0) & update (pk != 0)
	// - same pk can be inserted, then updated multiple times in the same batch
	// - input in record format with full details except row id (and other metadata)
	// - challenges
	//   1. lookup current row id for each updated pk (current tx snapshot visibility)
	//   2. Pks may be out of order
	//   3. updates may target earlier in-batch inserts and updates
	// - concepts
	//   1. pk -> rid index + in-journal index/lookup (single tx: all journal content is eligible)
	//   2. execute insterts first, build pack of updates, sort via sel vector
	//   2. track all pk->rid from this call's inserts & updates as map[u64]u64

	return 0, engine.ErrNotImplemented

	// 	// check message
	// 	if len(buf) == 0 {
	// 		return 0, nil
	// 	}
	// 	if len(buf) < t.schema.WireSize() {
	// 		return 0, engine.ErrShortMessage
	// 	}

	// 	// check table state
	// 	if t.opts.ReadOnly {
	// 		return 0, engine.ErrTableReadOnly
	// 	}
	// 	atomic.AddInt64(&t.metrics.UpdateCalls, 1)

	// 	// obtain shared table lock
	// 	tx := engine.GetTransaction(ctx)
	// 	err := tx.RLock(ctx, t.id)
	// 	if err != nil {
	// 		return 0, err
	// 	}

	// 	// register table for commit/abort callbacks
	// 	tx.Touch(t.id)

	// 	// alloc scratch buffer for wal messages, we write 8 bytes first rid
	// 	// plus 4 bytes batch size plus 8 byte per reference row id
	// 	wb := arena.AllocBytes(t.journal.MaxSize()*8 + 12)
	// 	defer arena.Free(wb)

	// 	// break message batch into pieces so that each piece fits into the
	// 	// current journal's active segment. for each pk lookup its current
	// 	// live row id for reference is this will be marked for deletion.
	// 	view := schema.NewView(t.schema)
	// 	var count int64

	// bufloop:
	// 	for len(buf) > 0 {
	// 		var (
	// 			szHead, szBody int
	// 			vint           [num.MaxVarintLen64]byte
	// 			jcap           = t.journal.Capacity()
	// 			rid            = t.state.NextRid
	// 			wbuf           *bytes.Buffer
	// 		)

	// 		// split buf into wire messages
	// 		view, vbuf, _ := view.Cut(buf)

	// 		if tx.UseWal() {
	// 			wbuf = bytes.NewBuffer(wb)

	// 			// write fixed size placeholder for header len (will overwrite later)
	// 			var tmp [4]byte
	// 			wbuf.Write(tmp[:])

	// 			// write first rid in this batch
	// 			n := num.PutUvarint(vint[:], rid+1)
	// 			wbuf.Write(vint[:n])
	// 			szHead += 4 + n
	// 		}

	// 		// step 1: check pk is valid, lookup current live rid, append journal
	// 		for view.IsValid() && jcap > 0 {
	// 			// check pk is set
	// 			pk := view.GetPk()
	// 			if pk == 0 {
	// 				err = engine.ErrNoPk
	// 				break bufloop
	// 			}

	// 			// lookup most recent rid for this pk
	// 			var ref uint64
	// 			ref, err = t.doLookupRid(ctx, pk)
	// 			if err != nil {
	// 				break bufloop
	// 			}
	// 			if ref == 0 {
	// 				// record does not exist or was deleted
	// 				err = engine.ErrRecordNotFound
	// 				break bufloop
	// 			}

	// 			// append update under new row id
	// 			rid++
	// 			t.journal.Update(tx.Id(), rid, pk, ref, view.Bytes())

	// 			if tx.UseWal() {
	// 				// write record ref
	// 				n := num.PutUvarint(vint[:], ref)
	// 				wbuf.Write(vint[:n])
	// 				szHead += n
	// 			}
	// 			jcap--
	// 			count++
	// 			szBody += len(view.Bytes())
	// 			view, vbuf, _ = view.Cut(vbuf)
	// 		}

	// 		if tx.UseWal() {
	// 			// step 2: write updates to wal
	// 			head := wbuf.Bytes()
	// 			LE.PutUint32(head, uint32(szHead)) // patch header size

	// 			// write wal batch
	// 			_, err = t.engine.Wal().Write(&wal.Record{
	// 				Type:   wal.RecordTypeUpdate,
	// 				Tag:    types.ObjectTagTable,
	// 				Entity: t.id,
	// 				TxID:   tx.Id(),
	// 				Data:   [][]byte{head, buf[:szBody]},
	// 			})
	// 			if err != nil {
	// 				break
	// 			}
	// 		}

	// 		// advance message buffer and write latest row id to state
	// 		t.state.NextRid = rid
	// 		view.Reset(nil)
	// 		buf = buf[szBody:]

	// 		// once a journal segment is full
	// 		// - rotate segment, flush to disk
	// 		// - flush other dirty segment's xmeta to disk
	// 		// - write checkpoint record
	// 		if t.journal.Active().IsFull() {
	// 			err = t.flushJournal(ctx)
	// 			if err != nil {
	// 				break
	// 			}
	// 		}
	// 	}
	// 	if err != nil {
	// 		return 0, err
	// 	}

	// 	if count > 0 {
	// 		atomic.AddInt64(&t.metrics.UpdatedTuples, count)
	// 	}

	// return uint64(count), nil
}

// TODO
// - current interface is not useful (just so code compiles)
// - need Update(ctx, plan) (where plan contains change col list and ExprVM code)
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

	// run the query, forward result to journal update
	upd := NewUpdater(q, t.journal)
	if err = t.doQueryAsc(ctx, plan, upd); err != nil {
		return 0, err
	}
	atomic.AddInt64(&t.metrics.UpdatedTuples, int64(upd.Len()))

	return uint64(upd.Len()), nil
}
