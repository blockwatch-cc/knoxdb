// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package table

import (
	"context"

	"blockwatch.cc/knoxdb/internal/engine"
)

// TODO: refactor update
// - UpsertRows: [List Pks] -> [Index Lookup] -> [Update Journal]
// - Update[query]: [Query] -> [Apply Expr] -> [Update Journal]
//
// # WAL Record encoding
//
// | rid1 | sz | ref1 | ref2 | .. | wire1 | wire2 | ... |
// rid1: row id of first update record
// refX: row ids replaced by each record
func (t *Table) UpdateRows(ctx context.Context, buf []byte) (uint64, error) {
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
