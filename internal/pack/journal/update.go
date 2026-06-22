// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package journal

import (
	"bytes"
	"context"
	"encoding/binary"

	"blockwatch.cc/knoxdb/internal/arena"
	"blockwatch.cc/knoxdb/internal/bitset"
	"blockwatch.cc/knoxdb/internal/engine"
	"blockwatch.cc/knoxdb/internal/pack"
	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/internal/wal"
	"blockwatch.cc/knoxdb/pkg/schema"
)

// UpdateBatch appends record updates to journal and WAL. Requires an active
// write transaction which may or may not commit. Encoding schema for the
// batch is already validated to match the current table schema.
//
// For each updated record UpdateBatch adds one new record to the active
// journal segment (a.k.a. post-image) and one tombstone record to the
// active journal segment's tombstone which marks the pre-image row id
// as deleted. UpdateBatch assignes a new row id to the post image record
// and ensures row ids are not reused (assigment is sequential). However,
// when a transaction aborts, the still invisible new row ids are reclaimed
// which is safe under single-writer transaction policy.
//
// For efficient tombstone inserts the previous row id for an updated
// record is required. UpdateBatch expects this mapping to get passed
// in via ridMap, so it must be looked up prior to calling UpdateBatch.
// The mapping will be updated by to the most recent rid in case a batch
// contains multiple updates of the same pk.
//
// UpdateBatch does not analyze which fields have changed and instead
// writes full records to WAL.
//
// Only source of errors is WAL write or system crash. For efficient recovery
// we break the message batch into pieces so that each piece fits into the
// current journal's active segment. This ensures each journal segment aligns
// with a WAL LSN which we can use as recovery checkpoint.
//
//	WAL Record format for update actions
//
// | schema-version | schema-hash | changeset | rid-start | ref1 | wire1 | ... |
//
// Transactions can disable WAL mode selectively in which case no WAL records
// are written and updates go to the journal only.
func (j *Journal) UpdateBatch(ctx context.Context, batch *schema.Batch, ridMap map[uint64]uint64) (int, error) {
	var (
		sm       = j.tip.data.Schema()
		tx       = engine.GetTx(ctx)
		xid      = tx.Id()                    // id of user tx
		firstRid = j.tip.tstate.NextRid       // first assigned rid (per wal batch!)
		nextRid  = firstRid                   // next free row id to assign
		count    int                          // count of processed records so far
		bits     = bitset.New(sm.Len()).One() // bitset of all column positions
		scratch  [binary.MaxVarintLen64]byte
	)

	// write header once (schema + changeset)
	head := make([]byte, 0, schema.BatchHeaderSize+bits.Size())
	head = sm.AppendBatchHeader(head)
	head = append(head, bits.Bytes()...)

	// dimension WAL write buffer (rid + refid + record)
	sz := j.maxsz * (binary.MaxVarintLen64 + sm.EstWireSize)
	if batch.Len() < j.maxsz {
		sz = binary.MaxVarintLen64*batch.Len() + batch.Size()
	}
	buf := arena.Alloc[uint8](sz)[:0]

	for batch.Size() > 0 {
		var (
			sz, n int
			jcap  = j.Capacity()
		)

		// split batch into wire messages
		for _, view := range batch.Records() {
			// stop on journal segment bounadry
			if jcap == 0 {
				break
			}

			// get pk and lookup current rid
			pk := view.GetPk()
			ref := ridMap[pk]

			// write to wal msg | ref | wire |
			buf = binary.AppendUvarint(buf, ref)
			buf = append(buf, view.Buffer()...)

			// add update to journal
			j.tip.UpdateRecord(xid, nextRid, ref, view.Buffer())

			// keep new assigned rid (in case we update again later)
			ridMap[pk] = nextRid

			// next iteration
			nextRid++
			jcap--
			n++
			sz += len(view.Buffer())
		}
		// j.log.Debugf("journal updated %d records in segment %d", nextRid-firstRid, j.tip.Id())

		// 2 write to wal
		if tx.UseWal() {
			_, err := j.wal.Write(&wal.Record{
				Type:   wal.RecordTypeUpdate,
				Tag:    types.ObjectTagTable,
				Entity: j.id,
				TxID:   xid,
				Data: [][]byte{
					head,
					binary.AppendUvarint(scratch[:0], firstRid),
					buf,
				},
			})
			if err != nil {
				return 0, err
			}
			firstRid = nextRid
		}

		// advance batch buffer
		batch.TrimAt(sz)

		// prepare next iteration
		buf = buf[:0]
		count += n

		// update object state
		j.tip.tstate.NextRid = nextRid

		// rotate segment once full
		if tx.UseWal() {
			if err := j.rotateAndCheckpoint(); err != nil {
				return 0, err
			}
		} else {
			j.rotateWhenFull()
		}
	}

	arena.Free(buf)

	return count, nil
}

// Updates selected records from a pack, typically a query result with some
// changed (materialized/dirty) vectors and an optional selection vector.
// Src may contain a mix of compressed and materialized/computed columns and
// may be an otherwise read-only pack or another journal pack.
//
// Requires an active write transaction. At this point it is unclear if the
// tx will commit, hence the update is tentative and requires a subseqent
// Abort() or Commit() call.
//
// Only source of errors is WAL write or system crash. For efficient recovery
// we break the message batch into pieces so that each piece fits into the
// current journal's active segment. This ensures each journal segment
// aligns with a WAL LSN which we can use as recovery checkpoint.
//
// Transactions allow to turn WAL mode off selectively. We choose the appropriate
// algorithm for each case.
func (j *Journal) UpdatePack(ctx context.Context, src *pack.Package) (int, error) {
	tx := engine.GetTx(ctx)
	xid := tx.Id()
	if tx.UseWal() {
		return j.updatePackWithWal(src, xid)
	} else {
		return j.updatePackNoWal(src, xid)
	}
}

func (j *Journal) updatePackNoWal(src *pack.Package, xid types.XID) (int, error) {
	var (
		state   pack.AppendState
		mode    = pack.WriteModeAll
		nextRid = j.tip.tstate.NextRid // first assigned rid (per wal batch!)
		n       int
		count   int
	)
	if src.Selected() != nil {
		mode = pack.WriteModeIncludeSelected
	}

	for {
		pos := j.tip.data.Len()

		// call append and use last version of state, returns next state
		n, state = j.tip.data.AppendSelected(src, mode, state)
		count += n

		// write rid, ref, xmin
		rids := j.tip.data.RowIds()
		refs := j.tip.data.RefIds()
		xmins := j.tip.data.Xmins()
		for i := pos; i < pos+n; i++ {
			ref := rids.Get(i)

			// write rid, ref, xid vectors directly
			rids.Set(i, nextRid)
			refs.Set(i, ref)
			xmins.Set(i, uint64(xid))

			// add insert + delete info, set xmax on ref when in tip segment
			j.tip.NotifyUpdate(xid, nextRid, ref)
			nextRid++
		}

		// update object state
		j.tip.tstate.NextRid = nextRid

		// rotate segment once full
		j.rotateWhenFull()

		// stop when src is exhausted
		if !state.More() {
			break
		}
	}

	return count, nil
}

// updatePackWithWal processes all selected records from src and
// appends them as updates to journal and WAL. Not all column
// vectors in src may have changed, but it is guaranteed that
// the ones that are updated are backed by materialized blocks.
// Updated blocks are identified by the dirty flag. Src may contain
// a selection vector when some records have changed, otherwise
// all records are assumed to require update.
//
// The update constructs short wire messages from just the updated
// columns and signals which columns were updated with a changeset
// bitmap. Row ids in src are "old" pre-image row ids which are added
// to the tombstone. On journal insert, updatePackWithWal will assign
// new row ids sequentially. The WAL record will contain the first
// assigned rowid in its header.
//
// WAL format
// | schema-version | schema-hash | changeset | rid-start | ref1 | wire1 | ... |
func (j *Journal) updatePackWithWal(src *pack.Package, xid types.XID) (int, error) {
	var (
		sm      = src.Schema()         // source schema
		sel     = src.Selected()       // selection vector, may be nil
		changed = make([]int, 0)       // change column positions
		bits    = bitset.New(sm.Len()) // bitset of changed column positions
		nextRid = j.tip.tstate.NextRid // next free row id to assign
		count   int                    // count of processed records so far
		rids    = src.RowIds()         // current rowid accessor
		rec     = &wal.Record{         // wal record template
			Type:   wal.RecordTypeUpdate,
			Tag:    types.ObjectTagTable,
			Entity: j.id,
			TxID:   xid,
			Data:   make([][]byte, 3),
		}
		scratch [binary.MaxVarintLen64]byte
	)

	// determine change set columns (from block dirty flags) and
	// estimate change record size
	var sz int
	for i, b := range src.Blocks() {
		if b == nil || !b.IsDirty() {
			continue
		}
		bits.Set(i)
		changed = append(changed, i)
		f := sm.Fields[i]
		sz += f.WireSize()
		if !f.IsFixedSize() {
			sz += len(b.Max().([]byte)) // only var sized block type
		}
	}

	// write header once (schema + changeset)
	head := make([]byte, 0, schema.BatchHeaderSize+bits.Size())
	head = sm.AppendBatchHeader(head)
	head = append(head, bits.Bytes()...)

	// dimension WAL write buffer (N * refid + record)
	if src.NumSelected() < j.maxsz {
		sz += (binary.MaxVarintLen64 + sz) * src.NumSelected()
	} else {
		sz += (binary.MaxVarintLen64 + sz) * j.maxsz
	}
	buf := arena.Alloc[uint8](sz)
	msg := bytes.NewBuffer(buf[:0])

	if sel == nil {
		// write all records when no selection vector is defined
		var i int
		for i < src.Len() {
			n := min(src.Len()-count, j.Capacity())
			batchRid := nextRid

			// write to WAL msg and update journal metadata
			// | ref1 | wire1 | ..
			for range n {
				// get ref id and write to WAL msg
				ref := rids.Get(i)
				msg.Write(binary.AppendUvarint(scratch[:0], ref))

				// extract wire change format for the record
				src.ReadWireFields(msg, i, changed)

				// insert + delete ref to tip, set xmax on ref when in tip segment
				j.tip.NotifyUpdate(xid, nextRid, ref)

				nextRid++
				i++
			}

			// write to wal
			rec.Data[0] = head
			rec.Data[1] = binary.AppendUvarint(scratch[:0], batchRid)
			rec.Data[2] = msg.Bytes()
			_, err := j.wal.Write(rec)
			if err != nil {
				return 0, err
			}

			// prepare next round
			rec.Data[0] = nil
			rec.Data[1] = nil
			rec.Data[2] = nil
			msg.Reset()
			count += n

			// update object state
			j.tip.tstate.NextRid = nextRid

			// rotate segment once full
			if err := j.rotateAndCheckpoint(); err != nil {
				return 0, err
			}
		}
	} else {
		// write selected rows up until capacity limit, continue with next sel each round
		for len(sel) > 0 {
			n := min(len(sel), j.Capacity())
			batchRid := nextRid

			// write to WAL msg and update journal metadata
			// | ref1 | wire1 | ..
			for _, v := range sel[:n] {
				// get ref id and write to WAL msg
				ref := rids.Get(int(v))
				msg.Write(binary.AppendUvarint(scratch[:0], ref))

				// extract wire change format for this record
				src.ReadWireFields(msg, int(v), changed)

				// insert + delete ref to tip, set xmax on ref when in tip segment
				j.tip.NotifyUpdate(xid, nextRid, ref)

				nextRid++
			}

			// write to wal
			rec.Data[0] = head
			rec.Data[1] = binary.AppendUvarint(scratch[:0], batchRid)
			rec.Data[2] = msg.Bytes()
			_, err := j.wal.Write(rec)
			if err != nil {
				return 0, err
			}

			// prepare next round
			rec.Data[0] = nil
			rec.Data[1] = nil
			rec.Data[2] = nil
			msg.Reset()
			count += n
			sel = sel[n:]

			// update object state
			j.tip.tstate.NextRid = nextRid

			// rotate segment once full
			if err := j.rotateAndCheckpoint(); err != nil {
				return 0, err
			}
		}
	}

	arena.Free(buf)

	return count, nil
}
