// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package journal

import (
	"bytes"
	"context"

	"blockwatch.cc/knoxdb/internal/arena"
	"blockwatch.cc/knoxdb/internal/bitset"
	"blockwatch.cc/knoxdb/internal/engine"
	"blockwatch.cc/knoxdb/internal/pack"
	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/internal/wal"
	"blockwatch.cc/knoxdb/pkg/num"
)

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
	tx := engine.GetTransaction(ctx)
	xid := tx.Id()
	if tx.UseWal() {
		return j.updatePackWithWal(src, xid, tx.Engine().Wal())
	} else {
		return j.updatePackNoWal(src, xid)
	}
}

func (j *Journal) updatePackNoWal(src *pack.Package, xid uint64) (int, error) {
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
			xmins.Set(i, xid)

			// add insert + delete info, set xmax on ref when in tip segment
			j.tip.NotifyUpdate(xid, nextRid, ref)
			nextRid++
		}

		// update object state
		j.tip.tstate.NextRid = nextRid

		// rotate segment once full
		j.rotate()

		// stop when src is exhausted
		if !state.More() {
			break
		}
	}

	return count, nil
}

func (j *Journal) updatePackWithWal(src *pack.Package, xid uint64, w *wal.Wal) (int, error) {
	// - src: full pack with current PK / RID, some columns changed (dirty, materialized)
	//   and optional selection vector
	// - dst: journal and WAL
	//
	// update process
	// - assign new rids
	// - use old rids as refs
	// - mark old rids as deleted
	// - append full record to journal segment
	// - extract and write changeset to WAL
	//
	// WAL format
	// | changeset | n | rid1 | ref1 | ref2 | .. | wire1 | wire2 | ... |

	var (
		sel     = src.Selected()         // selection vector, may be nil
		changed = make([]int, 0)         // change column positions
		bits    = bitset.New(src.Cols()) // bitset of changed column positions
		nextRid = j.tip.tstate.NextRid   // first assigned rid (per wal batch!)
		sz      int                      // estimated per msg size for WAL buffer
		count   int                      // count of processed records so far
		rids    = src.RowIds()           // current rowid accessor
		rec     = &wal.Record{           // wal record template
			Type:   wal.RecordTypeUpdate,
			Tag:    types.ObjectTagTable,
			Entity: j.id,
			TxID:   xid,
			Data:   make([][]byte, 1),
		}
	)

	// determine change set columns (from block dirty flags)
	for i, b := range src.Blocks() {
		if b == nil || !b.IsDirty() {
			continue
		}
		bits.Set(i)
		changed = append(changed, i)
		sz += j.schema.Field(i).WireSize()
	}

	// dimension WAL write buffer (may still with grow with long strings)
	baseSz := (bits.Len()+7)/8 + num.MaxVarintLen64 // changeset + rid1
	sz += num.MaxVarintLen64                        // add max refid space
	if sel == nil {
		sz = baseSz + sz*src.Len()
	} else {
		sz = baseSz + sz*len(sel)
	}
	buf := arena.AllocBytes(sz)
	msg := bytes.NewBuffer(buf)

	if sel == nil {
		// write all records when no selection vector is defined
		var i int
		for i < src.Len() {
			n := min(src.Len()-count, j.Capacity())

			// 1 write WAL buffer and update journal metadata
			// | changeset | rid1 | ref1 | wire1 | ..
			msg.Write(bits.Bytes())
			num.WriteUvarint(msg, nextRid)
			for range n {
				ref := rids.Get(i)
				num.WriteUvarint(msg, ref)

				// extract wire change format for the record
				if err := src.ReadWireFields(msg, i, changed); err != nil {
					return 0, err
				}

				// add insert + delete info, set xmax on ref when in tip segment
				j.tip.NotifyUpdate(xid, nextRid, ref)

				nextRid++
				i++
			}

			// 2 write to wal
			rec.Data[0] = msg.Bytes()
			_, err := w.Write(rec)
			if err != nil {
				return 0, err
			}

			rec.Data[0] = nil
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

			// 1 write WAL buffer and update journal metadata
			// | changeset | rid1 | ref1 | wire1 | ..
			msg.Write(bits.Bytes())
			num.WriteUvarint(msg, nextRid)
			for _, v := range sel[:n] {
				ref := rids.Get(int(v))
				num.WriteUvarint(msg, ref)

				// extract wire change format for this record
				if err := src.ReadWireFields(msg, int(v), changed); err != nil {
					return 0, err
				}

				// add insert + delete record, set xmax on ref when in tip segment
				j.tip.NotifyUpdate(xid, nextRid, ref)

				nextRid++
			}

			// 2 write to wal
			rec.Data[0] = msg.Bytes()
			_, err := w.Write(rec)
			if err != nil {
				return 0, err
			}

			// prepare next round
			count += n
			sel = sel[n:]
			rec.Data[0] = nil
			msg.Reset()

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
