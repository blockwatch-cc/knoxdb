// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package journal

import (
	"context"
	"fmt"

	"blockwatch.cc/knoxdb/internal/bitset"
	"blockwatch.cc/knoxdb/internal/engine"
	"blockwatch.cc/knoxdb/internal/pack/stats"
	"blockwatch.cc/knoxdb/internal/query"
	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/internal/wal"
	"blockwatch.cc/knoxdb/internal/xroar"
	"blockwatch.cc/knoxdb/pkg/num"
	"blockwatch.cc/knoxdb/pkg/schema"
	"github.com/echa/log"
)

// In-memory journal for table insert, update and delete
// - journal acts as overlay to table storage
// - fix max size segments with data pack and tomb
// - row-id centric (each row-id represents a unique record version, updates produce new row ids)
//
// Queries
// - merge-on-query: merge journal data and tomb with table query result
// - uses snapshot isolation to hide invisible records and deletes
// - journal query produces a journal result which is a list of segment packs with selection
//   vectors
//
// Merge
// - only full segments and with no open tx can be merged
// takes to oldest mergable segmet
//
// Recover
// - journal data is saved to WAL and replayed on startup

type Journal struct {
	schema *schema.Schema // data schema
	wal    *wal.Wal       // wal reference
	key    []byte         // storage bucket name
	id     uint64         // table id (tagged hash)
	tip    *Segment       // active head segment used for writing
	tail   []*Segment     // immutable tail segments waiting for completion and flush
	maxsz  int            // max number of records before segment freeze
	maxseg int            // max number of immutable segments
	log    log.Logger     // journal logger instance
}

func NewJournal(s *schema.Schema, maxsz, maxseg int) *Journal {
	return &Journal{
		schema: s,
		key:    []byte(s.Name + "_journal"),
		id:     s.TaggedHash(types.ObjectTagTable),
		tip:    newSegment(s, 0, maxsz),
		tail:   make([]*Segment, 0, maxseg),
		maxsz:  maxsz,
		maxseg: maxseg,
		log:    log.Disabled,
	}
}

func (j *Journal) WithWal(w *wal.Wal) *Journal {
	j.wal = w
	return j
}

func (j *Journal) WithState(s engine.ObjectState) *Journal {
	s.Epoch++
	j.tip.WithState(s)
	j.tip.data.WithVersion(uint32(s.Epoch)).WithKey(uint32(s.Epoch))
	return j
}

func (j *Journal) WithLogger(l log.Logger) *Journal {
	j.log = l
	return j
}

func (j *Journal) Len() int {
	n := j.tip.Len()
	for _, v := range j.tail {
		n += v.Len()
	}
	return n
}

func (j *Journal) NumSegments() int {
	return 1 + len(j.tail)
}

func (j *Journal) NumTuples() int {
	n := j.tip.data.Len()
	for _, v := range j.tail {
		n += v.data.Len()
	}
	return n
}

func (j *Journal) NumTombstones() int {
	n := j.tip.tomb.Len()
	for _, v := range j.tail {
		n += v.tomb.Len()
	}
	return n
}

// number of records that can be inserted before active segment runs full
func (j *Journal) Capacity() int {
	return j.maxsz - j.tip.data.Len()
}

func (j *Journal) TombCapacity() int {
	return j.maxsz - j.tip.tomb.Len()
}

func (j *Journal) MaxSize() int {
	return j.maxsz
}

func (j *Journal) Schema() *schema.Schema {
	return j.schema
}

func (j *Journal) Tip() *Segment {
	return j.tip
}

func (j *Journal) State() engine.ObjectState {
	return j.tip.tstate
}

func (j *Journal) Segments() []*Segment {
	return append([]*Segment{j.tip}, j.tail...)
}

func (j *Journal) Size() (sz int) {
	sz = j.tip.Size()
	for _, v := range j.tail {
		sz += v.Size()
	}
	return
}

func (j *Journal) Reset() {
	j.tip.Reset()
	for _, v := range j.tail {
		v.Close()
	}
	clear(j.tail)
	j.tail = j.tail[:0]
}

func (j *Journal) Close() {
	j.schema = nil
	j.tip.Close()
	j.tip = nil
	for i := range j.tail {
		j.tail[i].Close()
		j.tail[i] = nil
	}
	j.tail = j.tail[:0]
	j.tail = nil
	j.wal = nil
}

// Force-rotates the current segment and writes a new WAL checkpoint.
// Rotated empty segments are still merged to make their checkpoints
// durable. Note the caller must schedule a merge task to actually
// write the new table checkpoint to disk.
func (j *Journal) Checkpoint(_ context.Context) error {
	j.doRotate()
	return j.doCheckpoint()
}

func (j *Journal) rotateAndCheckpoint() error {
	// rotate segment when full
	if !j.rotateWhenFull() {
		return nil
	}
	return j.doCheckpoint()
}

func (j *Journal) doCheckpoint() error {
	// write WAL checkpoint
	lsn, err := j.wal.Write(&wal.Record{
		Type:   wal.RecordTypeCheckpoint,
		Tag:    types.ObjectTagTable,
		Entity: j.id,
	})
	if err != nil {
		return err
	}

	// store checkpoint in segment
	j.tip.WithLSN(lsn)

	return nil
}

func (j *Journal) rotateWhenFull() bool {
	if !j.tip.IsFull() {
		return false
	}
	return j.doRotate()
}

func (j *Journal) doRotate() bool {
	j.log.Debugf("journal rotate segment %d with %d records %d tombstones",
		j.tip.Id(), j.tip.data.Len(), j.tip.tomb.Len())

	// change state
	j.tip.setState(SegmentStateWaiting)

	// generate metadata
	j.tip.stats = stats.NewRecordFromPack(j.tip.data.BuildStats(), 0)

	// append to immutable list
	j.tail = append(j.tail, j.tip)

	// create new segment and link to parent
	j.tip = newSegment(j.schema, j.tip.Id()+1, j.maxsz).WithParent(j.tip).WithState(j.tip.tstate)

	return true
}

// NextMergable returns the next journal segment that is ready to merge.
// If another segment is currently merging, error ErrAgain is returned.
func (j *Journal) NextMergable() (*Segment, error) {
	// no tail segment exists
	if len(j.tail) == 0 {
		return nil, nil
	}

	for _, seg := range j.tail {
		// segment is already merging
		switch seg.getState() {
		case SegmentStateMerging:
			if len(j.tail) > 1 {
				return nil, engine.ErrAgain
			}
			return nil, nil

		case SegmentStateComplete:
			// tail segment is complete
			seg.setState(SegmentStateMerging)

			// determine follower segment's checkpoint
			if len(j.tail) > 1 {
				seg.setCheckpoint(j.tail[1].lsn)
			} else {
				seg.setCheckpoint(j.tip.lsn)
			}
			return seg, nil

		default:
			// otherwise ignore segment
		}
	}

	// nothing to do yet
	return nil, nil
}

// Removes the merged segment from lists. Query results may still reference
// the segment's vector blocks, but the segment itself can be closed.
func (j *Journal) ConfirmMerged(ctx context.Context, s *Segment) {
	// set segment state
	s.setState(SegmentStateMerged)

	id := s.Id()
	j.log.Debugf("journal: removing merged segment %d", id)

	// remove empty and merged segments, concurrent readers hold a copy
	j.prune()

	// unlink tail segment's parent
	if len(j.tail) > 0 {
		j.tail[0].parent = nil
	} else {
		j.tip.parent = nil
	}
}

// reset segment state when merge has failed
func (j *Journal) AbortMerged(s *Segment) {
	s.setState(SegmentStateComplete)
}

func (j *Journal) CommitTx(xid types.XID) (canMerge bool, shouldWait bool) {
	if j.tip.ContainsTx(xid) {
		j.tip.CommitTx(xid)
	}

	var canPrune bool

	// commit tx across segments
	for _, v := range j.tail {
		switch v.getState() {
		case SegmentStateEmpty, SegmentStateMerged:
			canPrune = true
		case SegmentStateWaiting:
			if v.ContainsTx(xid) {
				v.CommitTx(xid)
				v.setState(SegmentStateComplete)
				canMerge = true
			}
		case SegmentStateComplete:
			canMerge = true
		case SegmentStateMerging:
			canMerge = false
		}
	}

	// handle empty and merged segments
	if canPrune {
		j.prune()
	}

	// let the table handle mergable segments
	return canMerge, len(j.tail) >= j.maxseg
}

func (j *Journal) AbortTx(xid types.XID) bool {
	// abort tx across segments, rollback table state
	var (
		pmin, rmin         uint64 = 1<<64 - 1, 1<<64 - 1
		nRowsDiff          int
		canPrune, canMerge bool
	)

	// roll-over nRowsDiff across segments to update each segments
	// row counter in case an abort crosses multiple segments
	for _, v := range j.tail {
		switch v.getState() {
		case SegmentStateEmpty, SegmentStateMerged:
			canPrune = true
		case SegmentStateComplete:
			canMerge = true
		case SegmentStateMerging:
			canMerge = false
		case SegmentStateWaiting:
			// forward abort when the segment contains this xid
			var n int
			if v.ContainsTx(xid) {
				n = v.AbortTx(xid)

				// check if state has changed
				if !v.IsEmpty() {
					v.setState(SegmentStateComplete)
					canMerge = true
				} else {
					v.setState(SegmentStateEmpty)
					canPrune = true
				}
			}
			pmin = min(pmin, v.tstate.NextPk)
			rmin = min(rmin, v.tstate.NextRid)
			v.tstate.NextPk = pmin
			v.tstate.NextRid = rmin
			v.tstate.NRows = uint64(int64(v.tstate.NRows) - int64(nRowsDiff))
			// log.Warnf("Adjust seg %d state nrowsdiff=%d to %#v", v.Id(), nRowsDiff, v.tstate)
			nRowsDiff += n
		}
	}

	// update tip, adjust state also when tip is empty to roll over changes from parent segment
	if j.tip.ContainsTx(xid) {
		j.tip.AbortTx(xid)
	}
	pmin = min(pmin, j.tip.tstate.NextPk)
	rmin = min(rmin, j.tip.tstate.NextRid)
	j.tip.tstate.NextPk = pmin
	j.tip.tstate.NextRid = rmin
	j.tip.tstate.NRows = uint64(int64(j.tip.tstate.NRows) - int64(nRowsDiff))
	// log.Warnf("Adjust tip %d state with nrowsdiff=%d to %#v", j.tip.Id(), nRowsDiff, j.tip.tstate)

	// handle empty and merged segments
	if canPrune {
		j.prune()
	}

	// let the table handle mergable segments
	return canMerge
}

// called once to finalize wal replay, rollback pk/rid state
func (j *Journal) AbortActiveTx() (int, bool) {
	var (
		nAborted, nRowsDiff int
		pmin, rmin          uint64 = 1<<64 - 1, 1<<64 - 1
		canPrune, canMerge  bool
	)
	for _, v := range j.tail {
		switch v.getState() {
		case SegmentStateEmpty, SegmentStateMerged:
			canPrune = true
		case SegmentStateComplete:
			canMerge = true
		case SegmentStateMerging:
			canMerge = false
		case SegmentStateWaiting:
			n, r := v.AbortActiveTx()
			pmin = min(pmin, v.tstate.NextPk)  // track cross-segment
			rmin = min(rmin, v.tstate.NextRid) // track cross-segment
			v.tstate.NextPk = pmin
			v.tstate.NextRid = rmin
			v.tstate.NRows = uint64(int64(v.tstate.NRows) - int64(nRowsDiff))
			nAborted += n
			nRowsDiff += r

			// check if state has changed
			if !v.IsEmpty() {
				v.setState(SegmentStateComplete)
				canMerge = true
			} else {
				v.setState(SegmentStateEmpty)
				canPrune = true
			}
		}
	}

	n, _ := j.tip.AbortActiveTx()
	nAborted += n
	pmin = min(pmin, j.tip.tstate.NextPk)  // track cross-segment
	rmin = min(rmin, j.tip.tstate.NextRid) // track cross-segment
	j.tip.tstate.NextPk = pmin
	j.tip.tstate.NextRid = rmin
	j.tip.tstate.NRows = uint64(int64(j.tip.tstate.NRows) - int64(nRowsDiff))

	// handle empty and merged segments
	if canPrune {
		j.prune()
	}

	// let the table handle mergable segments
	return nAborted, canMerge
}

// remove empty and merged tail segments
func (j *Journal) prune() {
	var k int
	for _, v := range j.tail {
		switch v.getState() {
		case SegmentStateEmpty, SegmentStateMerged:
			v.Close()
		default:
			j.tail[k] = v
			k++
		}
	}
	clear(j.tail[k:])
	j.tail = j.tail[:k]
}

// Merges results from a chain of journal segments under snapshot isolation
// rules. Guarantees to find the last visible version of each matching record
// or excludes the record when deleted. An epoch id (from table state,
// TableReader or IndexReader) ensures merged segments are skipped.
//
// Returns a stable read-only result containing (a) private copies of matching
// segment data packs with added selection vectors and (b) a global view
// of the tombstone. The tomb view is used during query processing to
// exclude deleted records from TableReader scans. The segment data pack
// matches function like regular table scan matches.
//
// The merge result is concurrency safe, i.e. readers can process a query
// without additional locks while a concurrent writer can add new data the
// journal in insert/update/delete calls.
func (j *Journal) Query(plan *query.QueryPlan, epoch uint32) *Result {
	// TODO: lock-free segment walk
	// - ideally only active segment requires lock
	// - use linked list for passive segments and optimistic locks
	// - requires max size array and rotation (is this desirable?)
	// - walk may conflict with rotation and free after merge (SegmentStateMerged)
	// j.mu.RLock()
	// defer j.mu.RUnlock()

	// alloc result and match bitset
	res := NewResult()
	bits := bitset.New(j.maxsz)

	// Single-pass merge
	// Walk segments in backwards order starting at tip. This ensures we first
	// find all snapshot visible tombstones (row ids) and use them to hide
	// deleted/replaced records from the query result as we walk segments.
	seg := j.tip
	for seg != nil {
		// skip merged and empty segments
		if seg.Id() <= epoch || seg.canDrop() {
			// plan.Log.Debugf("skip journal query segment %d", seg.Id())
			seg = seg.parent
			continue
		}

		// step 1: identify deleted records
		seg.MergeDeleted(res.tomb, plan.Snap)

		// step 2: match filters, apply snapshot visibility rules and tomb
		seg.Match(plan.Filters, plan.Snap, res.tomb, bits)

		// add segment to result if it has any match
		if bits.Any() {
			// plan.Log.Debugf("using journal segment %d with %d matches", seg.Id(), bits.Count())
			res.Append(seg, bits)
		}

		// next segment in history order
		seg = seg.parent
	}

	// free scratch
	bits.Close()

	return res
}

// Identify most recent visible row ids for primary keys in map. Walk segments in
// backwards order and keep max(rid). When the first rid is found in a visible
// tombstones or when an rid cannot be resolved return false. Only return true
// if all pks have been successfully resolved.
func (j *Journal) Lookup(ridMap map[uint64]uint64, snap *types.Snapshot) bool {
	// TODO: lock-free segment walk

	// stage 1: find highest visible rid for each pk
	// start at tip then load next segment in history order
	for seg := j.tip; seg != nil; seg = seg.parent {
		seg.LookupRids(ridMap, snap)
	}

	// check if all pks are resolved
	for _, rid := range ridMap {
		if rid == 0 {
			return false
		}
	}

	// stage 2: check tombs whether any found rid has been visibly deleted
	// again start at tip then load next segment in history order
	// stop at first deletion (our only use-case for lookup is for
	// update calls which fail when a user tries to update any deleted record)
	for seg := j.tip; seg != nil; seg = seg.parent {
		if !seg.CheckRids(ridMap, snap) {
			return false
		}
	}

	return true
}

func (j *Journal) ReplayWalRecord(ctx context.Context, rec *wal.Record, rd engine.TableReader) error {
	// j.log.Debugf("journal: apply %s", rec)
	switch rec.Type {
	case wal.RecordTypeCommit:
		j.CommitTx(rec.TxID)

	case wal.RecordTypeAbort:
		j.AbortTx(rec.TxID)

	case wal.RecordTypeCheckpoint:
		// each segment starts with a checkpoint
		j.tip.WithLSN(rec.Lsn)

	case wal.RecordTypeInsert:
		// read data header (first rid)
		buf := rec.Data[0]
		rid, n := num.Uvarint(buf)
		buf = buf[n:]
		var (
			count    uint64
			expectPk = j.tip.tstate.NextPk
		)

		// sanity check row id
		if j.tip.tstate.NextRid != rid {
			return fmt.Errorf("update: state rid %d does not match WAL record %d",
				j.tip.tstate.NextRid, rid)
		}

		// split buf into wire messages
		view, buf, _ := schema.NewView(j.schema).Cut(buf)
		for view.IsValid() {
			// check pk is correct
			pk := view.GetPk()
			if pk != expectPk {
				return fmt.Errorf("insert: unexpected pk=%d, expected=%d", pk, expectPk)
			}
			expectPk++

			// fail on overlow, should not happen
			if j.Capacity() == 0 {
				return fmt.Errorf("insert: journal overflow")
			}
			j.tip.InsertRecord(rec.TxID, rid, view.Bytes())
			rid++
			count++
			view, buf, _ = view.Cut(buf)
		}
		view.Reset(nil)
		if len(buf) > 0 {
			return fmt.Errorf("decode wal record: %d extra bytes", len(buf))
		}
		j.tip.tstate.NextPk = expectPk
		j.tip.tstate.NextRid = rid
		j.tip.tstate.NRows += count

	case wal.RecordTypeUpdate:
		buf := rec.Data[0]

		// changeset bitset
		csize := (j.schema.NumFields() + 7) / 8
		cset := bitset.NewFromBytes(buf[:csize], j.schema.NumFields())
		buf = buf[csize:]

		// peek first rowid
		rid, _ := num.Uvarint(buf)

		// sanity check row id
		if j.tip.tstate.NextRid != rid {
			return fmt.Errorf("update: state rid %d does not match WAL record %d",
				j.tip.tstate.NextRid, rid)
		}

		if cset.Count() == j.schema.NumFields() {
			// optimize, we have full records available
			var (
				view    = schema.NewView(j.schema)
				nextRid = j.tip.tstate.NextRid
			)
			for len(buf) > 0 {
				// decode rid
				rid, n := num.Uvarint(buf)
				buf = buf[n:]

				// decode ref
				ref, n := num.Uvarint(buf)
				buf = buf[n:]

				// decode record
				view, buf, _ = view.Cut(buf)

				// append to journal
				j.tip.UpdateRecord(rec.TxID, rid, ref, view.Bytes())
				nextRid++

				// ensure amount of updates fits into current journal tip
				if j.Capacity() == 0 {
					// should not happen
					return fmt.Errorf("update: num updates is larger than journal capacity")
				}
			}
			j.tip.tstate.NextRid = nextRid

		} else {
			// make change schema (for parsing change records)
			cids := make([]uint16, 0, cset.Count())
			cols := make([]int, 0, cset.Count())
			for i := range cset.Iterator() {
				cids = append(cids, j.schema.Fields[i].Id)
				cols = append(cols, i)
			}
			cschema, err := j.schema.SelectIds(cids...)
			if err != nil {
				// should not happen
				return fmt.Errorf("update: make change schema: %v", err)
			}

			// decode refs from WAL record and construct a query mask
			var (
				tmp  = buf
				refs = xroar.New()
				recs = make(map[uint64]int)
				view = schema.NewView(cschema)
				c    int
			)
			for len(buf) > 0 {
				// decode rid
				_, n := num.Uvarint(buf)
				buf = buf[n:]
				c += n
				// decode ref
				ref, n := num.Uvarint(buf)
				buf = buf[n:]
				c += n
				refs.Set(ref)
				recs[ref] = c
				// skip record
				view, buf, _ = view.Cut(buf)
				c += view.Len()
			}
			buf = tmp

			// ensure amount of updates fits into current journal tip
			if len(recs) > j.Capacity() {
				// should not happen
				return fmt.Errorf("update: num updates %d is larger than journal capacity %d",
					len(recs), j.Capacity())
			}

			// run query visiting all packs with matches
			rd.WithMask(refs, engine.ReadModeIncludeMask)
			for {
				pkg, err := rd.Next(ctx)
				if err != nil {
					return err
				}
				if pkg == nil {
					break
				}

				// materialize columns in the change set
				for _, col := range cols {
					pkg.MaterializeBlock(col)
				}

				// patch records
				for _, row := range pkg.Selected() {
					rid := pkg.RowId(int(row))
					ofs, ok := recs[rid]
					if !ok {
						// should not happen
						return fmt.Errorf("update: found invalid original rid=%d", rid)
					}

					// set values
					view.Reset(buf[ofs:])
					for i, col := range cols {
						val, _ := view.Get(i)
						pkg.Block(col).Set(int(row), val)
					}

					// remove patched update
					delete(recs, rid)
				}

				// append changed records to journal (will set new rowid, xid, ref)
				_, err = j.updatePackNoWal(pkg, rec.TxID)
				if err != nil {
					return fmt.Errorf("replay update: %v", err)
				}
			}

			// sanity check we have applied changes to all records found in WAL
			if len(recs) > 0 {
				// should not happen
				return fmt.Errorf("update: %d unhandled records", len(recs))
			}
		}

	case wal.RecordTypeDelete:
		buf := rec.Data[0]
		var nDeleted uint64
		for len(buf) > 0 && j.Capacity() > 0 {
			rid, n := num.Uvarint(buf)
			buf = buf[n:]

			// append to tomb, set xmax on rid when in tip segment
			j.tip.NotifyDelete(rec.TxID, rid)

			nDeleted++
		}

		// fail on overlow, should not happen
		if len(buf) > 0 {
			return fmt.Errorf("delete: journal overflow")
		}

		j.tip.tstate.NRows -= nDeleted
	}

	// try rotate segment once full
	j.rotateWhenFull()

	return nil
}
