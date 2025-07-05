// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package journal

import (
	"context"
	"fmt"
	"slices"

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
	maxseg int            // max number of immutable segments // TODO: unused, refactor
	log    log.Logger     // journal logger instance
}

func NewJournal(s *schema.Schema, maxsz, maxseg int) *Journal {
	return &Journal{
		schema: s,
		key:    []byte(s.Name() + "_journal"),
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

func (j *Journal) rotateAndCheckpoint() error {
	// rotate segment when full
	if !j.rotate() {
		return nil
	}

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

func (j *Journal) rotate() bool {
	if !j.tip.IsFull() {
		return false
	}

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
	for i, v := range j.tail {
		if v.is(SegmentStateMerging) {
			return nil, engine.ErrAgain
		}
		if v.is(SegmentStateComplete) {
			// flip state
			v.setState(SegmentStateMerging)

			// determine follower segment's checkpoint
			if i < len(j.tail)-1 {
				v.setCheckpoint(j.tail[i+1].lsn)
			} else {
				v.setCheckpoint(j.tip.lsn)
			}
			return v, nil
		}
	}
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
	j.tail = slices.DeleteFunc(j.tail, func(s *Segment) bool {
		ok := s.canDrop()
		if ok {
			s.Close()
		}
		return ok
	})

	// unlink tail segment's parent
	if len(j.tail) > 0 {
		j.tail[0].parent = nil
	} else {
		j.tip.parent = nil
	}
}

func (j *Journal) AbortMerged(s *Segment) {
	// reset segment state
	s.setState(SegmentStateComplete)
}

func (j *Journal) CommitTx(xid types.XID) bool {
	if j.tip.ContainsTx(xid) {
		j.tip.CommitTx(xid)
	}

	// commit tx across segments
	for _, v := range j.tail {
		if v.is(SegmentStateWaiting) && v.ContainsTx(xid) {
			v.CommitTx(xid)
		}
	}

	return j.compact()
}

func (j *Journal) AbortTx(xid types.XID) bool {
	// update tip
	if j.tip.ContainsTx(xid) {
		j.tip.AbortTx(xid)
	}

	// abort tx across segments
	for _, v := range j.tail {
		if v.is(SegmentStateWaiting) && v.ContainsTx(xid) {
			v.AbortTx(xid)
		}
	}

	return j.compact()
}

// called once to finalize wal replay
func (j *Journal) AbortActiveTx() (int, bool) {
	n := j.tip.AbortActiveTx()

	for _, v := range j.tail {
		if v.is(SegmentStateWaiting) {
			n += v.AbortActiveTx()
		}
	}

	return n, j.compact()
}

func (j *Journal) compact() bool {
	// remove empty segments and prepare merge for complete segments
	var (
		k            int
		haveMergable bool
	)
	for i, s := range j.tail {
		if s.canDrop() {
			s.Close()
			j.tail[i] = nil
			continue
		}

		// advance wait state to complete
		if s.is(SegmentStateWaiting) && s.IsDone() {
			s.setState(SegmentStateComplete)
			haveMergable = true
		} else {
			haveMergable = haveMergable || s.is(SegmentStateComplete)
		}
		j.tail[k] = s
		k++
	}
	clear(j.tail[k:])
	j.tail = j.tail[:k]

	return haveMergable
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
	node := plan.Filters
	snap := plan.Snap

	// Single-pass merge
	// Walk segments in backwards order starting at tip. This ensures we first
	// find all snapshot visible tombstones (row ids) and use them to hide
	// deleted/replaced records from the query result as we walk segments.
	seg := j.tip
	for seg != nil {
		// skip merged and empty segments
		if seg.Id() <= epoch || seg.canDrop() {
			plan.Log.Debugf("skip journal query segment %d", seg.Id())
			seg = seg.parent
			continue
		}

		// step 1: identify deleted records
		seg.MergeDeleted(res.tomb, snap)

		// step 2: match filters, apply snapshot visibility rules and tomb
		seg.Match(node, snap, res.tomb, bits.Zero())

		// add segment to result if it has any match
		if bits.Any() {
			plan.Log.Debugf("using journal segment %d with %d matches", seg.Id(), bits.Count())
			res.Append(seg, bits)
		}

		// next segment in history order
		seg = seg.parent
	}

	// free scratch
	bits.Close()

	return res
}

// Identify most recent visible row ids for primary keys in list. Walk journal backwards
// and keep max(rid), ignore tombstones.
func (j *Journal) Lookup(ridMap map[uint64]uint64, snap *types.Snapshot) {
	seg := j.tip
	for seg != nil {
		// merge max(rid)
		seg.LookupRids(ridMap, snap)

		// next segment in history order
		seg = seg.parent
	}
}

func (j *Journal) ReplayWalRecord(ctx context.Context, rec *wal.Record, rd engine.TableReader) error {
	switch rec.Type {
	case wal.RecordTypeCommit:
		j.CommitTx(rec.TxID)

	case wal.RecordTypeAbort:
		j.AbortTx(rec.TxID)

	case wal.RecordTypeCheckpoint:
		// each segment starts with a checkpoint
		j.log.Debugf("journal: apply %s", rec)
		j.tip.WithLSN(rec.Lsn)

	case wal.RecordTypeInsert:
		j.log.Debugf("journal: apply %s", rec)
		// read data header (first rid)
		buf := rec.Data[0]
		rid, n := num.Uvarint(buf)
		buf = buf[n:]
		var (
			count    uint64
			expectPk uint64 = j.tip.tstate.NextPk
		)

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
		j.tip.tstate.NextRid = rid + 1
		j.tip.tstate.NRows += count

	case wal.RecordTypeUpdate:
		j.log.Debugf("journal: apply %s", rec)
		buf := rec.Data[0]

		// changeset bitset
		csize := (j.schema.NumFields() + 7) / 8
		cset := bitset.NewFromBytes(buf[:csize], j.schema.NumFields())
		buf = buf[csize:]

		// first rowid
		rid, n := num.Uvarint(buf)
		buf = buf[n:]

		// sanity check row id
		if j.tip.tstate.NextRid != rid {
			// should not happen
			return fmt.Errorf("update: state rid %d does not match WAL record %d",
				j.tip.tstate.NextRid, rid)
		}

		// make change schema (for parsing change records)
		cids := make([]uint16, 0, cset.Count())
		cols := make([]int, 0, cset.Count())
		for i := range cset.Iterator() {
			cids = append(cids, j.schema.Field(i).Id())
			cols = append(cols, i)
		}
		cschema, err := j.schema.SelectFieldIds(cids...)
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
			ref, n := num.Uvarint(buf)
			buf = buf[n:]
			c += n
			refs.Set(ref)
			recs[ref] = c
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

	case wal.RecordTypeDelete:
		j.log.Debugf("journal: apply %s", rec)
		buf := rec.Data[0]
		var nRows uint64
		for len(buf) > 0 && j.Capacity() > 0 {
			rid, n := num.Uvarint(buf)
			buf = buf[n:]

			// append to tomb, set xmax on rid when in tip segment
			j.tip.NotifyDelete(rec.TxID, rid)

			nRows++
		}

		// fail on overlow, should not happen
		if len(buf) > 0 {
			return fmt.Errorf("delete: journal overflow")
		}

		j.tip.tstate.NRows -= nRows
	}

	// try rotate segment once full
	j.rotate()

	return nil
}
