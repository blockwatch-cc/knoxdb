// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package journal

import (
	"context"
	"sync"

	"blockwatch.cc/knoxdb/internal/engine"
	"blockwatch.cc/knoxdb/internal/pack/stats"
	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/internal/wal"
	"blockwatch.cc/knoxdb/pkg/schema"
	"github.com/echa/log"
)

// In-memory journal for table insert, update and delete
// - journal acts as overlay to table storage
// - fix max size segments with data pack and tomb
// - row-id centric
//   - each row-id represents a unique record version
//   - updates always produce new row ids
//   - visible (committed) rowids are never reused
//   - invisible (aborted) rowids are rolled back (safe due to single writer tx)
//
// Operations
// - insert: inserts new records from batch or pack with or without WAL
// - import: imports records from packs without WAL writes
// - update: inserts post-image records, adds pre-image rids to tombstone
// - delete: adds row ids to tombstone
// - merge: merges one journal segment into table storage
// - query: matches query conditions against MVCC visible journal segments
//
// Queries
//   - merge-on-query: merge journal data and tomb with table query result
//   - uses snapshot isolation to hide invisible records and deletes
//   - journal query produces a journal result which is a list of segment
//     packs with selection vectors
//
// Merge
//   - only full segments and with no open tx can be merged
//   - background task handles oldest mergable segment
//   - on success, the oldest segment is removed atomically
//   - on fail, already written data remains invisible (new storage key versions)
//     and will be overwritten next merge round
//
// Recover
// - journal data is saved to WAL and replayed on startup
//
// # Schema versioning
//
// When the table schema changes within a transaction (i.e. user calls
// the ALTER TABLE method) the current active journal segment is closed
// and a new segment using the new schema version is created. Future
// writes are expected to use the new schema when encoding batches.
// Aborting a schema changing transaction will mark all journal segments
// using the new schema as prunable after the abort was processed.
// Replaying WAL records will preserve the order of schema change and
// journal inserts/updates.
//
// Invariants:
// - schema is constant for all records in a batch
// - schema is constant for all records in a pack (and journal segment)
// - each schema change increases the version number
// - schema fields are never removed, field ids are not reused
// - schema updates precede inserts in WAL LSN order
type Journal struct {
	resolver   schema.SchemaResolver // finds schema versions
	wal        *wal.Wal              // wal reference
	key        []byte                // storage bucket name
	id         uint64                // table id (tagged hash)
	tip        *Segment              // active head segment used for writing
	tail       []*Segment            // immutable tail segments waiting for completion and flush
	maxsz      int                   // max number of records before segment freeze
	maxseg     int                   // max number of immutable segments
	log        log.Logger            // journal logger instance
	recordPool sync.Pool             // metadata records cache
}

type Option func(*Journal)

func WithMaxSize(s int) Option {
	return func(j *Journal) {
		j.maxsz = s
	}
}

func WithMaxSegments(s int) Option {
	return func(j *Journal) {
		j.maxseg = s
		j.tail = make([]*Segment, 0, s)
	}
}

func WithSchema(s *schema.Schema) Option {
	return func(j *Journal) {
		j.tip = newSegment(s, 0, j.maxsz)
		j.key = []byte(s.Name + "_journal")
		j.id = types.TaggedHash(types.ObjectTagTable, s.Name)
	}
}

func WithLogger(l log.Logger) Option {
	return func(j *Journal) {
		j.log = l
	}
}

func WithWal(w *wal.Wal) Option {
	return func(j *Journal) {
		j.wal = w
	}
}

func WithState(s engine.ObjectState) Option {
	return func(j *Journal) {
		j.ResetState(s)
	}
}

func WithResolver(r schema.SchemaResolver) Option {
	return func(j *Journal) {
		j.resolver = r
	}
}

func NewJournal(opts ...Option) *Journal {
	j := &Journal{
		tail:   make([]*Segment, 0),
		maxsz:  2048,
		maxseg: 16,
		log:    log.Disabled,
	}
	for _, o := range opts {
		o(j)
	}
	return j
}

func (j *Journal) ResetState(s engine.ObjectState) {
	s.Epoch++
	j.tip.WithState(s)
	j.tip.data.WithVersion(uint32(s.Epoch)).WithKey(uint32(s.Epoch))
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
	j.tip.Close()
	j.tip = nil
	for i := range j.tail {
		j.tail[i].Close()
		j.tail[i] = nil
	}
	j.tail = j.tail[:0]
	j.tail = nil
	j.wal = nil
	j.resolver = nil
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
	j.tip.stats = j.makeStats(j.tip)

	// append to immutable list
	j.tail = append(j.tail, j.tip)

	// create new segment and link to parent
	j.tip = newSegment(j.tip.data.Schema(), j.tip.Id()+1, j.maxsz).
		WithParent(j.tip).
		WithState(j.tip.tstate)

	return true
}

func (j *Journal) makeStats(s *Segment) *stats.Record {
	var rec *stats.Record

	// try reuse a recently dropped record
	if irec := j.recordPool.Get(); irec != nil {
		rec = irec.(*stats.Record)
	}

	// reuse only when schema matches
	if rec != nil && rec.SchemaId == s.data.Schema().Hash {
		return rec.Update(s.data.BuildStats(), 0)
	}

	// otherwise build a new record
	return stats.NewRecordFromPack(s.data.BuildStats(), 0)
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

	j.log.Debugf("journal: removing merged segment %d", s.Id())

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
			if v.stats != nil {
				j.recordPool.Put(v.stats)
			}
			v.Close()
		default:
			j.tail[k] = v
			k++
		}
	}
	clear(j.tail[k:])
	j.tail = j.tail[:k]
}
