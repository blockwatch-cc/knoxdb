// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package journal

import (
	"context"
	"encoding/binary"
	"fmt"
	"slices"

	"blockwatch.cc/knoxdb/internal/bitset"
	"blockwatch.cc/knoxdb/internal/pack"
	"blockwatch.cc/knoxdb/internal/query"
	"blockwatch.cc/knoxdb/internal/store"
	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/schema"
)

// TODO
// - split and roll segements automatically, return to caller with (n, ok?, err)
// - refactor segment.PrepareMerge
// - design when/how to store a segment and replace it with loaded/compressed blocks

// Outside caller must split message batches into individual operations and
// must break logical operations into physical append-only updates:
//
// Insert: append record (xmin = xid)
// Update: append delete record (old rid: xmax = xid)
//         append insert record (new rid: xmin = xid, xref)
//	       - same xid update after insert/update: replace new rid insert on merge
// Delete: append delete record (old rid) xmax = xid
//	       - same xid delete after insert/update: xmin = xmax = xid => can skip/clear
//
// Special case handling (same tx merge not yet implemented)
//
// FIXME: this comment is no longer correct:
// Any sequence of insert/update/delete to the same pk in the same tx
// is minified on merge combining all updates into a single event:
//
// - ins+del becomes a noop
// - ins+upd becomes an insert of the last updated row version
// - upd+upd becomes a single update using the lastest row version.

var LE = binary.LittleEndian

type Journal struct {
	name    string
	schema  *schema.Schema // data schema
	view    *schema.View   // data view
	active  *Segment       // active head segment used for writing
	passive []*Segment     // immutable tail segments waiting for completion and flush
	maxsz   int            // max number of records before segment freeze
	maxseg  int            // max number of immutable segments
}

func NewJournal(s *schema.Schema, maxsz, maxseg int) *Journal {
	return &Journal{
		name:    s.Name() + "_journal",
		schema:  s,
		view:    schema.NewView(s),
		active:  newSegment(s, 0, maxsz),
		passive: make([]*Segment, 0, maxseg),
		maxsz:   maxsz,
		maxseg:  maxseg,
	}
}

func (j *Journal) Len() int {
	n := j.active.Len()
	for _, v := range j.passive {
		n += v.Len()
	}
	return n
}

// number of records that can be inserted before active segment runs full
func (j *Journal) Capacity() int {
	return j.maxsz - j.active.Len()
}

func (j *Journal) MaxSize() int {
	return j.maxsz
}

func (j *Journal) Schema() *schema.Schema {
	return j.schema
}

func (j *Journal) Active() *Segment {
	return j.active
}

func (j *Journal) Segments() []*Segment {
	return append([]*Segment{j.active}, j.passive...)
}

func (j *Journal) Size() (sz int) {
	sz = j.active.Size()
	for _, v := range j.passive {
		sz += v.Size()
	}
	return
}

func (j *Journal) Reset() {
	j.active.Reset()
	for _, v := range j.passive {
		v.Close()
	}
	clear(j.passive)
	j.passive = j.passive[:0]
}

func (j *Journal) Close() {
	j.schema = nil
	j.view = nil
	j.active.Close()
	j.active = nil
	for i := range j.passive {
		j.passive[i].Close()
		j.passive[i] = nil
	}
	j.passive = j.passive[:0]
	j.passive = nil
}

// appends single record, WAL replay requires batch size matches segment capacity
func (j *Journal) Insert(xid, rid uint64, buf []byte) {
	// insert rid
	j.active.Insert(xid, rid, buf)
	if j.active.IsFull() {
		j.active.SetState(SegmentStateFlushing)
	}
}

func (j *Journal) Update(xid, rid, pk, ref uint64, buf []byte) {
	// insert rid and delete ref
	j.active.Update(pk, xid, rid, ref, buf)

	// update xmax when replaced record (pk/ref) is in active journal
	// don't write passive segments to prevent state and merge conflicts
	if j.active.ContainsRid(ref) {
		j.active.SetXmax(ref, xid)
	}

	if j.active.IsFull() {
		j.active.SetState(SegmentStateFlushing)
	}
}

func (j *Journal) Delete(xid, rid, pk uint64) {
	j.active.Delete(pk, xid, rid)

	// update xmax when deleted record (pk/rid) is in active journal
	// don't write passive segments to prevent state and merge conflicts
	if j.active.ContainsRid(rid) {
		j.active.SetXmax(rid, xid)
	}

	if j.active.IsFull() {
		j.active.SetState(SegmentStateFlushing)
	}
}

// returns the next journal segment that is ready to merge
func (j *Journal) MergeNext() *Segment {
	if len(j.passive) > 0 && j.passive[0].state == SegmentStateComplete {
		j.passive[0].SetState(SegmentStateMerging)
		return j.passive[0]
	}
	return nil
}

func (j *Journal) Flush(ctx context.Context, tx store.Tx) error {
	bucket := tx.Bucket([]byte(j.name))
	if bucket == nil {
		return store.ErrNoBucket
	}
	bucket.FillPercent(1.0)

	// flush passive segments with dirty data
	for _, s := range j.passive {
		if err := s.Store(ctx, bucket); err != nil {
			return err
		}
	}

	// remove empty and merged segments
	j.passive = slices.DeleteFunc(j.passive, func(s *Segment) bool {
		switch s.state {
		case SegmentStateEmpty, SegmentStateMerged:
			s.Close()
			return true
		default:
			return false
		}
	})

	// rotate active segment if it contains data
	if !j.active.IsEmpty() {
		// store active segment
		if err := j.active.Store(ctx, bucket); err != nil {
			return err
		}

		// TODO: free unused memory
		// - tombstones-only: can we safely release data pack here?
		// - data-only: can we safely relese tomb memory here?

		// append active segment to immutable list
		j.passive = append(j.passive, j.active)

		// create new active segment
		j.active = newSegment(j.schema, j.active.Id()+1, j.maxsz)
	}

	return nil
}

func (j *Journal) CommitTx(xid uint64) {
	if j.active.ContainsTx(xid) {
		j.active.CommitTx(xid)
	}
	for _, v := range j.passive {
		if !v.ContainsTx(xid) {
			continue
		}
		v.CommitTx(xid)

		// update segment state
		if v.IsDone() && v.state == SegmentStateFlushed {
			if v.IsEmpty() {
				v.SetState(SegmentStateEmpty)
			} else {
				v.SetState(SegmentStateCompleting)
			}
		}
	}
}

func (j *Journal) AbortTx(xid uint64) {
	if j.active.ContainsTx(xid) {
		j.active.AbortTx(xid)
	}
	for _, v := range j.passive {
		if !v.ContainsTx(xid) {
			continue
		}
		v.AbortTx(xid)

		// update segment state
		if v.IsDone() && v.state == SegmentStateFlushed {
			if v.IsEmpty() {
				v.SetState(SegmentStateEmpty)
			} else {
				v.SetState(SegmentStateCompleting)
			}
		}
	}
}

func (j *Journal) AbortActiveTx() (n int) {
	j.active.AbortActiveTx()
	for _, v := range j.passive {
		n += v.AbortActiveTx()
		if v.state == SegmentStateFlushed {
			if v.IsEmpty() {
				v.SetState(SegmentStateEmpty)
			} else {
				v.SetState(SegmentStateCompleting)
			}
		}
	}
	return
}

// Loads all journal segments found on disk
func (j *Journal) Load(ctx context.Context, tx store.Tx) error {
	bucket := tx.Bucket([]byte(j.name))
	if bucket == nil {
		return store.ErrNoBucket
	}

	// identify segment ids to load from all keys in bucket
	segIds := make([]uint32, 0)
	var last uint32
	err := bucket.ForEach(func(k, v []byte) error {
		id, _ := pack.DecodeBlockKey(k)
		if id == last {
			return nil
		}
		segIds = append(segIds, id)
		last = id
		return nil
	})
	if err != nil {
		return err
	}

	// load segments from disk
	for _, id := range segIds {
		seg, err := loadSegment(ctx, j.schema, bucket, id, j.maxsz)
		if err != nil {
			return fmt.Errorf("loading journal segment %d: %v", id, err)
		}
		seg.SetState(SegmentStateFlushed)
		j.passive = append(j.passive, seg)
	}

	// update active segment id
	j.active.data.WithKey(last + 1)

	return nil
}

// Matches all journal segments against the query and applies snapshot isolation
// rules to find the last visible version of each matching record for the current
// transaction. Returns a stable read-only result snapshot pointing to matching records
// across journal segments. This result can be used concurrently with insert/update/delete
// calls as such calls append new journal records but don't change existing records
// or their order.
func (j *Journal) Query(node *query.FilterTreeNode, snap *types.Snapshot) *Result {
	// TODO: lock-free segment walk
	// - ideally only active segment requires lock
	// - use linked list for passive segments and optimistic locks
	// - requires max size array and rotation (is this desirable?)
	// - walk conflicts with segment rotation and free after merge (SegmentStateMerged)
	// j.mu.RLock()
	// defer j.mu.RUnlock()

	// alloc result and match bitset
	res := NewResult()
	bits := bitset.New(j.maxsz)

	// Walk segments in LIFO order starting at active segment, this ensures we
	// find all deleted records (row ids) with the effect that we only keep the
	// most recent visible update of a record in the query result set.
	for _, seg := range slices.Backward(append(j.passive, j.active)) {
		// First, identify deleted records under snapshot isolation by merging visible
		// tombstones across all segments. This ensures we know which journal records
		// are actually active for a given xid and which records have been deleted
		// even if deletion happened in a younger segments. We will use this info to
		// exclude deleted row ids from table scans and exclude them from the current
		// and from older journal segments' match results. Recall, (a) we walk the journal
		// backwards in time and (b) passive/immutable segments will not have xmax set
		// if an update/delete happend after a segment was frozen. Because xmax is
		// not written in such cases, a regular SI visibility check alone is insufficient.
		seg.MergeDeleted(res.tomb, snap)

		// match filters & apply snapshot visibility and tomb
		seg.Match(node, snap, res.tomb, bits.Zero())

		// add segment to result if it has any match
		res.Append(seg, bits)
	}

	// free scratch
	bits.Close()

	return res
}
