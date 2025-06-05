// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package journal

import (
	"bytes"
	"context"
	"reflect"

	"blockwatch.cc/knoxdb/internal/bitset"
	"blockwatch.cc/knoxdb/internal/operator/filter"
	"blockwatch.cc/knoxdb/internal/pack"
	"blockwatch.cc/knoxdb/internal/pack/stats"
	"blockwatch.cc/knoxdb/internal/query"
	"blockwatch.cc/knoxdb/internal/store"
	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/internal/xroar"
	"blockwatch.cc/knoxdb/pkg/schema"
)

const (
	// block key suffixes (starting below pack xmeta)
	JournalXactKey uint16 = 0xFFFA
	TombKey        uint16 = 0xFFF9
)

type SegmentState byte

const (
	SegmentStateEmpty      SegmentState = iota // no data, can be closed
	SegmentStateActive                         // current write target
	SegmentStateFlushing                       // immutable, writing to disk
	SegmentStateFlushed                        // saved to disk and checkpointed
	SegmentStateCompleting                     // no more open txn, writing dirty metadata
	SegmentStateComplete                       // all data safe on disk, is mergable
	SegmentStateMerging                        // merge in progress
	SegmentStateMerged                         // merge complete, can be closed
)

var segmentSz = int(reflect.TypeOf(Segment{}).Size())

type Segment struct {
	xact  *xroar.Bitmap // uncommitted tx ids in this segment
	data  *pack.Package // full column data (insert/update) and tx metadata
	stats *stats.Record // column statistics (available when stored)
	tomb  *Tomb         // tombstone (compact delete records) with tx metadata
	meta  *schema.Meta  // cache for decoded row metadata
	state SegmentState  // segment lifecycle status

	// statistics
	minXid  uint64 // min xid in this segment (from ins/upd/del)
	maxXid  uint64 // max xid in this segment (from ins/upd/del)
	minRid  uint64 // min rid in this segment (from ins/upd)
	maxRid  uint64 // max rid in this segment (from ins/upd)
	nInsert uint32 // count of inserted records
	nUpdate uint32 // count of updated records
	nDelete uint32 // count of deleted records (excluding update replacements)
	nAbort  uint32 // count of aborted records (updates count only once)
}

func newSegment(s *schema.Schema, id uint32, maxsz int) *Segment {
	return &Segment{
		xact: xroar.New(),
		data: pack.New().
			WithSchema(s).
			WithMaxRows(maxsz).
			WithKey(id).
			Alloc(),
		tomb:   newTomb(maxsz),
		meta:   &schema.Meta{},
		minXid: 1<<64 - 1,
		maxXid: 0,
		minRid: 1<<64 - 1,
		maxRid: 0,
		state:  SegmentStateEmpty,
	}
}

func (s *Segment) Reset() {
	s.xact.Reset()
	s.data.Clear()
	s.tomb.Reset()
	s.stats = nil
	s.meta = &schema.Meta{}
	s.minXid = 1<<64 - 1
	s.maxXid = 0
	s.minRid = 1<<64 - 1
	s.maxRid = 0
	s.state = SegmentStateEmpty
}

func (s *Segment) Close() {
	s.xact = nil
	s.data.Release()
	s.data = nil
	s.tomb.Clear()
	s.tomb = nil
	s.stats = nil
	s.meta = nil
	s.minXid = 1<<64 - 1
	s.maxXid = 0
	s.minRid = 1<<64 - 1
	s.maxRid = 0
	s.state = SegmentStateEmpty
}

func (s *Segment) Id() uint32 {
	return s.data.Key()
}

func (s *Segment) Data() *pack.Package {
	return s.data
}

func (s *Segment) Tomb() *Tomb {
	return s.tomb
}

// Len returns the number of entries in this segment. Note this
// number can be higher than max capacity (up to 2x capacity)
// because updates count twice since they write data and tomb.
func (s *Segment) Len() int {
	return s.data.Len() + s.tomb.Len()
}

// A segment is considered full when either data or tombstone
// records exceed the segment's capacity.
func (s *Segment) IsFull() bool {
	return s.data.IsFull() || s.tomb.Len() >= s.data.Cap()
}

// A segment is considered empty when it either contains no data
// or all records originate from aborted transactions.
func (s *Segment) IsEmpty() bool {
	if s.Len() == 0 {
		return true
	}
	return s.nInsert+s.nUpdate+s.nDelete-s.nAbort == 0
}

// IsDone returns true when all transactions that wrote records into
// this segment have either committed or aborted.
func (s *Segment) IsDone() bool {
	if s.Len() == 0 {
		return false
	}
	return s.xact.IsEmpty()
}

func (s *Segment) Size() int {
	return s.xact.Size() + s.data.Size() + s.tomb.Size() + segmentSz
}

func (s *Segment) ContainsTx(xid uint64) bool {
	return s.xact.Contains(xid)
}

// ContainsRid returns true if rid is in segment bounds. Rids are
// assigned sequentially and only one segment is active in the journal.
// Once inactive a segment remains immutable and cannot be written to
// again even though transactions may still abort. This guarantees
// non-overlap in rid space between segments.
func (s *Segment) ContainsRid(rid uint64) bool {
	// fast range exclusion check
	return rid-s.minRid > s.maxRid-s.minRid
}

func (s *Segment) SetState(state SegmentState) {
	s.state = state
}

// append insert record
func (s *Segment) Insert(xid, rid uint64, buf []byte) {
	// xid
	s.xact.Set(xid)

	// set metadata
	s.meta.Rid = rid
	s.meta.Ref = rid
	s.meta.Xmin = xid
	s.meta.Xmax = 0

	// append to data pack
	s.data.AppendWire(buf, s.meta)

	// track xid range
	s.minXid = min(s.minXid, xid)
	s.maxXid = max(s.maxXid, xid)

	// track rid range
	s.minRid = max(s.minRid, rid)
	s.maxRid = max(s.maxRid, rid)

	// count
	s.nInsert++
}

// append update
func (s *Segment) Update(pk, xid, rid, ref uint64, buf []byte) {
	// update xid
	s.xact.Set(xid)

	// set metadata
	s.meta.Rid = rid
	s.meta.Ref = ref
	s.meta.Xmin = xid
	s.meta.Xmax = 0

	// append to data pack
	s.data.AppendWire(buf, s.meta)

	// append tomb entry for ref record
	s.tomb.Append(xid, ref)

	// track xid range
	s.minXid = min(s.minXid, xid)
	s.maxXid = max(s.maxXid, xid)

	// track rid range
	s.minRid = max(s.minRid, rid)
	s.maxRid = max(s.maxRid, rid)

	// count
	s.nUpdate++
	if s.ContainsRid(ref) {
		// same segment delete via update
		s.nDelete++
	}
}

// append delete
func (s *Segment) Delete(pk, xid, rid uint64) {
	// xid
	s.xact.Set(xid)

	// append tomb entry
	s.tomb.Append(xid, rid)

	// track xid range
	s.minXid = min(s.minXid, xid)
	s.maxXid = max(s.maxXid, xid)

	// count
	s.nDelete++
}

// assumes rids are unique sorted (append only) and never reused (post abort)
func (s *Segment) SetXmax(rid, xid uint64) {
	idx := int(rid - s.minRid)
	s.data.Xmaxs().Set(idx, xid)
	s.data.Dels().Set(idx)
	s.data.DelBlock().SetDirty()
	s.minXid = min(s.minXid, xid)
	s.maxXid = max(s.maxXid, xid)
}

func (s *Segment) CommitTx(xid uint64) {
	// drop from active set (xid may not exist)
	s.xact.Unset(xid)
}

func (s *Segment) AbortTx(xid uint64) {
	// reset all metadata records where xmin or xmax = xid to zero
	var dirty bool
	xmins := s.data.Xmins().Slice()
	for i, v := range xmins {
		if v == xid {
			xmins[i] = 0
			s.nAbort++ // count aborted insert + update rows
			dirty = true
		}
	}
	if dirty {
		s.data.XminBlock().SetDirty()
		dirty = false
	}

	xmaxs := s.data.Xmaxs().Slice()
	dels := s.data.Dels()
	for i, v := range xmaxs {
		if v == xid {
			xmaxs[i] = 0
			dels.Unset(i)
			dirty = true
		}
	}
	if dirty {
		s.data.XmaxBlock().SetDirty()
		s.data.DelBlock().SetDirty()
	}

	// update tomb, count aborted deletes (including replace by update)
	s.nAbort += uint32(s.tomb.AbortTx(xid))

	// drop from active set
	s.xact.Unset(xid)
}

func (s *Segment) AbortActiveTx() int {
	// reset all metadata records where xmin or xmax is in xact to zero
	var dirty bool
	xmins := s.data.Xmins().Slice()
	for i, v := range xmins {
		if s.xact.Contains(v) {
			xmins[i] = 0
			s.nAbort++ // count aborted insert + update rows
			dirty = true
		}
	}
	if dirty {
		s.data.XminBlock().SetDirty()
		dirty = false
	}

	xmaxs := s.data.Xmaxs().Slice()
	dels := s.data.Dels()
	for i, v := range xmaxs {
		if s.xact.Contains(v) {
			xmaxs[i] = 0
			dels.Unset(i)
			dirty = true
		}
	}
	if dirty {
		s.data.XmaxBlock().SetDirty()
		s.data.DelBlock().SetDirty()
	}

	// update tomb, count aborted deletes (including replace by update)
	s.nAbort += uint32(s.tomb.AbortActiveTx(s.xact))

	// clear xact
	n := s.xact.Count()
	s.xact.Reset()
	return n
}

func (s *Segment) Store(ctx context.Context, bucket store.Bucket) error {
	switch s.state {
	case SegmentStateFlushing:
		// write full segment to disk
		s.data.WithStats()
		if _, err := s.data.StoreToDisk(ctx, bucket); err != nil {
			return err
		}

		// generate stats record after store
		s.stats = stats.NewRecordFromPack(s.data, 0)
		s.data.CloseStats()

		// write tomb data to disk
		if s.tomb.dirty {
			if err := s.tomb.Store(ctx, bucket, s.Id()); err != nil {
				return err
			}
		}

		// write segment xact to disk
		var err error
		xkey := pack.EncodeBlockKey(s.data.Key(), JournalXactKey)
		if s.xact.IsEmpty() {
			err = bucket.Delete(xkey)
		} else {
			err = bucket.Put(xkey, s.xact.Bytes())
		}
		if err != nil {
			return err
		}

		// update segment state
		switch {
		case s.IsEmpty():
			s.SetState(SegmentStateEmpty)
		case s.IsDone():
			s.SetState(SegmentStateCompleting)
		default:
			s.SetState(SegmentStateFlushed)
		}
		return nil

	case SegmentStateCompleting:
		// write dirty metadata
		s.data.WithStats()
		if _, err := s.data.StoreToDisk(ctx, bucket); err != nil {
			return err
		}

		// update meta stats after store
		s.stats.Update(s.data)
		s.data.CloseStats()

		// write tomb data to disk
		if s.tomb.dirty {
			if err := s.tomb.Store(ctx, bucket, s.Id()); err != nil {
				return err
			}
		}

		// write segment xact to disk
		var err error
		xkey := pack.EncodeBlockKey(s.data.Key(), JournalXactKey)
		if s.xact.IsEmpty() {
			err = bucket.Delete(xkey)
		} else {
			err = bucket.Put(xkey, s.xact.Bytes())
		}
		if err != nil {
			return err
		}

		// update segment state
		if s.IsEmpty() {
			s.SetState(SegmentStateEmpty)
		} else {
			s.SetState(SegmentStateComplete)
		}
		return nil

	case SegmentStateEmpty, SegmentStateMerged:
		// remove all segment data
		xkey := pack.EncodeBlockKey(s.data.Key(), JournalXactKey)
		if err := bucket.Delete(xkey); err != nil {
			return err
		}
		if err := s.tomb.Remove(ctx, bucket, s.Id()); err != nil {
			return err
		}
		return s.data.RemoveFromDisk(ctx, bucket)

	default:
		return nil
	}
}

func loadSegment(ctx context.Context, s *schema.Schema, bucket store.Bucket, id uint32, maxsz int) (*Segment, error) {
	seg := &Segment{
		xact: xroar.New(),
		data: pack.New().
			WithSchema(s).
			WithMaxRows(maxsz).
			WithKey(id),
		tomb:   newTomb(maxsz),
		state:  SegmentStateEmpty,
		minXid: 1<<64 - 1,
		minRid: 1<<64 - 1,
	}

	// load data (nocache, size unknown)
	if _, err := seg.data.LoadFromDisk(ctx, bucket, nil, maxsz); err == nil {
		return nil, err
	}

	// FIXME: pack.Stats is empty here, need to initialize from blocks
	// regenerate stats after load
	seg.stats = stats.NewRecordFromPack(seg.data, 0)

	// find min and max xid, rid and skip zeros (aborted xid's)
	for _, v := range seg.data.Xmins().Iterator() {
		if v == 0 {
			seg.nAbort++
			continue
		}
		seg.minXid = min(seg.minXid, v)
		seg.maxXid = max(seg.maxXid, v)
	}
	for _, v := range seg.data.Xmaxs().Iterator() {
		if v == 0 {
			continue
		}
		seg.minXid = min(seg.minXid, v)
		seg.maxXid = max(seg.maxXid, v)
	}
	for _, v := range seg.data.RowIds().Iterator() {
		seg.minRid = min(seg.minRid, v)
		seg.maxRid = max(seg.maxRid, v)
	}

	// count inserts and updates (including aborted ins/upd)
	rids := seg.data.RowIds()
	refs := seg.data.RefIds()
	for i, v := range rids.Iterator() {
		if v == refs.Get(i) {
			seg.nInsert++
		} else {
			seg.nUpdate++
		}
	}

	// load xact (may not exist when empty)
	xkey := pack.EncodeBlockKey(id, JournalXactKey)
	buf := bucket.Get(xkey)
	if buf != nil {
		seg.xact = xroar.NewFromBytes(bytes.Clone(buf))
	}

	// load tomb
	if err := seg.tomb.Load(ctx, bucket, id); err != nil {
		return nil, err
	}

	// count deletes
	seg.nDelete = uint32(seg.tomb.Len())

	return seg, nil
}

// Match and exclude records not visible to this tx based on snapshot
// isolation rules. Considers both xmin (creation xid) and xmax (deletion xid)
// and also excludes records from aborted transactions (xmin  = 0) as well
// as records deleted in other journal segments.
func (s *Segment) Match(node *query.FilterTreeNode, snap *types.Snapshot, tomb *xroar.Bitmap, bits *bitset.Bitset) {
	// check empty state and return early
	if s.state == SegmentStateEmpty {
		return
	}

	// shortcut: skip when no records are visible to this snapshot (only future tx)
	if s.minXid >= snap.Xmax {
		return
	}

	// quick check on stats for any potential match (active segment has no stats)
	if s.stats != nil && !stats.Match(node, s.stats) {
		return
	}

	// run a vector match
	bits = filter.MatchTree(node, s.data, s.stats, bits)

	// stop early on empty match
	if bits.None() {
		return
	}

	// check if this segment contains any records that have visible tombstones
	hasTombstones := tomb.ContainsRange(s.minRid, s.maxRid)

	// apply snapshot isolation rules; a record is visible iff
	// - xmin is self AND xmax is null
	// - xmin is committed AND xmax is null OR xmax is from another uncommitted tx
	switch {
	case s.IsDone() && s.maxXid < snap.Xmin:
		// OPTIMIZATION
		// skip full snapshot checks when segment is behind horizon, i.e it
		// only contains data from txn that committed before the snapshot was created,
		// hence all matches are valid

		// remove deleted (xmax[n] > 0) and aborted records
		rids := s.data.RowIds()
		xmins := s.data.Xmins()
		switch {
		case hasTombstones && s.nAbort > 0:
			for i := range bits.Iterator() {
				if tomb.Contains(rids.Get(i)) || xmins.Get(i) == 0 {
					bits.Unset(i)
				}
			}
		case hasTombstones:
			// optimize potential: rids are sequential, may stop early when rid > tomb_max
			for i := range bits.Iterator() {
				if tomb.Contains(rids.Get(i)) {
					bits.Unset(i)
				}
			}
		case s.nAbort > 0:
			// optimize potential: vector check for == 0
			for i := range bits.Iterator() {
				if xmins.Get(i) == 0 {
					bits.Unset(i)
				}
			}
		}

	default:
		// general vectorized snapshot isolation check
		// - xmin[n] < snap.xmax
		// - xmin[n] NIN snap.xact
		rids := s.data.RowIds()
		xmins := s.data.Xmins()
		// xmaxs := s.data.Xmaxs()
		if hasTombstones {
			// 1. remove deleted records, i.e. xmax is set and visible
			// 2. remove new records when xmin is not yet visible
			//    (includes records from aborted transactions i.e. xmin = 0)
			for i := range bits.Iterator() {
				if tomb.Contains(rids.Get(i)) || !snap.IsVisible(xmins.Get(i)) {
					bits.Unset(i)
				}
			}
		} else {
			// 1. only remove new records when xmin is not yet visible
			//    (includes records from aborted transactions i.e. xmin = 0)
			for i := range bits.Iterator() {
				if !snap.IsVisible(xmins.Get(i)) {
					bits.Unset(i)
				}
			}
		}
	}
}

// MergeDeleted collects row ids of deleted records into a bitset considering
// snapshot isolation visibility rules. A row id is considered deleted when
// it was either replaced in an update or deleted explicitly and the corresponding
// transaction is visible to the snapshot.
func (s *Segment) MergeDeleted(set *xroar.Bitmap, snap *types.Snapshot) {
	// check empty state and return early
	if s.state == SegmentStateEmpty {
		return
	}

	// shortcut: can skip this segment when no tombstones are visible
	// to the snapshot, i.e. the segment contains only future tx
	if s.minXid >= snap.Xmax {
		return
	}

	// optimization: if the segment is complete (no more open tx) and all xids are
	// visible to the snapshot, we can merge the entire tombstone
	if s.IsDone() && s.maxXid < snap.Xmin {
		set.Or(s.tomb.rids)
		return
	}

	// merge only visible xids into set
	s.tomb.MergeVisible(set, snap)
}

// TODO: needs refactoring / cross-check
// - inJournalDeletes flag can be tracked in segment (careful with aborts)

// PrepareMerge constructs info in deleted records outside the segment. A merge algo
// can use tombMask [rid] to locate tuples for deletion and tombMap [rid->xid]
// to assign xmax metadata.
func (s *Segment) PrepareMerge() (stones Tombstones, tombMask *xroar.Bitmap, inJournalDeletes bool) {
	if s.tomb.Len() == 0 {
		return
	}

	// determine boundary (first journal rid), i.e. any row > boundary
	// has not yet been merged into the table, hence any delete/update
	// for such row has already written xmax inside the journal pack

	// prepare tombstones
	stones = s.tomb.stones
	tombMask = s.tomb.rids
	for _, stone := range stones {
		// detect in-segment deletes
		if stone.Rid >= s.minRid && stone.Rid <= s.maxRid {
			inJournalDeletes = true
			// 	continue
		}

	}
	// util.Sort(tombMask, 0)
	return
}
