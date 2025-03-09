// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package journal

import (
	"context"
	"reflect"
	"slices"

	"blockwatch.cc/knoxdb/internal/bitset"
	"blockwatch.cc/knoxdb/internal/pack"
	"blockwatch.cc/knoxdb/internal/pack/match"
	"blockwatch.cc/knoxdb/internal/pack/stats"
	"blockwatch.cc/knoxdb/internal/query"
	"blockwatch.cc/knoxdb/internal/store"
	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/internal/xroar"
	"blockwatch.cc/knoxdb/pkg/schema"
	"blockwatch.cc/knoxdb/pkg/util"
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
		xact: xroar.NewBitmap(),
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

// A segment is considered full when the sum of data and tombstone
// records exceeds the segment's capacity.
func (s *Segment) IsFull() bool {
	return s.data.IsFull() || s.Len() >= s.data.Cap()
}

// A segment is considered empty when it either contains no data
// or all records originate from aborted transactions.
func (s *Segment) IsEmpty() bool {
	if s.Len() == 0 {
		return true
	}
	return s.nInsert+s.nUpdate+s.nDelete-s.nAbort == 0
}

// IsDone returns true when all transactions who wrote records into
// this segment have either committed or aborted.
func (s *Segment) IsDone() bool {
	if s.Len() == 0 {
		return false
	}
	return s.xact.IsEmpty()
}

func (s *Segment) HeapSize() int {
	return s.xact.Size() + s.data.HeapSize() + s.tomb.HeapSize() + segmentSz
}

func (s *Segment) ContainsTx(xid uint64) bool {
	return s.xact.Contains(xid)
}

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
	s.tomb.Append(pk, xid, ref, true)

	// track xid range
	s.minXid = min(s.minXid, xid)
	s.maxXid = max(s.maxXid, xid)

	// track rid range
	s.minRid = max(s.minRid, rid)
	s.maxRid = max(s.maxRid, rid)

	// count
	s.nUpdate++
}

// append delete
func (s *Segment) Delete(pk, xid, rid uint64) {
	// xid
	s.xact.Set(xid)

	// append tomb entry
	s.tomb.Append(pk, xid, rid, false)

	// track xid range
	s.minXid = min(s.minXid, xid)
	s.maxXid = max(s.maxXid, xid)

	// count
	s.nDelete++
}

// assumes rids are unique sorted (append only) and never reused (post abort)
func (s *Segment) SetXmax(rid, xid uint64) {
	idx := int(rid - s.minRid)
	s.data.Xmaxs().Uint64().Set(idx, xid)
	s.data.Dels().Bool().Set(idx)
	s.data.Xmaxs().SetDirty()
	s.data.Dels().SetDirty()
	s.minXid = min(s.minXid, xid)
	s.maxXid = max(s.maxXid, xid)
}

func (s *Segment) CommitTx(xid uint64) {
	// drop from active set (xid may not exist)
	s.xact.Remove(xid)
}

func (s *Segment) AbortTx(xid uint64) {
	// reset all metadata records where xmin or xmax = xid to zero
	var dirty bool
	xmins := s.data.Xmins().Uint64().Slice()
	for i, v := range xmins {
		if v == xid {
			xmins[i] = 0
			s.nAbort++ // count aborted insert + update rows
			dirty = true
		}
	}
	if dirty {
		s.data.Xmins().SetDirty()
		dirty = false
	}

	xmaxs := s.data.Xmaxs().Uint64().Slice()
	dels := s.data.Dels().Bool()
	for i, v := range xmaxs {
		if v == xid {
			xmaxs[i] = 0
			dels.Clear(i)
			dirty = true
		}
	}
	if dirty {
		s.data.Xmaxs().SetDirty()
		s.data.Dels().SetDirty()
	}

	// update tomb, count aborted true deletes (exclude replace by update)
	s.nAbort += uint32(s.tomb.AbortTx(xid))

	// drop from active set
	s.xact.Remove(xid)
}

func (s *Segment) AbortActiveTx() int {
	// reset all metadata records where xmin or xmax is in xact to zero
	var dirty bool
	xmins := s.data.Xmins().Uint64().Slice()
	for i, v := range xmins {
		if s.xact.Contains(v) {
			xmins[i] = 0
			s.nAbort++ // count aborted insert + update rows
			dirty = true
		}
	}
	if dirty {
		s.data.Xmins().SetDirty()
		dirty = false
	}

	xmaxs := s.data.Xmaxs().Uint64().Slice()
	dels := s.data.Dels().Bool()
	for i, v := range xmaxs {
		if s.xact.Contains(v) {
			xmaxs[i] = 0
			dels.Clear(i)
			dirty = true
		}
	}
	if dirty {
		s.data.Xmaxs().SetDirty()
		s.data.Dels().SetDirty()
	}

	// update tomb, count aborted true deletes (exclude replace by update)
	s.nAbort += uint32(s.tomb.AbortActiveTx(s.xact))

	// clear xact
	n := s.xact.GetCardinality()
	s.xact.Reset()
	return n
}

func (s *Segment) Store(ctx context.Context, bucket store.Bucket) error {
	switch s.state {
	case SegmentStateFlushing:
		// write full segment to disk
		if _, err := s.data.StoreToDisk(ctx, bucket); err != nil {
			return err
		}

		// generate stats record after store
		s.stats = stats.NewRecordFromPack(s.data, 0)
		s.data.FreeAnalysis()

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
			err = bucket.Put(xkey, s.xact.ToBuffer())
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
		if _, err := s.data.StoreToDisk(ctx, bucket); err != nil {
			return err
		}

		// update meta stats after store
		s.stats.Update(s.data)
		s.data.FreeAnalysis()

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
			err = bucket.Put(xkey, s.xact.ToBuffer())
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
		xact: xroar.NewBitmap(),
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

	// regenerate stats after load
	seg.stats = stats.NewRecordFromPack(seg.data, 0)

	// find min and max xid, rid and skip zeros (aborted xid's)
	for _, v := range seg.data.Xmins().Uint64().Slice() {
		if v == 0 {
			seg.nAbort++
			continue
		}
		seg.minXid = min(seg.minXid, v)
		seg.maxXid = max(seg.maxXid, v)
	}
	for _, v := range seg.data.Xmaxs().Uint64().Slice() {
		if v == 0 {
			continue
		}
		seg.minXid = min(seg.minXid, v)
		seg.maxXid = max(seg.maxXid, v)
	}
	for _, v := range seg.data.RowIds().Uint64().Slice() {
		seg.minRid = min(seg.minRid, v)
		seg.maxRid = max(seg.maxRid, v)
	}

	// count inserts and updates (including aborted ins/upd)
	rids := seg.data.RowIds().Uint64().Slice()
	refs := seg.data.RefIds().Uint64().Slice()
	for i := range rids {
		if rids[i] == refs[i] {
			seg.nInsert++
		} else {
			seg.nUpdate++
		}
	}

	// load xact (may not exist when empty)
	xkey := pack.EncodeBlockKey(id, JournalXactKey)
	buf := bucket.Get(xkey)
	if buf != nil {
		seg.xact = xroar.FromBufferWithCopy(buf)
	}

	// load tomb
	if err := seg.tomb.Load(ctx, bucket, id); err != nil {
		return nil, err
	}

	// count true deletes and aborted deletes
	for _, s := range seg.tomb.Stones() {
		seg.nDelete += uint32(util.Bool2int(!s.upd))
		seg.nAbort += uint32(util.Bool2int(!s.upd && s.xid == 0))
	}

	return seg, nil
}

// match and exclude records not visible to this tx based on snapshot
// isolation rules. considers both xmin (creation xid) and xmax (deletion xid)
// and also excludes records from aborted transactions (xmin  = 0)
func (s *Segment) Match(node *query.FilterTreeNode, snap *types.Snapshot, bits *bitset.Bitset) *bitset.Bitset {
	// check empty state and return early
	if s.state == SegmentStateEmpty {
		return nil
	}

	// shortcut: can skip segment when no matches visible to this snapshot (only future tx)
	if s.minXid >= snap.Xmax {
		return nil
	}

	// quick check on stats for any potential match (active segment has no stats)
	if !stats.Match(node, s.stats) {
		return nil
	}

	// run a vector match
	bits = match.MatchTree(node, s.data, s.stats, bits)

	// stop early on empty match
	if bits.None() {
		return bits
	}

	// apply snapshot isolation rules; a record is visible iff
	// - xmin is self AND xmax is null
	// - xmin is committed AND xmax is null OR xmax is from another uncommitted tx
	switch {
	case s.IsDone() && s.maxXid < snap.Xmin:
		// OPTIMIZATION
		// skip full snapshot checks when segment is behind horizon, i.e it
		// only contains data from txn that committed before the snapshot was created,
		// hence all matches are valid

		// remove deleted records (xmax[n] > 0) and records from aborted
		// transactions (xmin[n] == 0)
		for i, l := 0, s.data.Len(); i < l; i++ {
			if !bits.IsSet(i) {
				continue
			}
			if s.data.Xmax(i) == 0 && s.data.Xmin(i) > 0 {
				continue
			}
			bits.Clear(i)
		}

	default:
		// general vectorized snapshot isolation check
		// - xmin[n] < snap.xmax
		// - xmin[n] NIN snap.xact
		for i, l := 0, s.data.Len(); i < l; i++ {
			if !bits.IsSet(i) {
				continue
			}

			// remove deleted records, i.e. xmax is set and visible
			if xmax := s.data.Xmax(i); xmax > 0 && snap.IsVisible(xmax) {
				bits.Clear(i)
				continue
			}

			// remove inserted records when xmin is not visible
			// (includes records from aborted transactions i.e. xmin = 0)
			if !snap.IsVisible(s.data.Xmin(i)) {
				bits.Clear(i)
			}
		}
	}

	return bits
}

// PrepareMerge constructs info in deleted records outside the segment. A merge algo
// can use tombMask [rid] to locate tuples for deletion and tombMap [rid->xid]
// to assign xmax metadata.
func (s *Segment) PrepareMerge() (tombMap map[uint64]uint64, tombMask []uint64, inJournalDeletes bool) {
	if s.tomb.Len() == 0 {
		return
	}

	// determine boundary (first journal rid), i.e. any row > boundary
	// has not yet been merged into the table, hence any delete/update
	// for such row has already written xmax inside the journal pack
	var bound uint64 = 1<<64 - 1
	if s.data.Len() > 0 {
		bound = s.data.RowId(0)
	}

	// collect out-of-journal tombstones
	tombMap = make(map[uint64]uint64, s.tomb.Len())
	tombMask = make([]uint64, 0, s.tomb.Len())
	for _, stone := range s.tomb.stones {
		// skip aborted data
		if stone.xid == 0 {
			continue
		}

		// skip in-journal upd/del
		if stone.rid >= bound {
			inJournalDeletes = true
			continue
		}

		// build mapping and mask
		tombMap[stone.rid] = stone.xid
		tombMask = append(tombMask, stone.rid)
	}
	slices.Sort(tombMask)
	return
}
