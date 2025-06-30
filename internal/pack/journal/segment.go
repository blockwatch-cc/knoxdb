// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package journal

import (
	"unsafe"

	"blockwatch.cc/knoxdb/internal/bitset"
	"blockwatch.cc/knoxdb/internal/engine"
	"blockwatch.cc/knoxdb/internal/operator/filter"
	"blockwatch.cc/knoxdb/internal/pack"
	"blockwatch.cc/knoxdb/internal/pack/stats"
	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/internal/wal"
	"blockwatch.cc/knoxdb/internal/xroar"
	"blockwatch.cc/knoxdb/pkg/schema"
)

const (
	// block storage key suffixes (starting below pack xmeta)
	JournalXactKey uint16 = 0xFFFA
	TombKey        uint16 = 0xFFF9
)

type SegmentState byte

const (
	SegmentStateEmpty    SegmentState = iota // 0 no data, can be closed
	SegmentStateActive                       // 1 current write target
	SegmentStateWaiting                      // 2 immutable, waiting to complete
	SegmentStateComplete                     // 3 all tx closed, now mergable
	SegmentStateMerging                      // 4 merge in progress
	SegmentStateMerged                       // 5 merge complete, can be closed
)

var segmentSz = int(unsafe.Sizeof(Segment{}))

// Journal segment optimized for single writer tx. Only the current tx
// can add/commit/abort data. Concurrent queries hide uncommitted,
// deleted and future data based on snapshot isolation (xmin/xmax tx ids).
type Segment struct {
	data    *pack.Package      // full column data (insert/update) and tx metadata
	stats   *stats.Record      // column statistics (available when full: waiting++)
	tomb    *Tomb              // tombstone (compact delete records) with tx metadata
	meta    *schema.Meta       // cache for decoded row metadata
	lsn     wal.LSN            // WAL checkpoint, i.e. first LSN that holds data for this segment
	xact    uint64             // single uncommitted xid in this segment (0 = none)
	tstate  engine.ObjectState // table state (serial number generators, checkpoint LSN)
	aborted *bitset.Bitset     // lazy allocated bitset flagging aborted records
	parent  *Segment           // parent segment (can form a DAG in future versions)
	state   SegmentState       // segment lifecycle status

	// statistics
	xmin    uint64 // min xid in this segment (from ins/upd/del)
	xmax    uint64 // max xid in this segment (from ins/upd/del)
	rmin    uint64 // min rid in this segment (from ins/upd)
	rmax    uint64 // max rid in this segment (from ins/upd)
	nInsert uint32 // count of inserted records
	nUpdate uint32 // count of updated records
	nDelete uint32 // count of deleted records (excluding update replacements)
	nAbort  uint32 // count of aborted records (updates count only once)
}

func newSegment(s *schema.Schema, id uint32, maxsz int) *Segment {
	return &Segment{
		data: pack.New().
			WithSchema(s).
			WithMaxRows(maxsz).
			WithKey(id).
			WithVersion(id).
			Alloc(),
		tomb:  newTomb(maxsz),
		meta:  &schema.Meta{},
		xmin:  1<<64 - 1,
		xmax:  0,
		rmin:  1<<64 - 1,
		rmax:  0,
		state: SegmentStateActive,
		lsn:   0,
	}
}

func (s *Segment) Reset() {
	s.data.Clear()
	s.tomb.Reset()
	if s.aborted != nil {
		s.aborted.Close()
		s.aborted = nil
	}
	s.stats = nil
	s.meta = &schema.Meta{}
	s.xmin = 1<<64 - 1
	s.xmax = 0
	s.rmin = 1<<64 - 1
	s.rmax = 0
	s.state = SegmentStateEmpty
	s.lsn = 0
	s.tstate = engine.ObjectState{}
	s.xact = 0
}

func (s *Segment) Close() {
	s.data.Release()
	s.data = nil
	s.tomb.Clear()
	s.tomb = nil
	if s.aborted != nil {
		s.aborted.Close()
		s.aborted = nil
	}
	s.stats = nil
	s.meta = nil
	s.xmin = 1<<64 - 1
	s.xmax = 0
	s.rmin = 1<<64 - 1
	s.rmax = 0
	s.state = SegmentStateEmpty
	s.lsn = 0
	s.tstate = engine.ObjectState{}
	s.xact = 0
	s.parent = nil
}

func (s *Segment) WithParent(p *Segment) *Segment {
	s.parent = p
	return s
}

func (s *Segment) WithLSN(lsn wal.LSN) *Segment {
	s.lsn = lsn
	return s
}

func (s *Segment) WithState(v engine.ObjectState) *Segment {
	s.tstate = v
	return s
}

func (s *Segment) setCheckpoint(lsn wal.LSN) *Segment {
	s.tstate.Checkpoint = lsn
	return s
}

func (s *Segment) setState(state SegmentState) *Segment {
	s.state = state
	return s
}

func (s *Segment) getState() SegmentState {
	return s.state
}

func (s *Segment) is(state SegmentState) bool {
	return s.state == state
}

func (s *Segment) canDrop() bool {
	switch s.state {
	case SegmentStateEmpty, SegmentStateMerged:
		return true
	default:
		return false
	}
}

func (s *Segment) isWriteable() bool {
	switch s.state {
	case SegmentStateComplete, SegmentStateMerging, SegmentStateMerged:
		return false
	default:
		return true
	}
}

func (s *Segment) Id() uint32 {
	return s.data.Key()
}

func (s *Segment) LSN() wal.LSN {
	return s.lsn
}

func (s *Segment) State() engine.ObjectState {
	return s.tstate
}

func (s *Segment) Data() *pack.Package {
	return s.data
}

func (s *Segment) Tomb() *Tomb {
	return s.tomb
}

func (s *Segment) Aborted() *bitset.Bitset {
	return s.aborted
}

// Len returns the number of entries in this segment. Note this
// number is either data len and tomb len whichever is higher.
// This treats inserts, updates and deletes equally becuae updates
// write both data and tomb.
func (s *Segment) Len() int {
	return max(s.data.Len(), s.tomb.Len())
}

// A segment is considered full when either data or tombstone
// records exceed the segment's capacity.
func (s *Segment) IsFull() bool {
	return s.data.IsFull() || s.tomb.IsFull()
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
	return s.xact == 0
}

func (s *Segment) Size() int {
	return s.data.Size() + s.tomb.Size() + segmentSz
}

// ContainsTx returns true if rid is within segment bounds.
func (s *Segment) ContainsTx(xid uint64) bool {
	return s.Len() > 0 && xid-s.xmin <= s.xmax-s.xmin
}

// IsActiveTx returns true when xid has written data to this segment but
// has not committed or aborted yet.
func (s *Segment) IsActiveTx(xid uint64) bool {
	return s.xact == xid
}

// ContainsRid returns true if rid is within segment bounds. Rids are
// assigned sequentially and only one segment is active in the journal.
// This guarantees non-overlap in rid space between segments unless
// transactions have aborted. Then the same rid may appear multiple times
// in the same or across segments.
func (s *Segment) ContainsRid(rid uint64) bool {
	// fast range exclusion check
	return s.data.Len() > 0 && rid-s.rmin <= s.rmax-s.rmin
}

// append insert record
func (s *Segment) InsertRecord(xid, rid uint64, buf []byte) {
	// set metadata
	s.meta.Rid = rid
	s.meta.Ref = rid
	s.meta.Xmin = xid
	s.meta.Xmax = 0

	// append to data pack
	s.data.AppendWire(buf, s.meta)

	// update state
	s.NotifyInsert(xid, rid)
}

// append update record
func (s *Segment) UpdateRecord(xid, rid, ref uint64, buf []byte) {
	// set metadata
	s.meta.Rid = rid
	s.meta.Ref = ref
	s.meta.Xmin = xid
	s.meta.Xmax = 0

	// append to data pack
	s.data.AppendWire(buf, s.meta)

	// update state
	s.NotifyUpdate(xid, rid, ref)
}

func (s *Segment) NotifyInsert(xid, rid uint64) {
	// xid
	s.xact = xid

	// track xid range
	s.xmin = min(s.xmin, xid)
	s.xmax = max(s.xmax, xid)

	// track rid range
	s.rmin = min(s.rmin, rid)
	s.rmax = max(s.rmax, rid)

	// count
	s.nInsert++

	// extend aborted set if used
	if s.aborted != nil {
		s.aborted.Append(false)
	}
}

// append update
func (s *Segment) NotifyUpdate(xid, rid, ref uint64) {
	// update xid
	s.xact = xid

	// append tomb entry for ref record
	s.tomb.Append(xid, ref, false)

	// track xid range
	s.xmin = min(s.xmin, xid)
	s.xmax = max(s.xmax, xid)

	// track rid range
	s.rmin = min(s.rmin, rid)
	s.rmax = max(s.rmax, rid)

	// count
	s.nUpdate++

	// same segment replace by update
	if s.ContainsRid(ref) {
		s.SetXmax(xid, ref, false)
	}

	// extend aborted set if used
	if s.aborted != nil {
		s.aborted.Append(false)
	}
}

// append delete
func (s *Segment) NotifyDelete(xid, rid uint64) {
	// xid
	s.xact = xid

	// append tomb entry
	s.tomb.Append(xid, rid, true)

	// track xid range
	s.xmin = min(s.xmin, xid)
	s.xmax = max(s.xmax, xid)

	// same segment delete
	if s.ContainsRid(rid) {
		s.SetXmax(xid, rid, true)
		s.nDelete++
	}
}

// Sets xmax to xid for record rid.
func (s *Segment) SetXmax(xid, rid uint64, isDeleted bool) {
	if !s.ContainsRid(rid) {
		return
	}
	if s.nAbort == 0 {
		// without aborts rids are unique sorted (append only) and never reused
		idx := int(rid - s.rmin)
		s.data.Xmaxs().Set(idx, xid)
		if isDeleted {
			s.data.Dels().Set(idx)
			s.data.DelBlock().SetDirty()
		}
	} else {
		// with aborts, rids may appear multiple times, but only once in any
		// non-aborted record
		rids := s.data.RowIds().Slice()
		xmins := s.data.Xmins().Slice()
		var idx = -1
		for i, v := range rids {
			if v == rid && xmins[i] > 0 {
				idx = i
				break
			}
		}
		if idx >= 0 {
			s.data.Xmaxs().Set(idx, xid)
			if isDeleted {
				s.data.Dels().Set(idx)
				s.data.DelBlock().SetDirty()
			}
		}
	}
	s.xmin = min(s.xmin, xid)
	s.xmax = max(s.xmax, xid)
}

func (s *Segment) CommitTx(xid uint64) {
	// drop from active set (xid may not exist)
	if s.xact == xid {
		s.xact = 0
	}
}

func (s *Segment) AbortTx(xid uint64) {
	if s.xact != xid {
		return
	}

	// lazy allocate aborted set
	if s.aborted == nil {
		s.aborted = bitset.New(s.data.Cap()).Resize(s.data.Len())
	}

	// reset all metadata records where xmin or xmax = xid to zero
	var dirty bool
	if s.nInsert+s.nUpdate > 0 {
		// segment is sorted by xid (single writer tx), walk backwards & stop early
		xmins := s.data.Xmins().Slice()
		i := len(xmins) - 1
		for i >= 0 && xmins[i] == xid {
			xmins[i] = 0
			s.aborted.Set(i) // set aborted flag
			s.nAbort++       // count aborted insert + update rows
			dirty = true
			i--
		}
		if dirty {
			// explicitly set block dirty flags (we change raw vector content above)
			s.data.XminBlock().SetDirty()
			dirty = false
		}
	}

	// revert delete and update effects on xmax and del metadata
	if s.nDelete > 0 || s.nUpdate > 0 {
		xmaxs := s.data.Xmaxs().Slice()
		dels := s.data.Dels()
		for i, v := range xmaxs {
			if v == xid {
				xmaxs[i] = 0     // reset xmax effectively undeleting the record
				dels.Unset(i)    // unset deleted flag (safe for both delete and replace)
				s.aborted.Set(i) // set aborted flag
				dirty = true
			}
		}
		if dirty {
			// explicitly set block dirty flags (we change raw vector content above)
			s.data.XmaxBlock().SetDirty()
			s.data.DelBlock().SetDirty()
		}
		// update tomb, count aborted deletes (excluding replace by update)
		_, d := s.tomb.AbortTx(xid)
		s.nAbort += uint32(d)
	}

	// drop from active set
	s.xact = 0
}

func (s *Segment) AbortActiveTx() int {
	if s.xact == 0 || s.IsEmpty() {
		return 0
	}
	s.AbortTx(s.xact)
	return 1
}

// Match and exclude records not visible to this tx based on snapshot
// isolation rules.
//
// exclude records from
// - concurrent or future write tx
// - aborted transactions (xmin = 0)
// - visible tombstones (same snapshot isolation rules apply)
//
// The tomb argument provides a snapshot-consistent view over all
// cross-segment tombstones.
//
// Note match may run concurrent with inserts/updates/deletes and
// commit/abort calls.
func (s *Segment) Match(node *filter.Node, snap *types.Snapshot, tomb *xroar.Bitmap, bits *bitset.Bitset) {
	// reset bits
	bits.Zero().Resize(s.Data().Len())

	// check empty state and return early
	if s.state == SegmentStateEmpty {
		// log.Infof("> segment empty")
		return
	}

	// shortcut: skip when no records are visible to this snapshot (only future tx)
	if s.xmin >= snap.Xmax {
		// log.Infof("> segment xmin[%d] >= snap.Xmax[%d]", s.xmin, snap.Xmax)
		return
	}

	// quick check on stats for any potential match (active segment has no stats)
	if s.stats != nil {
		if !stats.Match(node, s.stats) {
			// log.Infof("> no stats match")
			return
		}
		bits = filter.Match(node, s.data, s.stats, bits)
	} else {
		// pass nil interface instead of typed nil when stats don't exist yet
		bits = filter.Match(node, s.data, nil, bits)
	}

	// stop early on empty match
	if bits.None() {
		// log.Infof("> empty match")
		return
	}
	// log.Infof("> match with %d results", bits.Count())

	// remove aborted records from match
	if s.aborted != nil {
		bits.AndNot(s.aborted)
		if bits.None() {
			// log.Infof("> empty match after abort")
			return
		}
	}

	// apply snapshot isolation rules; a record is visible iff
	// - xmin is self AND xmax is null
	// - xmin is committed AND xmax is null OR xmax is from another uncommitted tx
	// conversely a record is not visible iff
	// - xmin > self (record from a future tx)
	// - xmax > 0 and xmax < xmin or xmax = self (visible tombstone)
	// - xmin = 0 (record was aborted)
	switch {
	case s.IsDone() && s.xmax < snap.Xmin:
		// No open XID, all xids are behind horizon (required for historic read query)
		// All data in this segment was committed or aborted. We can skip
		// snapshot checks. Note the segment can still be active at this time.

		// remove in-segment deletes (note del is only set for true deletes, not updates)
		if s.nDelete > 0 {
			bits.AndNot(s.data.Dels().Writer())
			if bits.None() {
				// log.Infof("> empty match after dels")
				return
			}
		}

		// remove records deleted outside this segment and records replaced
		// by in-segment updates using the global tomb view
		if tomb.ContainsRange(s.rmin, s.rmax) {
			// translate rid to position (since we lack a xroar seekable iterator,
			// we walk bits and check membership)
			rids := s.data.RowIds()
			tmax := tomb.Max()
			for i := range bits.Iterator() {
				rid := rids.Get(i)

				// stop early when we find an rid that is larger than any deleted rid
				if rid > tmax {
					break
				}

				// unset match bit when this record was deleted
				if tomb.Contains(rid) {
					bits.Unset(i)
				}
			}
		}
		// log.Infof("> quick match with %d results", bits.Count())

	default:
		// Open XID or query horizon within segment range
		// The segment contains (a) uncommitted, (b) aborted, (c) future [unlikely
		// due to single write tx] or (d) deleted records. Deleted records can either
		// be same segment deletes or cross-segment deletes. Aborts are already removed
		// above, so we don't worry about them here.
		if tomb.ContainsRange(s.rmin, s.rmax) {
			// 1. remove visible deleted records, i.e. xmax is set and visible
			// 2. remove invisible new records, i.e. xmin > xown
			rids := s.data.RowIds()
			xmins := s.data.Xmins()
			for i := range bits.Iterator() {
				if !snap.IsVisible(xmins.Get(i)) || tomb.Contains(rids.Get(i)) {
					bits.Unset(i)
				}
			}
			// log.Infof("> slow match with tomb with %d results", bits.Count())
		} else {
			// 1. remove invisible new records, i.e xmin > xown
			xmins := s.data.Xmins()
			for i := xmins.Len() - 1; i >= 0; i-- {
				xid := xmins.Get(i)

				// stop early when xid is no longer above snapshot, this works because
				// xids are sequential in a segment
				if xid < snap.Xmin {
					break
				}

				// reset match when xid is invisible
				if !snap.IsVisible(xid) {
					bits.Unset(i)
				}
			}
			// log.Infof("> slow match without tomb with %d results", bits.Count())
		}
	}
}

// MergeDeleted collects row ids of deleted records into a bitset considering
// snapshot isolation visibility rules. A row id is considered deleted when
// it was either replaced in an update or deleted explicitly and the corresponding
// transaction is visible to the snapshot.
func (s *Segment) MergeDeleted(set *xroar.Bitmap, snap *types.Snapshot) {
	// check empty state and return early
	if s.state == SegmentStateEmpty || s.IsEmpty() {
		return
	}

	// shortcut: can skip this segment when no tombstones are visible
	// to the snapshot, i.e. the segment contains only future tx
	if s.xmin >= snap.Xmax {
		return
	}

	// optimization: if the segment is complete (no more open tx) and all xids are
	// visible to the snapshot, we can merge the entire tombstone
	if s.IsDone() && (s.xmax < snap.Xmin || snap.Safe) {
		set.Or(s.tomb.rids)
		return
	}

	// merge only visible xids into set
	s.tomb.MergeVisible(set, snap)
}

// func (s *Segment) Store(ctx context.Context, bucket store.Bucket) error {
// 	switch s.state {
// 	case SegmentStateFlushing:
// 		// write full segment to disk
// 		s.data.WithStats()
// 		if _, err := s.data.StoreToDisk(ctx, bucket); err != nil {
// 			return err
// 		}

// 		// generate stats record after store
// 		s.stats = stats.NewRecordFromPack(s.data, 0)
// 		s.data.CloseStats()

// 		// write tomb data to disk
// 		if s.tomb.dirty {
// 			if err := s.tomb.Store(ctx, bucket, s.Id()); err != nil {
// 				return err
// 			}
// 		}

// 		// write segment xact to disk
// 		var err error
// 		xkey := pack.EncodeBlockKey(s.data.Key(), JournalXactKey)
// 		if s.xact.IsEmpty() {
// 			err = bucket.Delete(xkey)
// 		} else {
// 			err = bucket.Put(xkey, s.xact.Bytes())
// 		}
// 		if err != nil {
// 			return err
// 		}

// 		// update segment state
// 		switch {
// 		case s.IsEmpty():
// 			s.SetState(SegmentStateEmpty)
// 		case s.IsDone():
// 			s.SetState(SegmentStateCompleting)
// 		default:
// 			s.SetState(SegmentStateFlushed)
// 		}
// 		return nil

// 	case SegmentStateCompleting:
// 		// write dirty metadata
// 		s.data.WithStats()
// 		if _, err := s.data.StoreToDisk(ctx, bucket); err != nil {
// 			return err
// 		}

// 		// update meta stats after store
// 		s.stats.Update(s.data)
// 		s.data.CloseStats()

// 		// write tomb data to disk
// 		if s.tomb.dirty {
// 			if err := s.tomb.Store(ctx, bucket, s.Id()); err != nil {
// 				return err
// 			}
// 		}

// 		// write segment xact to disk
// 		var err error
// 		xkey := pack.EncodeBlockKey(s.data.Key(), JournalXactKey)
// 		if s.xact.IsEmpty() {
// 			err = bucket.Delete(xkey)
// 		} else {
// 			err = bucket.Put(xkey, s.xact.Bytes())
// 		}
// 		if err != nil {
// 			return err
// 		}

// 		// update segment state
// 		if s.IsEmpty() {
// 			s.SetState(SegmentStateEmpty)
// 		} else {
// 			s.SetState(SegmentStateComplete)
// 		}
// 		return nil

// 	case SegmentStateEmpty, SegmentStateMerged:
// 		// remove all segment data
// 		xkey := pack.EncodeBlockKey(s.data.Key(), JournalXactKey)
// 		if err := bucket.Delete(xkey); err != nil {
// 			return err
// 		}
// 		if err := s.tomb.Remove(ctx, bucket, s.Id()); err != nil {
// 			return err
// 		}
// 		return s.data.RemoveFromDisk(ctx, bucket)

// 	default:
// 		return nil
// 	}
// }

// func loadSegment(ctx context.Context, s *schema.Schema, bucket store.Bucket, id uint32, maxsz int) (*Segment, error) {
// 	seg := &Segment{
// 		xact: xroar.New(),
// 		data: pack.New().
// 			WithSchema(s).
// 			WithMaxRows(maxsz).
// 			WithKey(id),
// 		tomb:   newTomb(maxsz),
// 		state:  SegmentStateEmpty,
// 		xmin: 1<<64 - 1,
// 		rmin: 1<<64 - 1,
// 	}

// 	// load data (nocache, size unknown)
// 	if _, err := seg.data.LoadFromDisk(ctx, bucket, nil, maxsz); err == nil {
// 		return nil, err
// 	}

// 	// FIXME: pack.Stats is empty here, need to initialize from blocks
// 	// regenerate stats after load
// 	seg.stats = stats.NewRecordFromPack(seg.data, 0)

// 	// find min and max xid, rid and skip zeros (aborted xid's)
// 	for _, v := range seg.data.Xmins().Iterator() {
// 		if v == 0 {
// 			seg.nAbort++
// 			continue
// 		}
// 		seg.xmin = min(seg.xmin, v)
// 		seg.xmax = max(seg.xmax, v)
// 	}
// 	for _, v := range seg.data.Xmaxs().Iterator() {
// 		if v == 0 {
// 			continue
// 		}
// 		seg.xmin = min(seg.xmin, v)
// 		seg.xmax = max(seg.xmax, v)
// 	}
// 	for _, v := range seg.data.RowIds().Iterator() {
// 		seg.rmin = min(seg.rmin, v)
// 		seg.rmax = max(seg.rmax, v)
// 	}

// 	// count inserts and updates (including aborted ins/upd)
// 	rids := seg.data.RowIds()
// 	refs := seg.data.RefIds()
// 	for i, v := range rids.Iterator() {
// 		if v == refs.Get(i) {
// 			seg.nInsert++
// 		} else {
// 			seg.nUpdate++
// 		}
// 	}

// 	// load xact (may not exist when empty)
// 	xkey := pack.EncodeBlockKey(id, JournalXactKey)
// 	buf := bucket.Get(xkey)
// 	if buf != nil {
// 		seg.xact = xroar.NewFromBytes(bytes.Clone(buf))
// 	}

// 	// load tomb
// 	if err := seg.tomb.Load(ctx, bucket, id); err != nil {
// 		return nil, err
// 	}

// 	// count deletes
// 	seg.nDelete = uint32(seg.tomb.Len())

// 	// set state after load
// 	if !seg.IsEmpty() {
// 		seg.state = SegmentStateComplete
// 	}

// 	return seg, nil
// }
