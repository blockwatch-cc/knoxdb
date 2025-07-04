// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package journal

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"sort"

	"blockwatch.cc/knoxdb/internal/pack"
	"blockwatch.cc/knoxdb/internal/store"
	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/internal/xroar"
	"blockwatch.cc/knoxdb/pkg/num"
)

type Tomb struct {
	rids   *xroar.Bitmap // rids with tombstones (updated or deleted records)
	stones Tombstones    // list of tombstones sorted by xid
	sz     int           // max number of tombstones
	xmax   types.XID     // max xid, used to determine sort/insertion policy
	dirty  bool
}

type Tombstone struct {
	Xid   types.XID // transaction that deleted this record
	Rid   uint64    // unique row id of the deleted record
	IsDel bool      // whether the tomstone is for a true delete or update
}

// kept sorted by xid
type Tombstones []Tombstone

func newTomb(sz int) *Tomb {
	return &Tomb{sz: sz}
}

func (t *Tomb) Clear() {
	t.rids = nil
	t.stones = nil
	t.dirty = false
	t.xmax = 0
}

func (t *Tomb) Reset() {
	if t.rids != nil {
		t.rids.Reset()
	}
	t.stones = t.stones[:0]
	t.dirty = false
	t.xmax = 0
}

func (t *Tomb) Len() int {
	return len(t.stones)
}

func (t *Tomb) IsFull() bool {
	return len(t.stones) >= t.sz
}

func (t *Tomb) RowIds() *xroar.Bitmap {
	return t.rids
}

func (t *Tomb) Stones() []Tombstone {
	return t.stones
}

func (t *Tomb) Size() int {
	return 16*len(t.stones) + t.rids.Size() + 44
}

func (t *Tomb) Append(xid types.XID, rid uint64, isDelete bool) {
	if t.rids == nil {
		t.rids = xroar.New()
		t.stones = make(Tombstones, 0, t.sz)
	}
	if len(t.stones) > 0 && xid < t.xmax {
		// insert
		i := len(t.stones) - 1
		for i > 0 && t.stones[i].Xid > xid {
			i--
		}
		t.stones = append(t.stones, Tombstone{})
		copy(t.stones[i+1:], t.stones[i:])
		t.stones[i] = Tombstone{Xid: xid, Rid: rid, IsDel: isDelete}
	} else {
		// append
		t.stones = append(t.stones, Tombstone{Xid: xid, Rid: rid, IsDel: isDelete})
	}
	t.xmax = max(t.xmax, xid)
	t.rids.Set(rid)
	t.dirty = true
}

func (t *Tomb) CommitTx(xid types.XID) {
	// noop
}

func (t *Tomb) AbortTx(xid types.XID) (int, int) {
	l := len(t.stones)
	idx := sort.Search(l, func(i int) bool {
		return t.stones[i].Xid >= xid
	})
	if idx == l {
		return 0, 0
	}
	var n, d int
	for i := range t.stones[idx:] {
		s := &t.stones[idx+i]
		if s.Xid != xid {
			break
		}
		s.Xid = 0
		t.rids.Unset(s.Rid)
		n++
		if s.IsDel {
			d++
		}
	}

	// remove aborted stones
	if n > 0 {
		copy(t.stones[idx:], t.stones[idx+n:])
		t.stones = t.stones[:len(t.stones)-n]
		if l := len(t.stones); l > 0 {
			t.dirty = true
			t.xmax = t.stones[l-1].Xid
		} else {
			t.dirty = false
			t.xmax = 0
		}
	}

	// return number or aborted true deletes and total number of aborted tombstones
	return n, d
}

func (t *Tomb) AbortActiveTx(xid types.XID) (int, int) {
	var (
		n, d, i int
		l       = len(t.stones)
	)
	// skip non-matching stones
	for i < l && t.stones[i].Xid < xid {
		i++
	}

	// reset matching stones
	for i < l && t.stones[i].Xid == xid {
		t.stones[i].Xid = 0
		t.rids.Unset(t.stones[i].Rid)
		i++
		n++
		if t.stones[i].IsDel {
			d++
		}
	}

	// remove aborted stones
	if n > 0 {
		var j int
		for i, v := range t.stones {
			if v.Xid == 0 {
				continue
			}
			t.stones[j] = t.stones[i]
			j++
		}
		t.stones = t.stones[:len(t.stones)-n]
		if l := len(t.stones); l > 0 {
			t.dirty = true
			t.xmax = t.stones[l-1].Xid
		} else {
			t.dirty = false
			t.xmax = 0
		}
	}

	return n, d
}

func (t *Tomb) MergeVisible(set *xroar.Bitmap, snap *types.Snapshot) {
	if len(t.stones) == 0 {
		return
	}

	// are no tombstones visible to this tx?
	if t.stones[0].Xid >= snap.Xmax {
		return
	}

	// add all deleted rids
	set.Or(t.rids)

	// are all tombstones visible to this tx?
	if t.xmax < snap.Xmin || snap.Safe {
		return
	}

	// otherwise walk stones backwards to unset invisible rids checking
	// each xid once against snapshot
	var (
		last types.XID
		skip bool
	)
	for i := len(t.stones) - 1; i >= 0; i-- {
		s := t.stones[i]

		// stop when xid is behind snapshot horizon
		if s.Xid < snap.Xmin {
			break
		}

		if s.Xid != last {
			// full visibility check, once per each xid
			skip = snap.IsVisible(s.Xid)
			last = s.Xid
		}

		if !skip {
			set.Unset(s.Rid)
		}
	}
}

func (t *Tomb) Load(ctx context.Context, bucket store.Bucket, id uint32) error {
	if bucket == nil {
		return store.ErrNoBucket
	}
	buf := bucket.Get(pack.EncodeBlockKey(id, 0, TombKey))
	if buf == nil {
		return nil
	}
	return t.UnmarshalBinary(buf)
}

func (t *Tomb) Store(ctx context.Context, bucket store.Bucket, id uint32) error {
	if !t.dirty {
		return nil
	}
	if bucket == nil {
		return store.ErrNoBucket
	}
	key := pack.EncodeBlockKey(id, 0, TombKey)

	// don't store empty tombs
	if t.Len() == 0 {
		return bucket.Delete(key)
	}

	buf, err := t.MarshalBinary()
	if err != nil {
		return err
	}
	err = bucket.Put(key, buf)
	if err != nil {
		return err
	}
	t.dirty = false
	return nil
}

func (t *Tomb) Remove(ctx context.Context, bucket store.Bucket, id uint32) error {
	if bucket == nil {
		return store.ErrNoBucket
	}
	return bucket.Delete(pack.EncodeBlockKey(id, 0, TombKey))
}

func (t *Tomb) MarshalBinary() ([]byte, error) {
	var b [num.MaxVarintLen64]byte
	buf := bytes.NewBuffer(make([]byte, 0, len(t.stones)*8)) // approx

	// n items
	buf.Write(b[:num.PutUvarint(b[:], uint64(len(t.stones)))])

	if len(t.stones) == 0 {
		return buf.Bytes(), nil
	}

	// find minima
	minRid, minXid := t.stones[0].Rid, t.stones[0].Xid
	for _, v := range t.stones[1:] {
		minRid = min(minRid, v.Rid)
		minXid = min(minXid, v.Xid)
	}

	// write minima
	buf.Write(b[:num.PutUvarint(b[:], minRid)])
	buf.Write(b[:num.PutUvarint(b[:], uint64(minXid))])

	// write diffs
	for _, v := range t.stones {
		buf.Write(b[:binary.PutUvarint(b[:], v.Rid-minRid)])
		buf.Write(b[:binary.PutUvarint(b[:], uint64(v.Xid-minXid))])
	}

	return buf.Bytes(), nil
}

func (t *Tomb) UnmarshalBinary(buf []byte) error {
	// n items
	v, n := num.Uvarint(buf)
	t.stones = make(Tombstones, int(v))
	buf = buf[n:]

	// minima
	minRid, n := binary.Uvarint(buf)
	buf = buf[n:]
	minXid, n := binary.Uvarint(buf)
	buf = buf[n:]

	if n == 0 {
		return io.ErrShortBuffer
	}

	if len(t.stones) > 0 {
		t.rids = xroar.New()
	}

	// read diffs
	for i := range t.stones {
		v, n = num.Uvarint(buf)
		buf = buf[n:]
		rid := v + minRid
		t.stones[i].Rid = rid
		t.rids.Set(rid)

		v, n = num.Uvarint(buf)
		buf = buf[n:]
		t.stones[i].Xid = types.XID(v + minXid)

		if n == 0 {
			return io.ErrShortBuffer
		}
	}
	t.rids.Cleanup()

	if l := len(t.stones); l > 0 {
		t.xmax = t.stones[l-1].Xid
	}

	return nil
}
