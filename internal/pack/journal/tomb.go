// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package journal

import (
	"bytes"
	"container/heap"
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
	maxsz  int           // max number of tombstones
	dirty  bool
}

type Tombstone struct {
	Xid uint64 // transaction that deleted this record
	Rid uint64 // unique row id the deleted record
}

// implements container/heap interface to keep list sorted by xid
type Tombstones []Tombstone

func (l Tombstones) Len() int           { return len(l) }
func (l Tombstones) Less(i, j int) bool { return l[i].Xid < l[j].Xid }
func (l Tombstones) Swap(i, j int)      { l[i], l[j] = l[j], l[i] }

func (h *Tombstones) Push(x any) {
	*h = append(*h, x.(Tombstone))
}

func (h *Tombstones) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

func newTomb(maxsz int) *Tomb {
	return &Tomb{maxsz: maxsz}
}

func (t *Tomb) Clear() {
	t.rids = nil
	t.stones = nil
	t.dirty = false
}

func (t *Tomb) Reset() {
	if t.rids != nil {
		t.rids.Reset()
	}
	t.stones = t.stones[:0]
	t.dirty = false
}

func (t *Tomb) Len() int {
	return len(t.stones)
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

func (t *Tomb) Append(xid, rid uint64) {
	stone := Tombstone{Xid: xid, Rid: rid}
	if t.rids == nil {
		t.rids = xroar.New()
		t.stones = make([]Tombstone, 0, t.maxsz)
	}
	heap.Push(&t.stones, stone)
	t.rids.Set(rid)
	t.dirty = true
}

func (t *Tomb) CommitTx(xid uint64) {
	// noop
}

func (t *Tomb) AbortTx(xid uint64) int {
	l := len(t.stones)
	idx := sort.Search(l, func(i int) bool {
		return t.stones[i].Xid >= xid
	})
	if idx == l {
		return 0
	}
	var n int
	for i := range t.stones[idx:] {
		s := &t.stones[i]
		if s.Xid != xid {
			break
		}
		s.Xid = 0
		t.rids.Unset(s.Rid)
		n++
	}

	// fix heap and remove aborted entries
	if n > 0 {
		heap.Init(&t.stones)
		copy(t.stones, t.stones[n:])
		t.stones = t.stones[:len(t.stones)-n]
		t.dirty = len(t.stones) > 0
	}
	return n
}

func (t *Tomb) AbortActiveTx(xact *xroar.Bitmap) int {
	var (
		n, i int
		l    = len(t.stones)
	)
	it := xact.NewIterator()
	for {
		xid, ok := it.Next()
		if !ok || i == l {
			break
		}

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
		}
	}

	// fix heap and remove aborted entries
	if n > 0 {
		heap.Init(&t.stones)
		copy(t.stones, t.stones[n:])
		t.stones = t.stones[:len(t.stones)-n]
		t.dirty = len(t.stones) > 0
	}

	return n
}

func (t *Tomb) MergeVisible(set *xroar.Bitmap, snap *types.Snapshot) {
	if len(t.stones) == 0 {
		return
	}
	for _, s := range t.stones {
		// future tx are invisible
		if s.Xid > snap.Xmax {
			continue
		}

		// past tx before horizon and own tx are visible
		if s.Xid == snap.Xown || s.Xid < snap.Xmin {
			set.Set(s.Rid)
			continue
		}

		// TODO: benchmark if we can just use the full check without shortcuts

		// full visibility check
		if snap.IsVisible(s.Xid) {
			set.Set(s.Rid)
		}
	}
}

func (t *Tomb) Load(ctx context.Context, bucket store.Bucket, id uint32) error {
	if bucket == nil {
		return store.ErrNoBucket
	}
	buf := bucket.Get(pack.EncodeBlockKey(id, TombKey))
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
	key := pack.EncodeBlockKey(id, TombKey)

	// don't store empty tombs
	if t.Len() == 0 {
		return bucket.Delete(key)
	}

	buf, err := t.MarshalBinary()
	if err != nil {
		return err
	}
	return bucket.Put(key, buf)
}

func (t *Tomb) Remove(ctx context.Context, bucket store.Bucket, id uint32) error {
	if bucket == nil {
		return store.ErrNoBucket
	}
	return bucket.Delete(pack.EncodeBlockKey(id, TombKey))
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
	buf.Write(b[:num.PutUvarint(b[:], minXid)])

	// write diffs
	for _, v := range t.stones {
		buf.Write(b[:binary.PutUvarint(b[:], v.Rid-minRid)])
		buf.Write(b[:binary.PutUvarint(b[:], v.Xid-minXid)])
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
		t.stones[i].Xid = v + minXid

		if n == 0 {
			return io.ErrShortBuffer
		}
	}

	return nil
}
