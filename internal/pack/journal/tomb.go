// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package journal

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"slices"
	"sort"

	"blockwatch.cc/knoxdb/internal/pack"
	"blockwatch.cc/knoxdb/internal/store"
	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/internal/xroar"
	"blockwatch.cc/knoxdb/pkg/util"
)

type Tomb struct {
	keys   *xroar.Bitmap // pks with tombstones (updated or deleted records)
	stones []Tombstone   // list of tombstones sorted by pk and rid
	maxsz  int           // max number of tombstones
	dirty  bool
}

type Tombstone struct {
	pk  uint64 // unique pk of the deleted record
	rid uint64 // unique row id that deleted record
	xid uint64 // transaction that deleted this record
	upd bool   // true when deletion was due to an update
}

func cmpTombstone(a, b Tombstone) int {
	if a.pk < b.pk {
		return -1
	}
	if a.pk > b.pk {
		return 1
	}
	if a.rid < b.rid {
		return -1
	}
	if a.rid > b.rid {
		return 1
	}
	return 0
}

func newTomb(maxsz int) *Tomb {
	return &Tomb{maxsz: maxsz}
}

func (t *Tomb) Clear() {
	t.keys = nil
	t.stones = nil
	t.dirty = false
}

func (t *Tomb) Reset() {
	if t.keys != nil {
		t.keys.Reset()
	}
	t.stones = t.stones[:0]
	t.dirty = false
}

func (t *Tomb) Len() int {
	return len(t.stones)
}

func (t *Tomb) Keys() *xroar.Bitmap {
	return t.keys
}

func (t *Tomb) Stones() []Tombstone {
	return t.stones
}

func (t *Tomb) HeapSize() int {
	return 28*len(t.stones) + t.keys.Size() + 36
}

func (t *Tomb) Append(pk, xid, rid uint64, isUpd bool) {
	var idx int
	stone := Tombstone{pk: pk, rid: rid, xid: xid, upd: isUpd}
	if t.keys == nil {
		t.keys = xroar.NewBitmap()
		t.stones = make([]Tombstone, 1, t.maxsz)
	} else {
		// insert in place, keep duplicates (equal pk/rid, different xid)
		idx, _ = slices.BinarySearchFunc(t.stones, stone, cmpTombstone)
		t.stones = append(t.stones, Tombstone{})
		copy(t.stones[idx+1:], t.stones[idx:])
	}
	t.keys.Set(pk)
	t.stones[idx] = stone
	t.dirty = true
}

func (t *Tomb) CommitTx(xid uint64) {
	// noop
}

func (t *Tomb) AbortTx(xid uint64) int {
	var n int
	for i, v := range t.stones {
		if v.xid != xid {
			continue
		}
		t.stones[i].xid = 0
		n += util.Bool2int(!t.stones[i].upd)
		t.dirty = true
	}
	return n
}

func (t *Tomb) AbortActiveTx(xact *xroar.Bitmap) int {
	var n int
	for i, v := range t.stones {
		if !xact.Contains(v.xid) {
			continue
		}
		t.stones[i].xid = 0
		n += util.Bool2int(!t.stones[i].upd)
		t.dirty = true
	}
	return n
}

// allow multiple entries with the same pk and different rid/xids
func (t *Tomb) Lookup(pk uint64) ([]Tombstone, bool) {
	if t.keys == nil {
		return nil, false
	}
	if !t.keys.Contains(pk) {
		return nil, false
	}
	idx := sort.Search(len(t.stones), func(i int) bool {
		return t.stones[i].pk >= pk
	})
	if idx == len(t.stones) || t.stones[idx].pk != pk {
		return nil, false
	}
	var n int
	for idx+n < len(t.stones) && t.stones[idx+n].pk == pk {
		n++
	}
	return t.stones[idx : idx+n], true
}

func (t *Tomb) IsDeleted(pk uint64, snap *types.Snapshot) (Tombstone, bool) {
	stones, ok := t.Lookup(pk)
	if ok {
		for _, stone := range stones {
			// skip updates
			if stone.upd {
				continue
			}
			// skip aborted and future tx (quick check)
			if stone.xid == 0 || stone.xid >= snap.Xmax {
				continue
			}
			// check visibility (full check)
			if snap.IsVisible(stone.xid) {
				return stone, true
			}
		}
	}
	return Tombstone{}, false
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
	var b [binary.MaxVarintLen64]byte
	buf := bytes.NewBuffer(make([]byte, 0, len(t.stones)*13)) // approx

	// n items
	buf.Write(b[:binary.PutUvarint(b[:], uint64(len(t.stones)))])

	if len(t.stones) == 0 {
		return buf.Bytes(), nil
	}

	// minima
	minPk, minRid, minXid := t.stones[0].pk, t.stones[0].rid, t.stones[0].xid
	for _, v := range t.stones[1:] {
		minPk = min(minPk, v.pk)
		minRid = min(minRid, v.rid)
		minXid = min(minXid, v.xid)
	}

	// write minima
	buf.Write(b[:binary.PutUvarint(b[:], minPk)])
	buf.Write(b[:binary.PutUvarint(b[:], minRid)])
	buf.Write(b[:binary.PutUvarint(b[:], minXid)])

	// write diffs
	for _, v := range t.stones {
		buf.Write(b[:binary.PutUvarint(b[:], v.pk-minPk)])
		buf.Write(b[:binary.PutUvarint(b[:], v.rid-minRid)])
		buf.Write(b[:binary.PutUvarint(b[:], v.xid-minXid)])
		buf.WriteByte(util.Bool2byte(v.upd))
	}

	return buf.Bytes(), nil
}

func (t *Tomb) UnmarshalBinary(buf []byte) error {
	// n items
	v, n := binary.Uvarint(buf)
	t.stones = make([]Tombstone, int(v))
	buf = buf[n:]

	// minima
	minPk, n := binary.Uvarint(buf)
	buf = buf[n:]
	minRid, n := binary.Uvarint(buf)
	buf = buf[n:]
	minXid, n := binary.Uvarint(buf)
	buf = buf[n:]

	if n == 0 {
		return io.ErrShortBuffer
	}

	if len(t.stones) > 0 {
		t.keys = xroar.NewBitmap()
	}

	// read diffs
	for i := range t.stones {
		v, n = binary.Uvarint(buf)
		buf = buf[n:]
		t.stones[i].pk = v + minPk
		t.keys.Set(v + minPk)

		v, n = binary.Uvarint(buf)
		buf = buf[n:]
		t.stones[i].rid = v + minRid

		v, n = binary.Uvarint(buf)
		buf = buf[n:]
		t.stones[i].xid = v + minXid

		t.stones[i].upd = buf[0] > 0
		buf = buf[1:]
	}

	if n == 0 {
		return io.ErrShortBuffer
	}

	return nil
}
