// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package journal

import (
	"container/heap"
	"slices"

	"blockwatch.cc/knoxdb/internal/pack"
	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/internal/xroar"
	"blockwatch.cc/knoxdb/pkg/schema"
)

// TODO
// switch to vectorized pipeline
// - change to keep pairs of segment + selection vector only
// - on query
//   - post-process table packs (selection vectors) and exclude deleted rids
//   - output full pack with its sel vector as single result

type pkAddr struct {
	pk   uint64
	addr uint32
}

func newPkAddr(pk uint64, pos, ofs uint32) pkAddr {
	return pkAddr{
		pk:   pk,
		addr: pos<<24 | ofs&0x00FFFFFF,
	}
}

func (a pkAddr) segment() int {
	return int(a.addr >> 24)
}

func (a pkAddr) offset() int {
	return int(a.addr & 0x00FFFFFF)
}

// implements container/heap interface to keep list sorted by pk
type pkAddrs []pkAddr

func (l pkAddrs) Len() int           { return len(l) }
func (l pkAddrs) Less(i, j int) bool { return l[i].pk < l[j].pk }
func (l pkAddrs) Swap(i, j int)      { l[i], l[j] = l[j], l[i] }

func (h *pkAddrs) Push(x any) {
	*h = append(*h, x.(pkAddr))
}

func (h *pkAddrs) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

type Result struct {
	pks  *xroar.Bitmap // matched pks
	del  *xroar.Bitmap // deleted pks
	segs []*Segment    // segments with matches
	keys pkAddrs       // pk to address mapping (addr := segment(1) + offset(3))
}

func NewResult() *Result {
	return &Result{
		pks:  xroar.New(),
		del:  xroar.New(),
		segs: make([]*Segment, 0),
		keys: make(pkAddrs, 0),
	}
}

func (r *Result) Close() {
	clear(r.segs)
	r.pks = nil
	r.del = nil
	r.segs = nil
	r.keys = nil
}

func (r *Result) IsEmpty() bool {
	return len(r.keys) == 0
}

func (r *Result) Len() int {
	return r.pks.Count()
}

func (r *Result) SetDeleted(pk uint64) {
	if r.del == nil {
		r.del = xroar.New()
	}
	r.del.Set(pk)
	r.pks.Set(pk)
}

func (r *Result) IsDeleted(pk uint64) bool {
	if r.del == nil {
		return false
	}
	return r.del.Contains(pk)
}

func (r *Result) Append(seg *Segment, hits []uint32) {
	// skip without matches
	if len(hits) == 0 {
		return
	}

	// only add segment to result when any of its records is used
	segId := -1

	// Walk segment matches backwards (newest to oldest) to implement update shadowing.
	// Although we already use snapshot isolation in segment matching we do not update
	// xmax in completed segments so we don't know from just looking at each record
	// whether it is active or deleted.
	for _, i := range slices.Backward(hits) {
		// read primary key
		pk := seg.data.Pk(int(i))

		// skip deleted records (this is necessary because old passive segments
		// may contain records with xmax == 0 which were deleted later)
		if r.IsDeleted(pk) {
			continue
		}

		// skip old updates (this makes more recent segments shadows older records which
		// we attempt to insert here later). We do not update a record's xmax in passive
		// segments after their last xid completes.
		//
		// FIXME doesn't this violate what we need during merge? who sets xmax?
		if r.pks.Contains(pk) {
			continue
		}

		// add pk to known set
		r.pks.Set(pk)

		// add this segment to result list, use its position to make address records below
		if segId < 0 {
			r.segs = append(r.segs, seg)
			segId = len(r.segs)
		}

		// generate pointer
		ptr := newPkAddr(pk, uint32(segId), i)

		// add pointer to heap and keep result sorted
		heap.Push(&r.keys, ptr)
	}
}

// query integration
func (r *Result) FindPk(pk uint64) (int, bool) {
	// quick check
	if pk == 0 || !r.pks.Contains(pk) || r.del.Contains(pk) {
		return 0, false
	}

	// find pk in keys list
	idx, ok := slices.BinarySearchFunc(r.keys, pkAddr{pk, 0}, func(a, b pkAddr) int {
		if a.pk < b.pk {
			return -1
		}
		if a.pk > b.pk {
			return 1
		}
		return 0
	})
	if !ok {
		return 0, false
	}
	return idx, true
}

func (r *Result) GetRef(i int) (*pack.Package, int) {
	addr := r.keys[i]
	s := r.segs[addr.segment()]
	return s.data, addr.offset()
}

func (r *Result) GetMeta(i int) *schema.Meta {
	addr := r.keys[i]
	s := r.segs[addr.segment()]
	return s.data.Meta(addr.offset())
}

func (r *Result) UnsetMatch(pk uint64) {
	r.pks.Unset(pk)
}

func (r *Result) ForEach(fn func(*pack.Package, int) error) error {
	for _, addr := range r.keys {
		if !r.pks.Contains(addr.pk) {
			continue
		}

		err := fn(r.segs[addr.segment()].data, addr.offset())
		if err != nil {
			if err == types.EndStream {
				break
			}
			return err
		}
	}
	return nil
}

func (r *Result) ForEachReverse(fn func(*pack.Package, int) error) error {
	for i := len(r.keys); i >= 0; i-- {
		addr := r.keys[i]
		if !r.pks.Contains(addr.pk) {
			continue
		}

		err := fn(r.segs[addr.segment()].data, addr.offset())
		if err != nil {
			if err == types.EndStream {
				break
			}
			return err
		}
	}
	return nil
}
