// Copyright (c) 2024-2026 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package types

import (
	"math/bits"
	"strconv"
	"sync"
	"sync/atomic"

	"blockwatch.cc/knoxdb/internal/bitset"
)

type XID uint64

const ReadTxOffset XID = 1 << 63

func (x XID) String() string {
	if x > ReadTxOffset {
		return "R_" + strconv.FormatUint(uint64(x-ReadTxOffset), 10)
	} else {
		return "W_" + strconv.FormatUint(uint64(x), 10)
	}
}

type AtomicXID struct {
	a atomic.Uint64
}

func (x *AtomicXID) Load() XID {
	return XID(x.a.Load())
}

func (x *AtomicXID) Store(xid XID) {
	x.a.Store(uint64(xid))
}

func (x *AtomicXID) Add(xid XID) XID {
	return XID(x.a.Add(uint64(xid)))
}

func (x *AtomicXID) CompareAndSwap(old, new XID) bool {
	return x.a.CompareAndSwap(uint64(old), uint64(new))
}

var snapPool = sync.Pool{
	New: func() any { return new(Snapshot) },
}

type Snapshot struct {
	Xown XID            // current transaction id (0 when readonly)
	Xmin XID            // minimum active transaction id (next when no write tx)
	Xmax XID            // next tx id (not yet assigned)
	Xaci XID            // bitset with active tx ids (xmax-xmin <= 64)
	Xact *bitset.Bitset // bitset with active tx ids (xmax-xmin > 64)
	Safe bool           // snapshot is safe (xact = 0 || only readonly tx)
}

// NewSnapshot creates a new MVCC shapshot for transaction xid
// with horizon xmin and future (unassigned/next) xmax. It is
// used for deciding whether database records are visible to xid
// under snapshot isolation rules. As long as there are only a few
// write transactions (current design limits to one) the snapshot
// is most efficient and no extra tx id tracking bitset is allocated.
// A snapshot remains immutable after creation (New + AddActive) for
// its entire lifetime.
func NewSnapshot(xid, xmin, xmax XID) *Snapshot {
	s := snapPool.Get().(*Snapshot)
	s.Xown = xid
	s.Xmin = xmin
	s.Xmax = xmax
	s.Xaci = 0
	s.Xact = nil
	s.Safe = true
	if sz := int(xmax - xmin); sz > bits.UintSize {
		s.Xact = bitset.New(sz)
	}
	return s
}

func (s *Snapshot) Close() {
	if s.Xact != nil {
		s.Xact.Close()
	}
	*s = Snapshot{}
	snapPool.Put(s)
}

// AddActive adds ids of currently active write transactions to
// the monitored set.
func (s *Snapshot) AddActive(xid XID) *Snapshot {
	if s.Xact == nil {
		s.Xaci |= 1 << (xid - s.Xmin)
	} else {
		s.Xact.Set(int(xid - s.Xmin))
	}
	s.Safe = false
	return s
}

// IsVisible returns true when records updated by this xid
// are visible to the snapshot. It is used on reads.
func (s *Snapshot) IsVisible(xid XID) bool {
	// records from aborted tx (xid = 0) and future tx are invisible
	// note xmax is next assignable xid at time of snapshot
	if xid == 0 || xid >= s.Xmax {
		return false
	}

	// anything before global horizon is visible
	if xid < s.Xmin {
		return true
	}

	// safe snapshots can see anything < xmax
	if s.Safe && xid < s.Xmax {
		return true
	}

	// read-write txs can see their own data
	if s.Xown > 0 && xid == s.Xown {
		return true
	}

	// otherwise records are only visible iff the record's tx
	// was committed when the snapshot was made
	return !s.wasActive(xid)
}

// IsConflict returns true when two transactions are in
// write-write conflict with each other. This is used on writes.
func (s *Snapshot) IsConflict(xid XID) bool {
	if xid < s.Xmin || xid == s.Xown {
		return false
	}
	return s.wasActive(xid)
}

// wasActive returns true if xid was in the active set
// at snapshot creation time.
func (s *Snapshot) wasActive(xid XID) bool {
	if s.Xact == nil {
		return s.Xaci&(1<<(xid-s.Xmin)) > 0
	} else {
		return s.Xact.Contains(int(xid - s.Xmin))
	}
}
