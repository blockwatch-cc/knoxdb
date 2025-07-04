// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package types

import (
	"math/bits"
	"strconv"
	"sync"

	"blockwatch.cc/knoxdb/internal/bitset"
)

type XID uint64

const ReadTxOffset XID = 1 << 63

func (x XID) String() string {
	if x > ReadTxOffset {
		return "R-" + strconv.FormatUint(uint64(x-ReadTxOffset), 10)
	} else {
		return "W-" + strconv.FormatUint(uint64(x), 10)
	}
}

var snapPool = sync.Pool{
	New: func() any { return new(Snapshot) },
}

type Snapshot struct {
	Xown XID            // current transaction id (0 when readonly)
	Xmin XID            // minimum active transaction id
	Xmax XID            // next tx id (not yet assigned)
	Xaci XID            // bitset with active tx ids (xmax-xmin <= 64)
	Xact *bitset.Bitset // bitset with active tx ids (xmax-xmin > 64)
	Safe bool           // snapshot is safe (xact = 0 || only readonly tx)
}

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

// we only add writable tx here
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
// are visible to the snapshot.
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
	if s.Xact == nil {
		return s.Xaci&(1<<(xid-s.Xmin)) == 0
	} else {
		return !s.Xact.Contains(int(xid - s.Xmin))
	}
}

func (s *Snapshot) IsConflict(xid XID) bool {
	if xid < s.Xmin || xid == s.Xown {
		return false
	}
	if s.Xact == nil {
		return s.Xaci&(1<<(xid-s.Xmin)) > 0
	} else {
		return s.Xact.Contains(int(xid - s.Xmin))
	}
}
