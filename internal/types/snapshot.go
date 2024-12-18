// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package types

import (
	"math/bits"
	"sync"

	"blockwatch.cc/knoxdb/internal/bitset"
)

var snapPool = sync.Pool{
	New: func() any { return new(Snapshot) },
}

type Snapshot struct {
	Xown uint64         // current transaction id (0 when readonly)
	Xmin uint64         // minimum active transaction id
	Xmax uint64         // next tx id (not yet assigned)
	Xaci uint64         // bitset with active tx ids (xmax-xmin <= 64)
	Xact *bitset.Bitset // bitset with active tx ids (xmax-xmin > 64)
	Safe bool           // snapshot is safe (xact = 0 || only readonly tx)
}

func NewSnapshot(xid, xmin, xmax uint64) *Snapshot {
	s := snapPool.Get().(*Snapshot)
	s.Xown = xid
	s.Xmin = xmin
	s.Xmax = xmax
	s.Xaci = 0
	s.Xact = nil
	s.Safe = true
	if sz := int(xid - xmin); sz > bits.UintSize {
		s.Xact = bitset.NewBitset(sz)
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
func (s *Snapshot) AddActive(xid uint64) {
	if s.Xact == nil {
		s.Xaci |= 1 << (xid - s.Xmin)
	} else {
		s.Xact.Set(int(xid - s.Xmin))
	}
	s.Safe = false
}

// IsVisible returns true when records updated by this xid
// are visible to the snapshot.
func (s *Snapshot) IsVisible(xid uint64) bool {
	// records from aborted tx (xid = 0) and future tx are invisible
	// note xmax is next assignable xid at time of snapshot
	if xid == 0 || xid >= s.Xmax {
		return false
	}

	// anything before global horizon is visible
	if xid < s.Xmin {
		return true
	}

	// safe snapshots can see anything < xmax and
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
		return !s.Xact.IsSet(int(xid - s.Xmin))
	}
}

func (s *Snapshot) IsConflict(xid uint64) bool {
	if xid < s.Xmin || xid == s.Xown {
		return false
	}
	if s.Xact == nil {
		return s.Xaci&(1<<(xid-s.Xmin)) > 0
	} else {
		return s.Xact.IsSet(int(xid - s.Xmin))
	}
}
