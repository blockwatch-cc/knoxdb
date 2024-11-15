// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package types

type Snapshot struct {
	Xown uint64 // current transaction id (only assigned on first write, otherwise zero)
	Xmin uint64 // minimum active transaction id
	Xmax uint64 // next tx id (not yet assigned)
	Xact uint64 // bitset with active tx ids (max 64): xmin+63 ... xmin
	Safe bool   // snapshot is safe (xact = 0 || only readonly tx)
}

// IsVisible returns true when records updated by this xid
// are visible to the snapshot.
func (s *Snapshot) IsVisible(xid uint64) bool {
	// records from aborted tx have xid = 0
	if s.Safe || xid == 0 {
		return false
	}

	// records from future tx are invisible
	if xid > s.Xmax {
		return false
	}

	// read-write txs can see anything they created
	if s.Xown > 0 && xid == s.Xown {
		return true
	}

	// otherwise records are visible iff the record's tx
	// - has committed before (< global xmin horizon)
	// - was not active when the snapshot was made
	return xid < s.Xmin || s.Xact&(1<<(xid-s.Xmin)) == 0
}

func (s *Snapshot) IsConflict(xid uint64) bool {
	if xid < s.Xmin || xid == s.Xown {
		return false
	}
	return s.Xact&(1<<(xid-s.Xmin)) > 0
}
