// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package types

type Snapshot struct {
	Xown uint64 // current transaction id
	Xmin uint64 // minimum active transaction id
	Xact uint64 // bitset with active tx ids (max 64): xmin+63 ... xmin
}

func (s *Snapshot) WithId(id uint64) *Snapshot {
	s.Xown = id
	return s
}

func (s *Snapshot) IsVisible(xid uint64) bool {
	// records from aborted tx have xid = 0
	if xid == 0 {
		return false
	}

	// read-write tx can see anything they created
	if s.Xown > 0 && xid == s.Xown {
		return true
	}

	// otherwise records are visible iff the record's tx
	// - has committed before (< global xmin horizon)
	// - was not active when the snapshot was made
	return xid < s.Xmin || s.Xact&(1<<xid-s.Xmin) == 0
}
