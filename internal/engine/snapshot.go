// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package engine

type Snapshot struct {
	xown uint64 // current transaction id
	xmin uint64 // minimum active transaction id
	xact uint64 // bitset with active tx ids (max 64): xmin+63 ... xmin
}

func (e *Engine) NewSnapshot() *Snapshot {
	// e.RLock()
	s := &Snapshot{
		xmin: e.xmin,
	}
	for _, x := range e.txs {
		s.xact |= 1 << (x.id - s.xmin)
	}
	// e.RUnlock()
	return s
}

func (s *Snapshot) WithId(id uint64) *Snapshot {
	s.xown = id
	return s
}

func (s *Snapshot) IsVisible(xid uint64) bool {
	// records from aborted tx have xid = 0
	if xid == 0 {
		return false
	}

	// read-write tx can see anything they created
	if s.xown > 0 && xid == s.xown {
		return true
	}

	// otherwise records are visible iff the record's tx
	// - has committed before (< global xmin horizon)
	// - was not active when the snapshot was made
	return xid < s.xmin || s.xact&(1<<xid-s.xmin) == 0
}

// func (e *Engine) CreateSnapshot(name string) (*Snapshot, error) {

// }

// func (e *Engine) DropSnapshot(name string) error {

// }

// func (e *Engine) RollbackSnapshot(name string) error {

// }
