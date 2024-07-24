// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package mem

import (
    "sync/atomic"

	"blockwatch.cc/knoxdb/store"
)

// sequence represents a database sequence. It can be used to generate
// unique ids for objects to decrease the memory required to store keys
// in database indexes.
type sequence struct {
	key    string
	closed bool
	db     *db
    seq    atomic.Uint64
}

// Enforce sequence implements the store.Sequence interface.
var _ store.Sequence = (*sequence)(nil)

// Next returns a new id from the sequences id space.
func (s *sequence) Next() (uint64, error) {
	if s.closed {
		str := "sequence is closed"
		return 0, makeDbErr(store.ErrTxClosed, str, nil)
	}
	return s.seq.Add(1), nil
}

// release releases the sequence back to the database. This is an
// internal function also called during database close to release
// a sequence without touching the sequence management map.
func (s *sequence) release() error {
	s.closed = true
	return nil
}

func (s *sequence) Release() error {
	if s.closed {
		return nil
	}
	if err := s.release(); err != nil {
		return err
	}
	s.db.seqLock.Lock()
    s.closed = true
	delete(s.db.sequences, s.key)
	s.db.seqLock.Unlock()
    s.key = ""
	s.db = nil
	return nil
}
