// Copyright (c) 2018 - 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package badger

import (
	"blockwatch.cc/knoxdb/internal/store"
	"github.com/dgraph-io/badger/v4"
)

// sequence represents a database sequence. It can be used to generate
// unique ids for objects to decrease the memory required to store keys
// in database indexes.
type sequence struct {
	key    []byte
	closed bool
	db     *db
	seq    *badger.Sequence
}

// Enforce sequence implements the store.Sequence interface.
var _ store.Sequence = (*sequence)(nil)

// Next returns a new id from the sequences id space.
func (s *sequence) Next() (uint64, error) {
	if s.closed {
		str := "sequence is closed"
		return 0, makeDbErr(store.ErrTxClosed, str, nil)
	}
	val, err := s.seq.Next()
	if err != nil {
		return 0, convertErr("next sequence", err)
	}
	return val, nil
}

// release releases the sequence back to the database and reclaims
// any unused ids from the current lease. This is an internal function
// also called during database close to release a sequence without
// touching the sequence management map.
func (s *sequence) release() error {
	if s.closed {
		return nil
	}
	if err := s.seq.Release(); err != nil {
		return convertErr("release sequence", err)
	}
	log.Tracef("Closing sequence %s", string(s.key))
	s.closed = true
	s.seq = nil
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
	delete(s.db.sequences, string(s.key))
	s.db.seqLock.Unlock()
	s.key = nil
	s.db = nil
	return nil
}
