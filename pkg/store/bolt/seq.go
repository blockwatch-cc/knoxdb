// Copyright (c) 2018-2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package bolt

import "blockwatch.cc/knoxdb/pkg/store"

// sequence represents a database sequence. It can be used to generate
// unique ids for objects to decrease the memory required to store keys
// in database indexes.
type sequence struct {
	db  *db
	key []byte
}

// Enforce sequence implements the store.Sequence interface.
var _ store.Sequence = (*sequence)(nil)

// Next returns a new id from the sequences id space.
func (s *sequence) Next() (uint64, error) {
	var val uint64
	err := s.db.Update(func(tx store.Tx) error {
		bucket := tx.Bucket(s.key)
		var err error
		val, err = bucket.NextSequence()
		return err
	})
	return val, err
}

func (s *sequence) Release() error {
	return nil
}
