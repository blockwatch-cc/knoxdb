// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package engine

import (
	"context"
	"io"

	"blockwatch.cc/knoxdb/internal/store"
	"blockwatch.cc/knoxdb/internal/wal"
)

var (
	StateKeySuffix = []byte("_state")
	StateKey       = []byte("state")
)

// ObjectState stores volatile state of database objects such as
// tables and stores. Values represent pk sequence, num rows, and
// checkpoint lsn but can be repurposed for different types.
type ObjectState struct {
	Sequence   uint64  // next free sequence
	NRows      uint64  // total non-deleted rows
	Checkpoint wal.LSN // latest wal checkpoint LSN
}

func NewObjectState() ObjectState {
	return ObjectState{
		Sequence:   1,
		NRows:      0,
		Checkpoint: 0,
	}
}

func (s *ObjectState) Reset() {
	*s = NewObjectState()
}

func (s ObjectState) Encode() []byte {
	var buf [24]byte
	BE.PutUint64(buf[0:], s.Sequence)
	BE.PutUint64(buf[8:], s.NRows)
	BE.PutUint64(buf[16:], uint64(s.Checkpoint))
	return buf[:]
}

func (s *ObjectState) Decode(buf []byte) error {
	if len(buf) < 24 {
		return io.ErrShortBuffer
	}
	s.Sequence = BE.Uint64(buf[0:])
	s.NRows = BE.Uint64(buf[8:])
	s.Checkpoint = wal.LSN(BE.Uint64(buf[16:]))
	return nil
}

func (s *ObjectState) Load(ctx context.Context, tx store.Tx, name string) error {
	key := append([]byte(name), StateKeySuffix...)
	buf := tx.Bucket(key).Get(StateKey)
	if buf == nil || len(buf) < 24 {
		return ErrDatabaseCorrupt
	}
	return s.Decode(buf)
}

func (s ObjectState) Store(ctx context.Context, tx store.Tx, name string) error {
	key := append([]byte(name), StateKeySuffix...)
	if s.Sequence == 0 {
		return tx.Bucket(key).Delete(StateKey)
	}
	return tx.Bucket(key).Put(StateKey, s.Encode())
}
