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
	StatsKeySuffix   = []byte("_stats")   // statistics vectors bucket
	DataKeySuffix    = []byte("_data")    // data vectors bucket
	JournalKeySuffix = []byte("_journal") // journal vectors bucket
	StateKeySuffix   = []byte("_state")   // table state bucket
	StateKey         = []byte("state")    // table state key
)

// ObjectState stores volatile state of database objects such as
// tables and stores. Values represent pk sequence, num rows, and
// checkpoint lsn but can be repurposed for different types.
type ObjectState struct {
	Key        []byte  // state bucket name
	NextPk     uint64  // next free primary key sequence
	NextRid    uint64  // next free row id sequence
	NRows      uint64  // total non-deleted rows
	Size       uint64  // byte size
	Count      uint64  // block count
	Checkpoint wal.LSN // latest wal checkpoint LSN
}

func NewObjectState(name string) ObjectState {
	return ObjectState{
		Key:     append([]byte(name), StateKeySuffix...),
		NextPk:  1,
		NextRid: 1,
	}
}

func (s *ObjectState) Reset() {
	s.NextPk = 1
	s.NextRid = 1
	s.NRows = 0
	s.Size = 0
	s.Count = 0
	s.Checkpoint = 0
}

func (s ObjectState) Encode() []byte {
	var buf [48]byte
	BE.PutUint64(buf[0:], s.NextPk)
	BE.PutUint64(buf[8:], s.NextRid)
	BE.PutUint64(buf[16:], s.NRows)
	BE.PutUint64(buf[24:], s.Size)
	BE.PutUint64(buf[32:], s.Count)
	BE.PutUint64(buf[40:], uint64(s.Checkpoint))
	return buf[:]
}

func (s *ObjectState) Decode(buf []byte) error {
	if len(buf) < 48 {
		return io.ErrShortBuffer
	}
	s.NextPk = BE.Uint64(buf[0:])
	s.NextRid = BE.Uint64(buf[8:])
	s.NRows = BE.Uint64(buf[16:])
	s.Size = BE.Uint64(buf[24:])
	s.Count = BE.Uint64(buf[32:])
	s.Checkpoint = wal.LSN(BE.Uint64(buf[40:]))
	return nil
}

func (s *ObjectState) Load(ctx context.Context, tx store.Tx) error {
	buf := tx.Bucket(s.Key).Get(StateKey)
	if buf == nil || len(buf) < 40 {
		return ErrDatabaseCorrupt
	}
	return s.Decode(buf)
}

func (s ObjectState) Store(ctx context.Context, tx store.Tx) error {
	if s.NextPk == 0 {
		return tx.Bucket(s.Key).Delete(StateKey)
	}
	return tx.Bucket(s.Key).Put(StateKey, s.Encode())
}
