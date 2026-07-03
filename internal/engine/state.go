// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package engine

import (
	"context"
	"encoding/binary"

	"blockwatch.cc/knoxdb/internal/wal"
	"blockwatch.cc/knoxdb/pkg/store"
)

var (
	DataKeySuffix  = []byte("_data")  // data vectors bucket
	TombKeySuffix  = []byte("_tomb")  // tomb vectors bucket
	EpochKeySuffix = []byte("_epoch") // epoch watermark bucket
	StateKeySuffix = []byte("_state") // table state bucket
	StateKey       = []byte("state")  // table state key
)

// ObjectState stores volatile state of database objects such as
// tables and stores. Values represent pk sequence, num rows, and
// checkpoint lsn but can be repurposed for different types.
type ObjectState struct {
	Key        []byte  // state bucket name
	NextRid    uint64  // next free row id sequence
	NextPk     uint64  // next free primary key sequence
	NRows      uint64  // total non-deleted rows
	Epoch      uint64  // latest object version epoch
	Checkpoint wal.LSN // latest wal checkpoint LSN
}

func NewObjectState(name string) ObjectState {
	return ObjectState{
		Key:     append([]byte(name), StateKeySuffix...),
		NextPk:  1,
		NextRid: 1,
		Epoch:   0,
	}
}

func (s *ObjectState) Reset() {
	s.NextRid = 1
	s.NextPk = 1
	s.NRows = 0
	s.Epoch = 0
	s.Checkpoint = 0
}

func (s *ObjectState) Update(n ObjectState) {
	// don't replace key
	s.NextRid = n.NextRid
	s.NextPk = n.NextPk
	s.NRows = n.NRows
	s.Epoch = n.Epoch
	s.Checkpoint = n.Checkpoint
}

func (s *ObjectState) Encode() []byte {
	var tmp [5 * binary.MaxVarintLen64]byte
	buf := binary.AppendUvarint(tmp[:0], s.NextRid)
	buf = binary.AppendUvarint(buf, s.NextPk)
	buf = binary.AppendUvarint(buf, s.NRows)
	buf = binary.AppendUvarint(buf, s.Epoch)
	buf = binary.AppendUvarint(buf, uint64(s.Checkpoint))
	return buf
}

func (s *ObjectState) Decode(buf []byte) error {
	var (
		n    int
		vals [5]uint64
	)
	for i := range 5 {
		vals[i], n = binary.Uvarint(buf)
		if n == 0 {
			return ErrDatabaseCorrupt
		}
		buf = buf[n:]
	}
	s.NextRid = vals[0]
	s.NextPk = vals[1]
	s.NRows = vals[2]
	s.Epoch = vals[3]
	s.Checkpoint = wal.LSN(vals[4])
	return nil
}

func (s *ObjectState) Load(ctx context.Context, tx store.Tx) error {
	buf, err := store.GetKey(tx, s.Key, StateKey)
	if err != nil {
		return ErrDatabaseCorrupt
	}
	return s.Decode(buf)
}

func (s *ObjectState) Store(ctx context.Context, tx store.Tx) error {
	b, err := tx.Bucket(s.Key)
	if err != nil {
		return err
	}
	if s.NextPk == 0 || s.NextRid == 0 {
		return b.Delete(StateKey)
	}
	return b.Put(StateKey, s.Encode())
}
