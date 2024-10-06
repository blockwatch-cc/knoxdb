// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package wal

import (
	"fmt"

	"blockwatch.cc/knoxdb/internal/types"
)

type RecordType byte

const (
	RecordTypeInvalid RecordType = iota
	RecordTypeInsert
	RecordTypeUpdate
	RecordTypeDelete
	RecordTypeCommit
	RecordTypeAbort
	RecordTypeCheckpoint
)

var (
	recordTypeNames    = "__insert_update_delete_commit_abort_checkpoint"
	recordTypeNamesOfs = [...]int{0, 2, 9, 16, 23, 30, 36, 47}
)

func (t RecordType) IsValid() bool {
	return t != RecordTypeInvalid && t <= RecordTypeCheckpoint
}

func (t RecordType) String() string {
	return recordTypeNames[recordTypeNamesOfs[t] : recordTypeNamesOfs[t+1]-1]
}

// LSN represents a Log Serial Number for WAL records. The LSN is
// the unique position (offset) of a record in the wal.
type LSN uint64

func (l LSN) Add(n int) LSN {
	return l + LSN(n)
}

func (l LSN) Segment(sz int) int {
	return int(uint64(l) / uint64(sz))
}

func (l LSN) Offset(sz int) int64 {
	return int64(uint64(l) % uint64(sz))
}

type Record struct {
	Type   RecordType
	Tag    types.ObjectTag // object kind (db, table, store, enum, etc)
	TxID   uint64          // unique transaction id this record was belongs to
	Entity uint64          // object id (tagged hash for db, table, store, enum, etc)
	Data   []byte          // body with encoded data, may be empty
	Lsn    LSN             // the record's byte offset in the WAL
}

func (r Record) String() string {
	return fmt.Sprintf("wal: LSN=0x%016x xid=0x%016x  typ=%s tag=%s entity=0x%016x len=%d",
		r.Lsn, r.TxID, r.Type, r.Tag, r.Entity, len(r.Data),
	)
}

func (r Record) IsValid() bool {
	return r.Type.IsValid() && r.Tag.IsValid() && (r.TxID > 0 || (r.Type == RecordTypeCheckpoint && r.TxID == 0))
}

func (r Record) Validate() error {
	if !r.Type.IsValid() {
		return ErrInvalidRecordType
	}
	if !r.Tag.IsValid() {
		return ErrInvalidObjectTag
	}
	switch r.Type {
	case RecordTypeCheckpoint:
		if r.TxID > 0 {
			return ErrInvalidTxId
		}
		if len(r.Data) > 0 {
			return ErrInvalidBodySize
		}
	case RecordTypeCommit, RecordTypeAbort:
		if r.TxID == 0 {
			return ErrInvalidTxId
		}
	default:
		if r.TxID == 0 {
			return ErrInvalidTxId
		}
		if len(r.Data) == 0 {
			return ErrInvalidBodySize
		}
	}
	return nil
}

func (r *Record) Header() (h RecordHeader) {
	h.SetType(r.Type)
	h.SetTag(r.Tag)
	h.SetTxId(r.TxID)
	h.SetEntity(r.Entity)
	h.SetBodySize(len(r.Data))
	return
}
