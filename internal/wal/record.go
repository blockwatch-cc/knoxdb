// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc, abdul@blockwatch.cc

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

	MinimumTxIDDistance = 10 << 10
)

var (
	recordTypeNames    = "__insert_update_delete_commit_abort_checkpoint"
	recordTypeNamesOfs = [...]int{0, 2, 9, 16, 23, 30, 36, 47}
)

func (t RecordType) IsValid() bool {
	return t != RecordTypeInvalid
}

func (t RecordType) String() string {
	return recordTypeNames[recordTypeNamesOfs[t] : recordTypeNamesOfs[t+1]-1]
}

// LSN represents a Log Serial Number for WAL records. The LSN is
// the unique position (offset) of a record in the wal.
type LSN uint64

func NewLSN(id, sz, pos int64) LSN {
	return LSN(id*sz + pos)
}

func (lsn LSN) calculateFilename(sz int) int64 {
	return int64(lsn / LSN(sz))
}

func (lsn LSN) calculateOffset(sz int) int64 {
	return int64(lsn % LSN(sz))
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
	return r.Type.IsValid() && r.Tag.IsValid()
}

func (r Record) IsTxIDValid(prevTxId uint64) bool {
	diff := uint64(0)
	if r.TxID > prevTxId {
		diff = r.TxID - prevTxId
	} else {
		diff = prevTxId - r.TxID
	}
	return diff <= MinimumTxIDDistance
}
