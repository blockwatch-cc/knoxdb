// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package wal

import "blockwatch.cc/knoxdb/internal/types"

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

func (t RecordType) IsValid() bool {
	return t != RecordTypeInvalid
}

// LSN represents a Log Serial Number for WAL records. The LSN is
// the unique position (offset) of a record in the wal.
type LSN uint64

type Record struct {
	Type   RecordType
	Tag    types.ObjectTag // object kind (db, table, store, enum, etc)
	TxID   uint64          // unique transaction id this record was belongs to
	Entity uint64          // object id (tagged hash for db, table, store, enum, etc)
	Data   []byte          // body
	Lsn    LSN             //
}

// RecordHeader is written to
type RecordHeader struct {
	Type     RecordType
	Tag      types.ObjectTag // object kind (db, table, store, enum, etc)
	TxID     uint64          // unique transaction id this record was belongs to
	Entity   uint64          // object id (tagged hash for db, table, store, enum, etc)
	Len      int             // data length
	Checksum uint64          // chained hash checksum (prev record hash + header + data)
}
