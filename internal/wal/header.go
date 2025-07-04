// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package wal

import (
	"fmt"

	"blockwatch.cc/knoxdb/internal/types"
)

const (
	HeaderSize = 30
	MaxTxIdGap = types.XID(1 << 20)
)

type RecordHeader [HeaderSize]byte

func (h RecordHeader) Type() RecordType     { return RecordType(h[0]) }
func (h RecordHeader) Tag() types.ObjectTag { return types.ObjectTag(h[1]) }
func (h RecordHeader) TxId() types.XID      { return types.XID(LE.Uint64(h[2:10])) }
func (h RecordHeader) Entity() uint64       { return LE.Uint64(h[10:18]) }
func (h RecordHeader) BodySize() int        { return int(LE.Uint32(h[18:22])) }
func (h RecordHeader) Checksum() uint64     { return LE.Uint64(h[22:30]) }

func (h *RecordHeader) SetType(v RecordType)     { h[0] = byte(v) }
func (h *RecordHeader) SetTag(v types.ObjectTag) { h[1] = byte(v) }
func (h *RecordHeader) SetTxId(v types.XID)      { LE.PutUint64(h[2:10], uint64(v)) }
func (h *RecordHeader) SetEntity(v uint64)       { LE.PutUint64(h[10:18], v) }
func (h *RecordHeader) SetBodySize(v int)        { LE.PutUint32(h[18:22], uint32(v)) }
func (h *RecordHeader) SetChecksum(v uint64)     { LE.PutUint64(h[22:30], v) }

func (h RecordHeader) NewRecord() *Record {
	var body [][]byte
	if l := h.BodySize(); l > 0 {
		body = [][]byte{make([]byte, l)}
	}
	return &Record{
		Type:   h.Type(),
		Tag:    h.Tag(),
		TxID:   h.TxId(),
		Entity: h.Entity(),
		Data:   body,
	}
}

func (h RecordHeader) Validate(lastXid types.XID, lsn, maxLsn LSN) error {
	if !h.Type().IsValid() {
		return ErrInvalidRecordType
	}
	if !h.Tag().IsValid() {
		return ErrInvalidObjectTag
	}
	switch h.Type() {
	case RecordTypeCheckpoint:
		if h.TxId() != 0 {
			return ErrInvalidTxId
		}
		if h.BodySize() != 0 {
			return ErrInvalidBodySize
		}
	default:
		xid := h.TxId()
		if xid == 0 || (lastXid > 0 && absDiff(xid, lastXid) > MaxTxIdGap) {
			return ErrInvalidTxId
		}
		if maxLsn > 0 && lsn.Add(h.BodySize()) > maxLsn {
			return ErrInvalidBodySize
		}
	}
	return nil
}

func (h RecordHeader) String() string {
	return fmt.Sprintf("typ=%s tag=%s xid=0x%016x entity=0x%016x len=%d csum=0x%016x",
		h.Type(), h.Tag(), h.TxId(), h.Entity(), h.BodySize(), h.Checksum(),
	)
}

func absDiff(a, b types.XID) types.XID {
	if a < b {
		return b - a
	}
	return a - b
}
