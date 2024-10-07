// Copyright (c) 2024 Blockwatch Data Inc.
// Author: abdul@blockwatch.cc

package wal

import "errors"

var (
	ErrInvalidRecord = errors.New("invalid record")

	ErrInvalidChecksum   = errors.New("invalid checksum")
	ErrInvalidWalOption  = errors.New("invalid option")
	ErrInvalidRecordType = errors.New("invalid record type")
	ErrInvalidObjectTag  = errors.New("invalid object tag")
	ErrInvalidTxId       = errors.New("invalid tx id")
	ErrInvalidBodySize   = errors.New("invalid body size")

	ErrInvalidLSN   = errors.New("seek to non-checkpoint LSN")
	ErrReaderClosed = errors.New("wal reader closed")
	ErrWalClosed    = errors.New("wal closed")

	ErrSegmentClosed     = errors.New("wal segment closed")
	ErrSegmentReadOnly   = errors.New("wal segment read-only")
	ErrSegmentAppendOnly = errors.New("wal seek on append-only segment")
	ErrSegmentOverflow   = errors.New("wal segment overflow")
)
