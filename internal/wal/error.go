// Copyright (c) 2024 Blockwatch Data Inc.
// Author: abdul@blockwatch.cc

package wal

import "errors"

var (
	ErrInvalidLSN        = errors.New("wal: invalid LSN")
	ErrInvalidChecksum   = errors.New("wal: invalid checksum")
	ErrInvalidRecord     = errors.New("wal: invalid record")
	ErrInvalidWalOption  = errors.New("wal: invalid option")
	ErrInvalidRecordType = errors.New("wal: invalid record type")
	ErrInvalidObjectTag  = errors.New("wal: invalid object tag")
	ErrInvalidTxId       = errors.New("wal: invalid tx id")
	ErrInvalidBodySize   = errors.New("wal: invalid body size")

	ErrReaderClosed = errors.New("wal: reader closed")
	ErrWalClosed    = errors.New("wal: closed")

	ErrSegmentClosed   = errors.New("wal: segment closed")
	ErrSegmentReadOnly = errors.New("wal: segment read-only")
	ErrSegmentActive   = errors.New("wal: segment is active")
	ErrSegmentOverflow = errors.New("wal: segment overflow")
)
