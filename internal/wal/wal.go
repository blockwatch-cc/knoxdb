// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package wal

import (
	"encoding/binary"

	"blockwatch.cc/knoxdb/internal/types"
)

var LE = binary.LittleEndian

type WalReader interface {
	Seek(LSN) error
	Next() (*Record, error)
	// NextN([]*Record) error

	WithType(RecordType) WalReader
	WithTag(types.ObjectTag) WalReader
	WithEntity(uint64) WalReader
	WithTxID(uint64) WalReader
}

type WalOptions struct {
	Path           string
	MaxSegmentSize int
}

type Wal struct {
	opts   WalOptions
	active *segment
}

func Create(opts WalOptions) (*Wal, error) {
	// create directory
	// create active wal segment
	return &Wal{}, nil
}

func Open(opts WalOptions) (*Wal, error) {
	// try open directory
	// set exclusive lock
	// open last segment file
	// read hash of last record

	return &Wal{}, nil
}

func (w *Wal) Close() error {
	err := w.active.Close()
	w.active = nil
	return err
}

func (w *Wal) Sync() error {
	return w.active.Sync()
}

func (w *Wal) Write(rec *Record) (LSN, error) {
	return w.active.Write(rec)
}
