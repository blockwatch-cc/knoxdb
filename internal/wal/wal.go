// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package wal

import (
	"encoding/binary"
	"os"
	"path/filepath"
)

var LE = binary.LittleEndian

type WalReader interface {
	Seek(LSN) error
	Next() (*Record, error)
	// NextN([]*Record) error
}

type Wal struct {
	w *os.File
}

func Open(path, name string) (*Wal, error) {
	f, err := os.Create(filepath.Join(path, name) + ".wal")
	if err != nil {
		return nil, err
	}
	w := &Wal{
		w: f,
	}
	return w, nil
}

func (w *Wal) Close() {
	if w == nil || w.w == nil {
		return
	}
	w.w.Close()
	w.w = nil
}

// func (w *Wal) Reset() error {
// 	if w == nil || w.w == nil {
// 		return nil
// 	}
// 	err := w.w.Truncate(0)
// 	if err != nil {
// 		return err
// 	}
// 	_, err = w.w.Seek(0, 0)
// 	return err
// }

func (w *Wal) Sync() error {
	if w == nil || w.w == nil {
		return nil
	}
	return w.w.Sync()
}

func (w *Wal) Write(rec *Record) (LSN, error) {
	if w == nil || w.w == nil || rec == nil {
		return 0, nil
	}
	lsn := LSN(w.w.Offset())
	w.Write([]byte{rec.Type})
	w.Write([]byte{rec.Tag})
	binary.Write(w, LE, rec.Entity)
	binary.Write(w, LE, rec.TxID)
	binary.Write(w, LE, rec.Entity)
	binary.Write(w, LE, rec.Checksum)
	_, err := w.Write(rec.Data)
	if err != nil {
		return 0, err
	}
	return lsn, nil
}
