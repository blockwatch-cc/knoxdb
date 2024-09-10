// Copyright (c) 2018-2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc
//go:build ignore
// +build ignore

package pack

import (
	"bytes"
	"os"
	"path/filepath"
	"strconv"
)

type WalRecordType string

var (
	WalRecordTypeInsert WalRecordType = "I "
	WalRecordTypeUpdate WalRecordType = "U "
	WalRecordTypeDelete WalRecordType = "D "
)

type Wal struct {
	w *os.File
}

func OpenWal(path, name string) (*Wal, error) {
	f, err := os.Create(filepath.Join(path, name) + ".wal")
	if err != nil {
		return nil, err
	}
	w := &Wal{
		w: f,
	}
	return w, nil
}

func (w *Wal) IsOpen() bool {
	return w != nil && w.w != nil
}

func (w *Wal) Close() {
	if w == nil || w.w == nil {
		return
	}
	w.w.Close()
	w.w = nil
}

func (w *Wal) Reset() error {
	if w == nil || w.w == nil {
		return nil
	}
	err := w.w.Truncate(0)
	if err != nil {
		return err
	}
	_, err = w.w.Seek(0, 0)
	return err
}

func (w *Wal) Sync() error {
	if w == nil || w.w == nil {
		return nil
	}
	return w.w.Sync()
}

func (w *Wal) Write(rec WalRecordType, pk uint64, val any) error {
	if w == nil || w.w == nil {
		return nil
	}
	var b [256]byte
	buf := bytes.NewBuffer(b[:0])
	buf.WriteString(string(rec))
	buf.WriteString(strconv.FormatUint(pk, 10))
	buf.WriteByte('\n')
	if _, err := w.w.Write(buf.Bytes()); err != nil {
		return err
	}
	return nil
}

func (w *Wal) WriteMulti(rec WalRecordType, pks []uint64, vals any) error {
	if w == nil || w.w == nil {
		return nil
	}
	for i := range pks {
		var b [256]byte
		buf := bytes.NewBuffer(b[:0])
		buf.WriteString(string(rec))
		buf.WriteString(strconv.FormatUint(pks[i], 10))
		buf.WriteByte('\n')
		if _, err := w.w.Write(buf.Bytes()); err != nil {
			return err
		}
		// TODO
		// serialize value (if exists)
	}
	return nil
}

func (w *Wal) WritePack(rec WalRecordType, pkg *Package, pos, n int) error {
	if w == nil || w.w == nil {
		return nil
	}
	col, _ := pkg.Column(pkg.pkindex)
	pks, _ := col.([]uint64)
	for _, pk := range pks[pos : pos+n] {
		var b [256]byte
		buf := bytes.NewBuffer(b[:0])
		buf.WriteString(string(rec))
		buf.WriteString(strconv.FormatUint(pk, 10))
		buf.WriteByte('\n')
		if _, err := w.w.Write(buf.Bytes()); err != nil {
			return err
		}
	}
	return nil
}
