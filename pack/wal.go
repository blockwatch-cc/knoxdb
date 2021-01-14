// Copyright (c) 2018-2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package pack

import (
	"bytes"
	"io"
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
	w io.ReadWriteCloser
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
	return w.w != nil
}

func (w *Wal) Close() {
	if w.w != nil {
		w.w.Close()
		w.w = nil
	}
}

func (w *Wal) Write(rec WalRecordType, pk uint64, val Item) {
	if w == nil {
		return
	}
	var b [256]byte
	buf := bytes.NewBuffer(b[:0])
	buf.WriteString(string(rec))
	buf.WriteString(strconv.FormatUint(pk, 10))
	buf.WriteByte('\n')
	w.w.Write(buf.Bytes())
}

func (w *Wal) WriteMulti(rec WalRecordType, pks []uint64, vals []Item) {
	if w == nil {
		return
	}
	for i := range pks {
		var b [256]byte
		buf := bytes.NewBuffer(b[:0])
		buf.WriteString(string(rec))
		buf.WriteString(strconv.FormatUint(pks[i], 10))
		buf.WriteByte('\n')
		w.w.Write(buf.Bytes())
		// TODO
		// serialize value (if exists)
	}
}

func (w *Wal) WritePack(rec WalRecordType, pkg *Package, pos, n int) {
	col, _ := pkg.Column(pkg.pkindex)
	pks, _ := col.([]uint64)
	for _, pk := range pks[pos : pos+n] {
		var b [256]byte
		buf := bytes.NewBuffer(b[:0])
		buf.WriteString(string(rec))
		buf.WriteString(strconv.FormatUint(pk, 10))
		buf.WriteByte('\n')
		w.w.Write(buf.Bytes())
	}
}
