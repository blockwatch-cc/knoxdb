// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package wal

import (
	"hash"
	"os"
)

type segment struct {
	sz   int
	pos  int
	csum uint64
	fd   *os.File
	hash hash.Hash64
}

func createSegment(id LSN, csum uint64) (*segment, error) {
	// load last record's checksum
	return nil, nil
}

func openSegment(id LSN) (*segment, error) {
	// load last record's checksum
	return nil, nil
}

func (s *segment) Close() error {
	err := s.fd.Close()
	s.fd = nil
	s.sz = 0
	s.pos = 0
	s.csum = 0
	return err
}

func (s *segment) Sync() error {
	return s.fd.Sync()
}

func (s *segment) Write(rec *Record) (lsn LSN, err error) {
	// create header
	var head [28]byte
	head[0] = byte(rec.Type)
	head[1] = byte(rec.Tag)
	LE.PutUint64(head[2:], rec.Entity)
	LE.PutUint64(head[10:], rec.TxID)
	LE.PutUint32(head[16:], uint32(len(rec.Data)))

	// calculate chained checksum
	s.hash.Reset()
	var b [8]byte
	LE.PutUint64(b[:], s.csum)
	s.hash.Write(b[:])
	s.hash.Write(head[:20])
	s.hash.Write(rec.Data)
	s.hash.Sum(head[24:])

	// write header
	var n, sz int
	n, err = s.fd.Write(head[:])
	if err != nil {
		return
	}
	sz += n

	// write data
	n, err = s.fd.Write(rec.Data)
	if err != nil {
		return
	}
	sz += n

	// update state
	s.pos += sz
	s.csum = s.hash.Sum64()

	lsn = LSN(s.pos)
	return
}
