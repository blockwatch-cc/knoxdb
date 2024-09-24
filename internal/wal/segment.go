// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package wal

import (
	"os"
)

type segment struct {
	id  int
	pos int
	fd  *os.File
}

// func newSegement() *segment {
// 	return &segment{
// 		hash: xxhash.New(),
// 	}
// }

func createSegment(id LSN) (*segment, error) {
	// use the seed as first checksum
	return nil, nil
}

func openSegment(id LSN) (*segment, error) {
	// load last record's checksum
	return nil, nil
}

func (s *segment) Close() error {
	err := s.fd.Close()
	s.fd = nil
	s.id = 0
	s.pos = 0
	return err
}

func (s *segment) Sync() error {
	return s.fd.Sync()
}

func (s *segment) LastRecord() (*Record, error) {
	return nil, nil
}

func (s *segment) Truncate(sz int) error {
	return s.fd.Truncate(int64(sz))
}

func (s *segment) Write(buf []byte) (int, error) {
	n, err := s.fd.Write(buf)
	if err != nil {
		return n, err
	}
	s.pos += n
	return n, nil
}

// func (s *segment) Write(rec *Record) (lsn LSN, err error) {
// 	// Note: this is only an example to show how a record can be written
// 	//
// 	// create header
// 	var head [28]byte
// 	head[0] = byte(rec.Type)
// 	head[1] = byte(rec.Tag)
// 	LE.PutUint64(head[2:], rec.Entity)
// 	LE.PutUint64(head[10:], rec.TxID)
// 	LE.PutUint32(head[16:], uint32(len(rec.Data)))

// 	// calculate chained checksum
// 	s.hash.Reset()
// 	var b [8]byte
// 	LE.PutUint64(b[:], s.csum)
// 	s.hash.Write(b[:])
// 	s.hash.Write(head[:20])
// 	s.hash.Write(rec.Data)
// 	s.hash.Sum(head[20:])

// 	// write header
// 	var n, sz int
// 	n, err = s.fd.Write(head[:])
// 	if err != nil {
// 		return
// 	}
// 	sz += n

// 	// write data
// 	n, err = s.fd.Write(rec.Data)
// 	if err != nil {
// 		return
// 	}
// 	sz += n

// 	// TODO: mix in the segment id
// 	lsn = LSN(s.id + s.pos)

// 	// update state
// 	s.pos += sz
// 	s.csum = s.hash.Sum64()

// 	return
// }
