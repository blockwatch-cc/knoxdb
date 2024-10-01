// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package wal

import (
	"fmt"
	"os"
	"path/filepath"
)

const (
	segmentExt = "SEG"
)

type segment struct {
	id  int64
	pos int64
	fd  *os.File
	sz  int64
}

func createSegment(id LSN, opts WalOptions) (*segment, error) {
	filename := id.calculateFilename(opts.MaxSegmentSize)
	name := fmt.Sprintf("%d.%s", filename, segmentExt)
	f, err := os.OpenFile(filepath.Join(opts.Path, name), os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			f.Close()
		}
	}()
	var dir *os.File
	dir, err = os.Open(opts.Path)
	if err != nil {
		return nil, err
	}
	defer dir.Close()
	if err = dir.Sync(); err != nil {
		return nil, err
	}
	return &segment{
		pos: 0,
		id:  filename,
		fd:  f,
		sz:  0,
	}, nil
}

func openSegment(id LSN, opts WalOptions) (*segment, error) {
	filename := id.calculateFilename(opts.MaxSegmentSize)
	name := fmt.Sprintf("%d.%s", filename, segmentExt)
	f, err := os.OpenFile(filepath.Join(opts.Path, name), os.O_RDWR, os.ModePerm)
	if err != nil {
		return nil, err
	}
	finfo, err := f.Stat()
	if err != nil {
		return nil, err
	}
	fileOffset := id.calculateOffset(opts.MaxSegmentSize)
	return &segment{
		fd:  f,
		id:  filename,
		pos: fileOffset,
		sz:  finfo.Size(),
	}, nil
}

func doesSegmentExist(id int64, opt WalOptions) bool {
	name := fmt.Sprintf("%d.%s", id, segmentExt)
	_, err := os.Stat(filepath.Join(opt.Path, name))
	return err == nil
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

func (s *segment) Truncate(sz int64) error {
	return s.fd.Truncate(sz)
}

func (s *segment) Write(buf []byte) (int, error) {
	n, err := s.fd.Write(buf)
	if err != nil {
		return n, err
	}
	s.pos += int64(n)
	s.sz += int64(n)
	return int(s.pos), nil
}

func (s *segment) Seek(offset int64, whence int) (int64, error) {
	n, err := s.fd.Seek(offset, whence)
	if err != nil {
		return 0, err
	}
	s.pos = n
	return n, nil
}

func (s *segment) Size() int64 {
	return s.sz
}
