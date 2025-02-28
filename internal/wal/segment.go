// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package wal

import (
	"fmt"
	"os"
	"path/filepath"
)

const (
	SEG_FILE_SUFFIX  = ".seg"
	SEG_FILE_PATTERN = "%016d.seg"
	SEG_FILE_MODE    = 0644
	SEG_FILE_MINSIZE = 1 << 10 // 1k
	SEG_FILE_MAXSIZE = 1 << 34 // 4G
)

var (
	createFlags = os.O_CREATE | os.O_EXCL | os.O_WRONLY | os.O_APPEND
	writeFlags  = os.O_WRONLY | os.O_APPEND
	readFlags   = os.O_RDONLY
)

type segment struct {
	id  int
	fd  *os.File
	sz  int
	max int
	ro  bool
}

func (w *Wal) segmentName(id int) string {
	return filepath.Join(w.opts.Path, fmt.Sprintf(SEG_FILE_PATTERN, id))
}

// must call with lock held
func (w *Wal) createSegment(id int) (*segment, error) {
	name := w.segmentName(id)
	w.log.Debugf("wal: create segment %s", name)
	fd, err := OpenFile(name, createFlags, SEG_FILE_MODE)
	if err != nil {
		return nil, err
	}
	dir, err := os.Open(w.opts.Path)
	if err != nil {
		fd.Close()
		return nil, err
	}
	defer dir.Close()
	if err = dir.Sync(); err != nil {
		fd.Close()
		return nil, err
	}
	s := &segment{
		id:  id,
		fd:  fd,
		sz:  0,
		max: w.opts.MaxSegmentSize,
		ro:  false,
	}
	return s, nil
}

// must call with lock held
func (w *Wal) openSegment(id int, active bool) (*segment, error) {
	// check before we attempt opening a file
	if !w.hasSegment(id) {
		// w.log.Debugf("wal: missing segment id %d", id)
		// w.log.Debug(string(debug.Stack()))
		return nil, ErrSegmentNotFound
	}

	// we expect the segment file to exists
	name := w.segmentName(id)
	flags := readFlags
	if active {
		flags = writeFlags
	}
	w.log.Debugf("wal: open segment %s active=%t", name, active)
	fd, err := OpenFile(name, flags, SEG_FILE_MODE)
	if err != nil {
		return nil, err
	}
	err = fd.Sync()
	if err != nil {
		return nil, err
	}
	stat, err := fd.Stat()
	if err != nil {
		fd.Close()
		return nil, err
	}
	s := &segment{
		id:  id,
		fd:  fd,
		sz:  int(stat.Size()),
		max: w.opts.MaxSegmentSize,
		ro:  !active,
	}
	return s, nil
}

func (w *Wal) hasSegment(id int) bool {
	return id == 0 || LSN(id*w.opts.MaxSegmentSize) < w.lsn
}

func (s *segment) Close() (err error) {
	if s.fd != nil {
		if !s.ro {
			if err = s.fd.Sync(); err != nil {
				return
			}
		}
		err = s.fd.Close()
	}
	*s = segment{}
	return
}

func (s *segment) ForceClose() (err error) {
	if s.fd != nil {
		err = s.fd.Close()
	}
	*s = segment{}
	return
}

func (s *segment) Id() int {
	return s.id
}

func (s *segment) Len() int {
	return s.sz
}

func (s *segment) Cap() int {
	return s.max - s.sz
}

func (s *segment) Sync() error {
	if s.ro {
		return nil
	}
	if s.fd == nil {
		return ErrSegmentClosed
	}
	return s.fd.Sync()
}

func (s *segment) Seek(n int64, _ int) (int64, error) {
	if s.fd == nil {
		return 0, ErrSegmentClosed
	}
	if !s.ro {
		return 0, ErrSegmentAppendOnly
	}
	return s.fd.Seek(n, 0)
}

func (s *segment) Write(buf []byte) (int, error) {
	if s.ro {
		return 0, ErrSegmentReadOnly
	}
	if s.fd == nil {
		return 0, ErrSegmentClosed
	}
	if len(buf) == 0 {
		return 0, nil
	}
	if s.Cap() < len(buf) {
		return 0, ErrSegmentOverflow
	}
	n, err := s.fd.Write(buf)
	s.sz += n
	return n, err
}

func (s *segment) Read(buf []byte) (int, error) {
	if s.fd == nil {
		return 0, ErrSegmentClosed
	}
	return s.fd.Read(buf)
}
