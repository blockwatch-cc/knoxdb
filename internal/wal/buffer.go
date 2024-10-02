// Copyright (c) 2024 Blockwatch Data Inc.
// Author: abdul@blockwatch.cc

package wal

import (
	"bytes"
	"errors"
	"io"
)

var (
	ErrClosed = errors.New("closed")
)

const (
	ChunkSize = 10 << 10
)

type bufferedReader struct {
	seg         *segment
	wal         *Wal
	buf         *bytes.Buffer
	lastReadPos int64
}

func newBufferedReader(wal *Wal) *bufferedReader {
	return &bufferedReader{
		wal:         wal,
		buf:         bytes.NewBuffer(make([]byte, 0, ChunkSize)),
		lastReadPos: 0,
	}
}

func (b *bufferedReader) Close() error {
	var err error
	if b.seg != nil {
		err = b.seg.Close()
		b.seg = nil

	}
	b.wal = nil
	return err
}

func (b *bufferedReader) Seek(lsn LSN) error {
	if b.IsClosed() {
		return ErrClosed
	}
	// open segment and seek
	filepos := lsn.calculateOffset(b.wal.opts.MaxSegmentSize)
	seg, err := openSegment(lsn, b.wal.opts)
	if err != nil {
		return err
	}
	_, err = seg.Seek(int64(filepos), 0)
	if err != nil {
		return err
	}
	b.lastReadPos = filepos
	b.seg = seg
	return nil
}

func (b *bufferedReader) Read(size int) ([]byte, error) {
	if b.IsClosed() {
		return nil, ErrClosed
	}
	// if segment is not set, this is the first read
	// start read from seed
	if b.seg == nil {
		err := b.Seek(LSN(0))
		if err != nil {
			return nil, err
		}
	}

	hasMore := true

	var extraData []byte

	for {
		blen := b.buf.Len()
		if size <= blen {
			if len(extraData) == 0 {
				return b.buf.Next(size), nil
			} else {
				result := append(extraData, b.buf.Next(size)...)
				extraData = nil
				return result, nil
			}
		} else {
			if len(extraData) == 0 {
				extraData = bytes.Clone(b.buf.Bytes())
			} else {
				extraData = append(extraData, b.buf.Bytes()...)
			}
			size -= blen
		}

		if !hasMore {
			break
		}

		r := io.NewSectionReader(b.seg.fd, b.lastReadPos, ChunkSize)
		b.buf.Reset()
		n, err := b.buf.ReadFrom(r)
		if err != nil {
			return nil, err
		}
		b.lastReadPos += int64(n)
		if n < ChunkSize {
			if b.hasNextSegment() {
				err = b.nextSegment()
				if err != nil {
					return nil, err
				}
			} else {
				hasMore = false
			}
		}
	}

	if len(extraData) > 0 {
		return nil, io.ErrShortBuffer
	}

	return nil, io.EOF
}

func (b *bufferedReader) hasNextSegment() bool {
	if b.IsClosed() {
		return false
	}
	if b.seg == nil {
		if err := b.Seek(LSN(0)); err != nil {
			return false
		}
	}
	return doesSegmentExist(b.seg.id+1, b.wal.opts)
}

func (b *bufferedReader) IsClosed() bool {
	return b.wal == nil
}

func (b *bufferedReader) nextSegment() error {
	if b.IsClosed() {
		return ErrClosed
	}
	if b.seg == nil {
		if err := b.Seek(LSN(0)); err != nil {
			return err
		}
	}
	defer b.seg.Close()
	lsn := NewLSN(b.seg.id+1, int64(b.wal.opts.MaxSegmentSize), 0)
	seg, err := openSegment(lsn, b.wal.opts)
	if err != nil {
		return err
	}
	b.lastReadPos = 0
	b.seg = seg
	return nil
}
