// Copyright (c) 2024 Blockwatch Data Inc.
// Author: abdul@blockwatch.cc

package wal

import (
	"bytes"
	"hash"
	"io"

	"blockwatch.cc/knoxdb/internal/hash/xxhash"
)

const (
	ChunkSize = 10 << 10
)

type bufferedReader struct {
	seg                *segment
	hash               hash.Hash64
	buf                *bytes.Buffer
	opts               WalOptions
	lastBufferReadPos  int64
	lastSegmentReadPos int64
}

func newBufferedReader(wal *Wal) *bufferedReader {
	opts := wal.opts
	opts.readOnly = true
	return &bufferedReader{
		opts:               opts,
		hash:               xxhash.New(),
		buf:                bytes.NewBuffer(make([]byte, 0, ChunkSize)),
		lastBufferReadPos:  0,
		lastSegmentReadPos: 0,
	}
}

func (b *bufferedReader) Close() error {
	var err error
	if b.seg != nil {
		err = b.seg.Close()
		b.seg = nil

	}
	b.hash = nil
	return err
}

func (b *bufferedReader) Seek(lsn LSN) error {
	if b.IsClosed() {
		return ErrClosed
	}
	// open segment and seek
	filepos := lsn.calculateOffset(b.opts.MaxSegmentSize)
	seg, err := openSegment(lsn, b.opts)
	if err != nil {
		return err
	}
	_, err = seg.Seek(int64(filepos), 0)
	if err != nil {
		return err
	}
	b.lastBufferReadPos = filepos
	b.lastSegmentReadPos = filepos
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
				b.lastSegmentReadPos += int64(size)
				return b.buf.Next(size), nil
			} else {
				b.lastSegmentReadPos += int64(size + len(extraData))
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

		r := io.NewSectionReader(b.seg.fd, b.lastBufferReadPos, ChunkSize)
		b.buf.Reset()
		n, err := b.buf.ReadFrom(r)
		if err != nil {
			return nil, err
		}
		b.lastBufferReadPos += int64(n)
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
	return doesSegmentExist(b.seg.id+1, b.opts)
}

func (b *bufferedReader) IsClosed() bool {
	return b.hash == nil
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
	lsn := NewLSN(b.seg.id+1, int64(b.opts.MaxSegmentSize), 0)
	seg, err := openSegment(lsn, b.opts)
	if err != nil {
		return err
	}
	b.lastBufferReadPos = 0
	b.lastSegmentReadPos = 0
	b.seg = seg
	return nil
}

func (b *bufferedReader) ReadPosition() int64 {
	return b.lastSegmentReadPos
}
