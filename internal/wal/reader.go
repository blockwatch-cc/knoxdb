// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package wal

import (
	"bufio"
	"fmt"
	"hash"
	"io"

	"blockwatch.cc/knoxdb/internal/hash/xxhash"
	"blockwatch.cc/knoxdb/internal/types"
)

const BufferSize = 1 << 19 // 512k

type WalReader interface {
	Seek(LSN) error
	Next() (*Record, error)
	Close() error
	Checksum() uint64
	Lsn() LSN

	WithType(RecordType) WalReader
	WithTag(types.ObjectTag) WalReader
	WithEntity(uint64) WalReader
	WithTxID(uint64) WalReader
	WithCommitted() WalReader
}

type RecordFilter struct {
	typ    RecordType
	tag    types.ObjectTag
	entity uint64
	txid   uint64
	xlog   *CommitLog
}

func (f *RecordFilter) Match(r *Record) bool {
	if f == nil {
		return true
	}
	if f.typ.IsValid() && r.Type != f.typ {
		return false
	}
	if f.tag.IsValid() && r.Tag != f.tag {
		return false
	}
	if f.entity > 0 && r.Entity != f.entity {
		return false
	}
	if f.txid > 0 && r.TxID != f.txid {
		return false
	}
	if f.xlog != nil {
		ok, err := f.xlog.IsCommitted(r.TxID)
		if !ok && err == nil {
			return false
		}
	}
	return true
}

var _ WalReader = (*Reader)(nil)

type Reader struct {
	wal    *Wal
	flt    *RecordFilter
	seg    *segment
	rd     *bufio.Reader
	hash   hash.Hash64
	csum   uint64
	xid    uint64
	lsn    LSN
	maxSeg int
	maxLsn LSN
}

func (w *Wal) NewReader() WalReader {
	if w.IsClosed() {
		return &Reader{}
	}
	w.mu.RLock()
	defer w.mu.RUnlock()
	return &Reader{
		wal:    w,
		rd:     bufio.NewReaderSize(nil, BufferSize),
		hash:   xxhash.New(),
		csum:   w.opts.Seed,
		maxSeg: w.opts.MaxSegmentSize,
		maxLsn: w.lsn,
	}
}

func (r *Reader) WithCommitted() WalReader {
	if r.flt == nil {
		r.flt = &RecordFilter{}
	}
	r.flt.xlog = r.wal.xlog
	return r
}

func (r *Reader) WithType(t RecordType) WalReader {
	if r.flt == nil {
		r.flt = &RecordFilter{}
	}
	r.flt.typ = t
	return r
}

func (r *Reader) WithTag(t types.ObjectTag) WalReader {
	if r.flt == nil {
		r.flt = &RecordFilter{}
	}
	r.flt.tag = t
	return r
}

func (r *Reader) WithEntity(v uint64) WalReader {
	if r.flt == nil {
		r.flt = &RecordFilter{}
	}
	r.flt.entity = v
	return r
}

func (r *Reader) WithTxID(v uint64) WalReader {
	if r.flt == nil {
		r.flt = &RecordFilter{}
	}
	r.flt.txid = v
	return r
}

func (r *Reader) IsClosed() bool {
	return r.wal == nil
}

func (r *Reader) Close() (err error) {
	if r.seg != nil {
		r.rd.Reset(nil)
		err = r.seg.Close()
		r.seg = nil
	}
	r.wal = nil
	r.flt = nil
	r.rd = nil
	r.hash = nil
	r.csum = 0
	r.xid = 0
	r.lsn = 0
	r.maxSeg = 0
	r.maxLsn = 0
	return
}

func (r *Reader) Lsn() LSN {
	return r.lsn
}

func (r *Reader) Checksum() uint64 {
	return r.csum
}

func (r *Reader) Seek(lsn LSN) error {
	if r.IsClosed() {
		return ErrReaderClosed
	}

	sid := lsn.Segment(r.maxSeg)

	// try current segment
	if r.seg != nil && r.seg.Id() != sid {
		r.seg.Close()
		r.seg = nil
	}

	// open segment and seek
	if r.seg == nil {
		s, err := r.wal.openSegment(sid, false)
		if err != nil {
			return err
		}
		r.seg = s
	}

	// reset buffer reader
	r.rd.Reset(r.seg)

	// special case lsn = 0
	if lsn == 0 {
		r.csum = r.wal.opts.Seed
		return nil
	}

	// seek to lsn offset
	if _, err := r.seg.Seek(lsn.Offset(r.maxSeg), 0); err != nil {
		return err
	}

	// read record (we expect a checkpoint record)
	var head RecordHeader
	if err := r.read(head[:]); err != nil {
		return err
	}

	// ensure this header looks correct
	if err := head.Validate(head.TxId(), lsn, r.maxLsn); err != nil {
		return err
	}

	// ensure this is a checkpoint record
	if head.Type() != RecordTypeCheckpoint {
		return fmt.Errorf("seek to non-checkpoint LSN")
	}

	// init checksum from this record
	r.csum = head.Checksum()

	// init next lsn
	r.lsn = lsn.Add(HeaderSize)

	return nil
}

func (r *Reader) Next() (*Record, error) {
	if r.IsClosed() {
		return nil, ErrReaderClosed
	}
	if r.seg == nil {
		if err := r.Seek(0); err != nil {
			return nil, err
		}
	}
	for {
		// read header, will return io.EOF at end of last segment
		var head RecordHeader
		if err := r.read(head[:]); err != nil {
			return nil, err
		}

		// validate header
		if err := head.Validate(r.xid, r.lsn, r.wal.lsn); err != nil {
			return nil, err
		}

		// read body
		rec := head.NewRecord()
		if head.BodySize() > 0 {
			if err := r.read(rec.Data); err != nil {
				return nil, err
			}
		}

		// compare checksum
		csum := checksum(r.hash, r.csum, &head, rec.Data)
		if csum != head.Checksum() {
			return nil, ErrInvalidChecksum
		}
		rec.Lsn = r.Lsn()

		// update reader state
		r.csum = csum
		r.lsn = r.lsn.Add(HeaderSize + len(rec.Data))
		if xid := head.TxId(); xid > 0 {
			r.xid = xid
		}

		// check filters and return on match
		if r.flt.Match(rec) {
			return rec, nil
		}
	}
}

func (r *Reader) read(buf []byte) error {
	for {
		n, err := r.rd.Read(buf)
		// handle cross-file reads
		if err != nil {
			if err != io.EOF {
				return err
			}
			// close the current segment
			sid := r.seg.Id()
			r.rd.Reset(nil)
			r.seg.Close()
			r.seg = nil

			// open another segment if available
			r.wal.mu.RLock()
			s, err := r.wal.openSegment(sid+1, false)
			r.wal.mu.RUnlock()
			if err != nil {
				return err
			}
			r.maxLsn = r.wal.lsn
			r.seg = s
			r.rd.Reset(r.seg)
		}
		if n == len(buf) {
			break
		}
		buf = buf[n:]
	}
	return nil
}

func checksum(h hash.Hash64, seed uint64, head *RecordHeader, body []byte) uint64 {
	h.Reset()
	var b [8]byte
	LE.PutUint64(b[:], seed)
	h.Write(b[:])
	h.Write(head[:22])
	h.Write(body)
	return h.Sum64()
}
