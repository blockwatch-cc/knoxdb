// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package wal

import (
	"bufio"
	"fmt"
	"hash"
	"io"

	"blockwatch.cc/knoxdb/internal/hash/xxhash64"
	"blockwatch.cc/knoxdb/internal/types"
)

type WalReader interface {
	Seek(LSN) error
	Next() (*Record, error)
	Close() error
	Checksum() uint64
	Lsn() LSN

	WithType(RecordType) WalReader
	WithTag(types.ObjectTag) WalReader
	WithEntity(uint64) WalReader
	WithTxID(types.XID) WalReader
}

type RecordFilter struct {
	typ    RecordType
	tag    types.ObjectTag
	entity uint64
	txid   types.XID
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
	isCommitAbort := r.Type == RecordTypeCommit || r.Type == RecordTypeAbort
	if f.entity > 0 && r.Entity != f.entity && !isCommitAbort {
		return false
	}
	if f.txid > 0 && r.TxID != f.txid {
		return false
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
	xid    types.XID
	lsn    LSN
	maxSz  int
	maxLsn LSN
	rcmode RecoveryMode
}

func (w *Wal) NewReader() WalReader {
	if w.IsClosed() {
		return &Reader{}
	}
	w.mu.RLock()
	defer w.mu.RUnlock()
	return &Reader{
		wal:    w,
		rd:     bufio.NewReaderSize(nil, WAL_BUFFER_SIZE),
		hash:   xxhash64.New(),
		csum:   w.opts.Seed,
		maxSz:  w.opts.MaxSegmentSize,
		maxLsn: w.nextLsn,
		rcmode: w.opts.RecoveryMode,
	}
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

func (r *Reader) WithTxID(v types.XID) WalReader {
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
	r.maxSz = 0
	r.maxLsn = 0
	r.rcmode = 0
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
	if lsn > r.maxLsn {
		return ErrInvalidLSN
	}

	sid := lsn.Segment(r.maxSz)

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
		if _, err := r.seg.Seek(0, 0); err != nil {
			return err
		}
		r.csum = r.wal.opts.Seed
		r.maxLsn = r.wal.nextLsn
		r.lsn = 0
		return nil
	}

	// check lsn is within segment size
	if int64(r.seg.sz) < lsn.Offset(r.maxSz) {
		return ErrSegmentTooShort
	}

	// seek to lsn offset
	// r.wal.log.Debugf("wal: seek lsn 0x%016x offs=%d", lsn, lsn.Offset(r.maxSz))
	if _, err := r.seg.Seek(lsn.Offset(r.maxSz), 0); err != nil {
		if err == io.EOF {
			return fmt.Errorf("wal: invalid seek to LSN 0x%016x offs=%x/%x",
				lsn, lsn.Offset(r.maxSz), r.seg.sz)
		}
		return err
	}

	// read record (we expect a checkpoint record)
	var head RecordHeader
	if err := r.read(head[:]); err != nil {
		return fmt.Errorf("wal: reading checkpoint at LSN 0x%016x offs=%x/%x: %v",
			lsn, lsn.Offset(r.maxSz), r.seg.sz, err)
	}

	// ensure this header looks correct
	if err := head.Validate(head.TxId(), lsn, r.maxLsn); err != nil {
		return fmt.Errorf("wal: %w on seek: header %s: %v", ErrInvalidRecord, head, err)
	}

	// ensure this is a checkpoint record
	switch head.Type() {
	case RecordTypeCheckpoint, RecordTypeCommit:
		// ok
	default:
		return ErrInvalidLSN
	}

	// init checksum from this record
	r.csum = head.Checksum()

	// init next lsn and reinit max lsn
	r.lsn = lsn.Add(HeaderSize)
	r.maxLsn = r.wal.nextLsn

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
		if err := head.Validate(r.xid, r.lsn, r.maxLsn); err != nil {
			return nil, fmt.Errorf("wal: %w: header %s: %v", ErrInvalidRecord, head, err)
		}

		// read body
		rec := head.NewRecord()
		if head.BodySize() > 0 {
			if err := r.read(rec.Data[0]); err != nil {
				// convert EOF error on short tail read
				if err == io.EOF {
					return nil, fmt.Errorf("wal: %w: header %s: %v", ErrInvalidRecord, head, ErrInvalidBodySize)
				}
				return nil, err
			}
		}

		// compare checksum
		var skipRecord bool
		csum := checksum(r.hash, r.csum, &head, rec.Data)
		if csum != head.Checksum() {
			switch r.rcmode {
			case RecoveryModeIgnore:
				// continue
			case RecoveryModeSkip:
				skipRecord = true
			default:
				return nil, fmt.Errorf("wal: %w: %v", ErrInvalidRecord, ErrInvalidChecksum)
			}
		}
		rec.Lsn = r.Lsn()

		// update reader state
		r.csum = csum
		r.lsn = r.lsn.Add(HeaderSize + rec.BodySize())
		r.xid = max(r.xid, head.TxId())

		// skip on broken checksum
		if skipRecord {
			continue
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

			// return EOF at end of wal
			sid := r.seg.Id()
			if !r.wal.hasSegment(sid + 1) {
				return io.EOF
			}

			// close the current segment
			r.rd.Reset(nil)
			r.seg.Close()
			r.seg = nil

			// open another segment if available
			r.wal.mu.RLock()
			s, err := r.wal.openSegment(sid+1, false)
			r.maxLsn = r.wal.nextLsn
			r.wal.mu.RUnlock()
			if err != nil {
				return err
			}
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

func checksum(h hash.Hash64, seed uint64, head *RecordHeader, body [][]byte) uint64 {
	h.Reset()
	var b [8]byte
	LE.PutUint64(b[:], seed)
	h.Write(b[:])
	h.Write(head[:22])
	for _, v := range body {
		h.Write(v)
	}
	return h.Sum64()
}
