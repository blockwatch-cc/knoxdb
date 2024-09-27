// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package wal

import (
	"errors"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"

	"blockwatch.cc/knoxdb/internal/bitset"
	"github.com/echa/log"
)

var (
	ErrShortFrame = errors.New("short frame")
	ErrChecksum   = errors.New("checksum mismatch")

	castagnoliTable *crc32.Table
)

func init() {
	castagnoliTable = crc32.MakeTable(crc32.Castagnoli)
}

const (
	CommitLogName          = "xlog.bin"
	CommitFrameHeaderSize  = 12                              // bytes
	CommitFramePayloadSize = 1 << 16                         // 64k bytes
	CommitFrameShift       = 19                              // bit shift for frame/offset calculation
	CommitFrameMask        = uint64(1<<CommitFrameShift) - 1 // bits for address calculation
	CommitFrameSize        = CommitFrameHeaderSize + CommitFramePayloadSize
)

// TODO: roll tail when next xid is beyond (do we even need tail?)
//
// - maybe flip tail & last when write goes to the other
// - write & sync on evict or close
// - header should use min lsn (first commit in this range) for recovery
// - header does not need xmin (can calculate from id)

type CommitLog struct {
	wal        *Wal
	fd         *os.File
	nFrames    int
	checkpoint LSN
	tail       *CommitFrame
	last       *CommitFrame
	log        log.Logger
}

type CommitFrame struct {
	checkpoint LSN            // latest update written to this frame
	offset     int64          // file offset (calculated)
	xmin       uint64         // min xid (calculated)
	bits       *bitset.Bitset // commit bits
	dirty      bool           // content changed, must flush to disk
}

func NewCommitFrame(id int) *CommitFrame {
	return &CommitFrame{
		offset: int64(id) * CommitFrameSize,
		xmin:   uint64(id) << CommitFrameShift,
		bits:   bitset.NewBitset(CommitFramePayloadSize << 3),
	}
}

func (f *CommitFrame) Close() {
	f.checkpoint = 0
	f.offset = 0
	f.xmin = 0
	f.bits.Close()
	f.bits = nil
	f.dirty = false
}

func (f *CommitFrame) Xmin() uint64 {
	return f.xmin
}

func (f *CommitFrame) Xmax() uint64 {
	return f.xmin + 1<<CommitFrameShift - 1
}

func (f *CommitFrame) IsCommitted(xid uint64) bool {
	return f.bits.IsSet(int(xid - f.xmin))
}

func (f *CommitFrame) Append(xid uint64, lsn LSN) {
	f.bits.Set(int(xid - f.xmin))
	f.checkpoint = max(f.checkpoint, lsn)
}

func (f *CommitFrame) Contains(xid uint64) bool {
	return xid-f.xmin <= CommitFrameMask
}

func (f *CommitFrame) ReadFrom(fd *os.File) error {
	// read header
	var head [CommitFrameHeaderSize]byte
	n, err := fd.ReadAt(head[:], f.offset)
	if err != nil {
		return err
	}
	if n != CommitFrameHeaderSize {
		return ErrShortFrame
	}
	f.checkpoint = LSN(LE.Uint64(head[:]))

	// read data into bitset
	n, err = fd.ReadAt(f.bits.Bytes(), f.offset+CommitFrameHeaderSize)
	if err != nil {
		return err
	}
	if n != CommitFramePayloadSize {
		return ErrShortFrame
	}

	// check checksum
	crc := crc32.New(castagnoliTable)
	crc.Write(head[:8])
	crc.Write(f.bits.Bytes())
	if LE.Uint32(head[8:]) != crc.Sum32() {
		return ErrChecksum
	}

	return nil
}

func (f *CommitFrame) WriteTo(fd *os.File) error {
	if f == nil || !f.dirty {
		return nil
	}

	// prepare header
	var head [CommitFrameHeaderSize]byte
	LE.PutUint64(head[0:], uint64(f.checkpoint))
	crc := crc32.New(castagnoliTable)
	crc.Write(head[:8])
	crc.Write(f.bits.Bytes())
	LE.PutUint32(head[8:], crc.Sum32())

	// write head
	_, err := fd.WriteAt(head[:], f.offset)
	if err != nil {
		return err
	}

	// write body
	_, err = fd.WriteAt(f.bits.Bytes(), f.offset+CommitFrameHeaderSize)
	if err != nil {
		return err
	}

	f.dirty = false
	return nil
}

func NewCommitLog(wal *Wal) *CommitLog {
	return &CommitLog{
		wal: wal,
		log: log.Log, // TODO: init from wal
	}
}

func (c *CommitLog) Open() error {
	name := filepath.Join(c.wal.opts.Path, CommitLogName)
	fd, err := os.OpenFile(name, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return err
	}
	c.fd = fd

	// check file size and truncate if broken
	stat, err := c.fd.Stat()
	if err != nil {
		return err
	}

	if extra := stat.Size() % int64(CommitFrameSize); extra != 0 {
		c.log.Errorf("wal: broken xlog file size, %s bytes extra", extra)
		if err := c.fd.Truncate(stat.Size() - extra); err != nil {
			return err
		}
		stat, _ = c.fd.Stat()
	}
	c.nFrames = int(stat.Size() / int64(CommitFrameSize))

	// init frames
	switch c.nFrames {
	case 0:
		// empty file, create a new frame
		c.tail = NewCommitFrame(0)
	case 1:
		// load tail frame
		c.tail, err = c.LoadFrame(0)
	default:
		// load last two frames
		c.last, err = c.LoadFrame(c.nFrames - 2)
		if err == nil {
			c.tail, err = c.LoadFrame(c.nFrames - 1)
		}
	}

	// truncate file to zero (we don't know at which LSN to start recovery mid-file
	// because commits may happen in arbitrary order)
	if err != nil {
		if !errors.Is(err, ErrChecksum) && !errors.Is(err, ErrShortFrame) {
			return err
		}
		c.log.Errorf("wal: %v, recovering", err)
		if err := c.fd.Truncate(0); err != nil {
			return err
		}
		if c.tail != nil {
			c.tail.Close()
			c.tail = NewCommitFrame(0)
		}
		if c.last != nil {
			c.last.Close()
			c.last = nil
		}
	} else {
		// identify last LSN (instead of reading all frame headers we make the
		// assumption that a maximal late committing txid is no older than
		// two full frames, i.e. 1,048,576 transactions)
		c.checkpoint = c.tail.checkpoint
		if c.last != nil {
			c.checkpoint = max(c.checkpoint, c.last.checkpoint)
		}
	}

	// recover from last known checkpoint
	if err := c.Recover(c.checkpoint); err != nil {
		return err
	}

	return nil
}

func (c *CommitLog) Close() error {
	err := c.Sync()
	if err != nil {
		c.log.Errorf("wal: sync on close: %v", err)
	}
	err = c.fd.Close()
	if err != nil {
		c.log.Errorf("wal: close: %v", err)
	}
	c.fd = nil
	c.wal = nil
	c.nFrames = 0
	c.checkpoint = 0
	if c.tail != nil {
		c.tail.Close()
		c.tail = nil
	}
	if c.last != nil {
		c.last.Close()
		c.last = nil
	}
	return err
}

func (c *CommitLog) Sync() error {
	// write dirty frames
	if err := c.last.WriteTo(c.fd); err != nil {
		return err
	}
	if err := c.tail.WriteTo(c.fd); err != nil {
		return err
	}

	// fsync file
	return c.fd.Sync()
}

func (c *CommitLog) IsCommitted(xid uint64) (bool, error) {
	if c.tail.Contains(xid) {
		return c.tail.IsCommitted(xid), nil
	}
	if c.last != nil && !c.last.Contains(xid) {
		if err := c.last.WriteTo(c.fd); err != nil {
			return false, err
		}
		c.last.Close()
		c.last = nil
	}
	if c.last == nil {
		frame, err := c.LoadFrame(int(xid >> CommitFrameShift))
		if err != nil {
			return false, err
		}
		c.last = frame
	}
	return c.last.IsCommitted(xid), nil
}

func (c *CommitLog) AppendCommit(rec *Record) error {
	c.checkpoint = max(c.checkpoint, rec.Lsn)

	// txid is in tail frame
	if c.tail.Contains(rec.TxID) {
		c.tail.Append(rec.TxID, rec.Lsn)
		return nil
	}

	// txid is after tail frame (create new tail and roll last)
	if rec.TxID > c.tail.Xmax() {
		if c.last != nil {
			if err := c.last.WriteTo(c.fd); err != nil {
				return err
			}
			c.last.Close()
		}
		c.last = c.tail
		c.tail = NewCommitFrame(int(rec.TxID >> CommitFrameShift))
		c.tail.Append(rec.TxID, rec.Lsn)
		return nil
	}

	// txid is before tail frame, check last and potentially load another frame
	if c.last != nil && !c.last.Contains(rec.TxID) {
		if err := c.last.WriteTo(c.fd); err != nil {
			return err
		}
		c.last.Close()
		c.last = nil
	}
	if c.last == nil {
		frame, err := c.LoadFrame(int(rec.TxID >> CommitFrameShift))
		if err != nil {
			return err
		}
		c.last = frame
	}
	c.last.Append(rec.TxID, rec.Lsn)
	return nil
}

func (c *CommitLog) LoadFrame(id int) (*CommitFrame, error) {
	f := NewCommitFrame(id)
	err := f.ReadFrom(c.fd)
	if err != nil {
		f.Close()
		return nil, err
	}
	return f, nil
}

func (c *CommitLog) Recover(lsn LSN) error {
	// read wal starting at last checkpoint and add all commits
	r := c.wal.NewReader().WithType(RecordTypeCommit)
	err := r.Seek(lsn)
	if err != nil {
		return err
	}
	for {
		var rec *Record
		rec, err = r.Next()
		if err != nil {
			break
		}
		err = c.AppendCommit(rec)
		if err != nil {
			break
		}
	}
	if err != nil && err != io.EOF {
		return err
	}
	return nil
}
