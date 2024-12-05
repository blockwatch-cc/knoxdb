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
	ErrShortFrame   = errors.New("short frame")
	ErrTxIdTooLarge = errors.New("tx id too large")
	ErrChecksum     = errors.New("checksum mismatch")

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

type CommitLog struct {
	fd         *os.File
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
	log        log.Logger     // logger
}

func NewCommitFrame(id int64) *CommitFrame {
	return &CommitFrame{
		offset: id * CommitFrameSize,
		xmin:   uint64(id) << CommitFrameShift,
		bits:   bitset.NewBitset(CommitFramePayloadSize << 3),
		log:    log.Disabled,
	}
}

func (f *CommitFrame) WithLogger(log log.Logger) *CommitFrame {
	f.log = log
	return f
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
	if f.bits == nil {
		f.log.Debugf("appending xid: %d lsn: %d to closed frame", xid, lsn)
	}
	f.dirty = true
	f.bits.Set(int(xid - f.xmin))
	f.checkpoint = max(f.checkpoint, lsn)
}

func (f *CommitFrame) Contains(xid uint64) bool {
	return xid-f.xmin <= CommitFrameMask
}

func (f *CommitFrame) ReadFrom(fd *os.File) error {
	if f.bits == nil {
		f.log.Debug("reading from closed frame")
	}
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

	if f.bits == nil {
		f.log.Debug("writing to closed frame")
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

func NewCommitLog() *CommitLog {
	return &CommitLog{
		log: log.Disabled,
	}
}

func (c *CommitLog) WithLogger(l log.Logger) *CommitLog {
	c.log = l.Clone().WithTag("xlog:")
	return c
}

func (c *CommitLog) Open(path string, wal *Wal) error {
	name := filepath.Join(path, CommitLogName)
	c.log.Debugf("using file %s", name)
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
		c.log.Errorf("broken file size, %s bytes extra", extra)
		if err := c.fd.Truncate(stat.Size() - extra); err != nil {
			return err
		}
		stat, _ = c.fd.Stat()
	}
	nFrames := stat.Size() / int64(CommitFrameSize)

	// init frames
	switch nFrames {
	case 0:
		// empty file, create a new frame
		c.tail = NewCommitFrame(0).WithLogger(c.log)
	case 1:
		// load tail frame
		c.tail, err = c.LoadFrame(0)
	default:
		// load last two frames
		c.last, err = c.LoadFrame(nFrames - 2)
		if err == nil {
			c.tail, err = c.LoadFrame(nFrames - 1)
		}
	}

	// truncate file to zero (we don't know at which LSN to start recovery mid-file
	// because commits may happen in arbitrary order)
	if err != nil {
		if !errors.Is(err, ErrChecksum) && !errors.Is(err, ErrShortFrame) {
			return err
		}
		c.log.Errorf("recovering after %v", err)
		if err := c.fd.Truncate(0); err != nil {
			return err
		}
		if c.tail != nil {
			c.tail.Close()
			c.tail = NewCommitFrame(0).WithLogger(c.log)
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
	if err := c.Recover(c.checkpoint, wal); err != nil {
		return err
	}

	return nil
}

func (c *CommitLog) Close() error {
	err := c.Sync()
	if err != nil {
		c.log.Errorf("sync on close: %v", err)
	}
	err = c.fd.Close()
	if err != nil {
		c.log.Errorf("close: %v", err)
	}
	c.fd = nil
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
		frame, err := c.LoadFrame(int64(xid >> CommitFrameShift))
		if err != nil {
			return false, err
		}
		c.last = frame
	}
	return c.last.IsCommitted(xid), nil
}

func (c *CommitLog) Append(xid uint64, lsn LSN) error {
	c.checkpoint = max(c.checkpoint, lsn)

	// txid is in tail frame
	if c.tail.Contains(xid) {
		c.tail.Append(xid, lsn)
		return nil
	}

	// txid is after tail frame (create new tail and roll last)
	if xid > c.tail.Xmax() {
		// prevent gaps in the frame sequence
		if xid-c.tail.Xmax() > CommitFramePayloadSize<<3 {
			return ErrTxIdTooLarge
		}
		if c.last != nil {
			if err := c.last.WriteTo(c.fd); err != nil {
				return err
			}
			c.last.Close()
		}
		c.last = c.tail
		c.tail = NewCommitFrame(int64(xid >> CommitFrameShift)).WithLogger(c.log)
		c.tail.Append(xid, lsn)
		return nil
	}

	// txid is before tail frame, check last and potentially load another frame
	if c.last != nil && !c.last.Contains(xid) {
		if err := c.last.WriteTo(c.fd); err != nil {
			return err
		}
		c.last.Close()
		c.last = nil
	}
	if c.last == nil {
		frame, err := c.LoadFrame(int64(xid >> CommitFrameShift))
		if err != nil {
			return err
		}
		c.last = frame
	}
	c.last.Append(xid, lsn)
	return nil
}

func (c *CommitLog) LoadFrame(id int64) (*CommitFrame, error) {
	f := NewCommitFrame(id).WithLogger(c.log)
	err := f.ReadFrom(c.fd)
	if err != nil {
		f.Close()
		return nil, err
	}
	return f, nil
}

func (c *CommitLog) Recover(lsn LSN, wal *Wal) error {
	// read wal starting at last checkpoint and add all commits
	c.log.Debugf("replay from lsn %d", lsn)
	r := wal.NewReader().WithType(RecordTypeCommit)
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
		err = c.Append(rec.TxID, rec.Lsn)
		if err != nil {
			break
		}
	}
	if err == io.EOF {
		err = nil
	}
	return err
}
