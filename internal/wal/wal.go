// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package wal

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/echa/log"

	"blockwatch.cc/knoxdb/internal/hash/xxhash"
	"blockwatch.cc/knoxdb/pkg/util"
)

var LE = binary.LittleEndian

const (
	WAL_DIR_MODE = 0755
)

type RecoveryMode byte

const (
	RecoveryModeFail RecoveryMode = iota
	RecoveryModeSkip
	RecoveryModeTruncate
	RecoveryModeIgnore
)

var (
	recoveryModeNames    = "fail_skip_truncate_ignore"
	recoveryModeNamesOfs = [...]int{0, 5, 10, 19, 26}
)

func (m RecoveryMode) IsValid() bool {
	return m <= RecoveryModeIgnore
}

func (m RecoveryMode) String() string {
	return recoveryModeNames[recoveryModeNamesOfs[m] : recoveryModeNamesOfs[m+1]-1]
}

func ParseRecoveryMode(s string) (RecoveryMode, error) {
	for m := RecoveryModeFail; m <= RecoveryModeIgnore; m++ {
		if s == m.String() {
			return m, nil
		}
	}
	return 0, fmt.Errorf("invalid recovery mode %q", s)
}

func (t *RecoveryMode) Set(s string) error {
	m, err := ParseRecoveryMode(s)
	if err == nil {
		*t = m
	}
	return err
}

type WalOptions struct {
	Seed           uint64
	Path           string
	MaxSegmentSize int
	RecoveryMode   RecoveryMode
	Logger         log.Logger
}

var DefaultOptions = WalOptions{
	Path:           "",
	MaxSegmentSize: 1 << 20, // 1MB
	RecoveryMode:   RecoveryModeFail,
	Logger:         log.Disabled,
}

func (o WalOptions) IsValid() bool {
	return len(o.Path) > 0 && o.MaxSegmentSize >= SEG_FILE_MINSIZE && o.MaxSegmentSize <= SEG_FILE_MAXSIZE
}

func (o WalOptions) Merge(o2 WalOptions) WalOptions {
	o.Path = util.NonZero(o2.Path, o.Path)
	o.MaxSegmentSize = util.NonZero(o2.MaxSegmentSize, o.MaxSegmentSize)
	o.RecoveryMode = util.NonZero(o2.RecoveryMode, o.RecoveryMode)
	o.Seed = o2.Seed
	if o2.Logger != nil {
		o.Logger = o2.Logger
	}
	return o
}

type Wal struct {
	mu     sync.RWMutex
	opts   WalOptions
	active *segment
	wr     *bufio.Writer
	csum   uint64
	hash   hash.Hash64
	lsn    LSN
	log    log.Logger
}

func Create(opts WalOptions) (*Wal, error) {
	opts = DefaultOptions.Merge(opts)
	if !opts.IsValid() {
		return nil, ErrInvalidWalOption
	}

	// create directory
	err := os.MkdirAll(opts.Path, WAL_DIR_MODE)
	if err != nil {
		return nil, err
	}

	wal := &Wal{
		opts: opts,
		wr:   bufio.NewWriterSize(nil, BufferSize),
		hash: xxhash.New(),
		csum: opts.Seed,
		lsn:  0,
		log:  opts.Logger,
	}

	// create active segment
	wal.active, err = wal.createSegment(0)
	if err != nil {
		return nil, err
	}
	wal.wr.Reset(wal.active)

	return wal, nil
}

func possibleMaxLsn(opts WalOptions) (maxLsn LSN, err error) {
	opts.Logger.Debugf("wal: walking %s for possible highest segment file", opts.Path)
	var last fs.FileInfo
	err = filepath.Walk(opts.Path, func(path string, d fs.FileInfo, err error) error {
		if filepath.Ext(d.Name()) == SEG_FILE_SUFFIX {
			last = d
		}
		if d.IsDir() && opts.Path != path {
			return filepath.SkipDir
		}
		return nil
	})
	if err != nil {
		return
	}
	if last != nil {
		name := last.Name()
		name = strings.TrimSuffix(name, filepath.Ext(name))
		id, err := strconv.ParseInt(name, 10, 0)
		if err != nil {
			return 0, err
		}
		maxLsn = LSN(id*int64(opts.MaxSegmentSize) + last.Size())
	}
	return maxLsn, nil
}

func Open(lsn LSN, opts WalOptions) (*Wal, error) {
	opts = DefaultOptions.Merge(opts)
	if !opts.IsValid() {
		return nil, ErrInvalidWalOption
	}

	// guess possible max lsn based on segment names
	// used for only validating checksum
	// actual max lsn will be set after
	maxLsn, err := possibleMaxLsn(opts)
	if err != nil {
		return nil, err
	}

	wal := &Wal{
		opts: opts,
		wr:   bufio.NewWriterSize(nil, BufferSize),
		hash: xxhash.New(),
		csum: opts.Seed,
		lsn:  maxLsn,
		log:  opts.Logger,
	}
	wal.log.Debugf("wal: verifying from LSN %d", lsn)

	r := wal.NewReader()
	defer r.Close()

	// validate wal contents starting at LSN (must be start or a checkpoint)
	if err = r.Seek(lsn); err != nil {
		return nil, err
	}

	// walk all records after the checkpoint and validate checksums
scan:
	for {
		var rec *Record
		rec, err = r.Next()
		switch {
		case err == nil:
			// next record
		case err == io.EOF:
			break scan
		case errors.Is(err, ErrInvalidRecord):
			if err2 := wal.tryRecover(lsn, err); err2 != nil {
				return nil, err2
			}
			break scan
		default:
			return nil, err
		}

		// keep last good lsn
		lsn = lsn.Add(HeaderSize + rec.BodySize())
	}

	// after successful init check (or truncate)
	wal.lsn = lsn
	wal.csum = r.Checksum()
	wal.log.Debugf("wal: last record LSN %d", wal.lsn)

	// open active segment
	wal.active, err = wal.openSegment(wal.lsn.Segment(opts.MaxSegmentSize), true)
	if err != nil {
		return nil, err
	}
	wal.wr.Reset(wal.active)

	return wal, nil
}

func (w *Wal) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	err := w.wr.Flush()
	err2 := w.active.Close()
	if err == nil {
		err = err2
	}
	w.active = nil
	w.wr = nil
	w.csum = 0
	w.hash = nil
	w.lsn = 0
	w.log = nil
	return err
}

func (w *Wal) ForceClose() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	err := w.active.ForceClose()
	w.active = nil
	w.wr = nil
	w.csum = 0
	w.hash = nil
	w.lsn = 0
	w.log = nil
	return err
}

func (w *Wal) IsClosed() bool {
	return w.hash == nil
}

func (w *Wal) Len() int64 {
	return int64(w.lsn)
}

func (w *Wal) Write(rec *Record) (LSN, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.write(rec)
}

func (w *Wal) WriteAndSync(rec *Record) (LSN, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	lsn, err := w.write(rec)
	if err != nil {
		return 0, err
	}
	err = w.sync()
	return lsn, err
}

func (w *Wal) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.sync()
}

func (w *Wal) sync() error {
	if err := w.wr.Flush(); err != nil {
		return err
	}
	return w.active.Sync()
}

func (w *Wal) write(rec *Record) (LSN, error) {
	if w.IsClosed() {
		return 0, ErrWalClosed
	}
	if rec == nil {
		return 0, ErrInvalidRecord
	}
	if err := rec.Validate(); err != nil {
		return 0, err
	}

	w.log.Trace(rec.Trace)

	// create header
	head := rec.Header()

	// calculate chained checksum
	csum := checksum(w.hash, w.csum, &head, rec.Data)
	head.SetChecksum(csum)

	// remember current lsn and truncate on failed write
	lsn := w.lsn

	// write header
	_, err := w.writeBuffer(head[:])
	if err != nil {
		if err2 := w.truncate(lsn); err2 != nil {
			return 0, err2
		}
		return 0, err
	}

	// write body
	for _, v := range rec.Data {
		_, err = w.writeBuffer(v)
		if err != nil {
			if err2 := w.truncate(lsn); err2 != nil {
				return 0, err2
			}
			return 0, err
		}
	}

	// all ok, update csum and next lsn
	w.csum = csum
	w.lsn = lsn.Add(HeaderSize + rec.BodySize())

	return lsn, nil
}

// must hold exclusive lock
func (w *Wal) writeBuffer(buf []byte) (int, error) {
	space := w.active.Cap() - (BufferSize - w.wr.Available())
	if space >= len(buf) {
		return w.wr.Write(buf)
	}

	// split and roll when active segment has not enough space
	var count int
	for {
		n, err := w.wr.Write(buf[:min(space, len(buf))])
		if err != nil {
			return count, err
		}
		buf = buf[n:]
		count += n

		// stop when
		if len(buf) == 0 {
			break
		}

		// open next segment
		next, err := w.createSegment(w.active.Id() + 1)
		if err != nil {
			return count, err
		}

		// close active
		err = w.wr.Flush()
		if err != nil {
			return count, err
		}
		err = w.active.Close()
		if err != nil {
			return count, err
		}

		// reinit writer and capacity
		w.active = next
		w.wr.Reset(next)
		space = next.Cap()
	}

	return count, nil
}

// must hold exclusive lock
func (w *Wal) truncate(lsn LSN) error {
	w.log.Debugf("wal: truncating to LSN %d", lsn)

	// close active segment
	var reloadActive bool
	if w.active != nil {
		if err := w.wr.Flush(); err != nil {
			return err
		}
		w.wr.Reset(nil)
		if err := w.active.Close(); err != nil {
			return err
		}
		w.active = nil
		reloadActive = true
	}

	// open directory
	dir, err := os.Open(w.opts.Path)
	if err != nil {
		return err
	}
	defer dir.Close()

	sid := lsn.Segment(w.opts.MaxSegmentSize)
	ofs := lsn.Offset(w.opts.MaxSegmentSize)

	// find the largest segment file id
	next := sid
	for {
		_, err := os.Stat(w.segmentName(next + 1))
		if err != nil {
			if os.IsNotExist(err) {
				break
			}
			return err
		}
		next++
	}

	// remove segment files in reverse order, this way we can continue after a
	// crash during file removal
	for next > sid {
		name := w.segmentName(next)
		w.log.Debugf("wal: unlink %s", name)
		err := os.Remove(name)
		if err != nil {
			if os.IsNotExist(err) {
				break
			}
			return err
		}
		next--
	}

	// last, truncate the broken segment to LSN offset
	if err := os.Truncate(w.segmentName(sid), ofs); err != nil {
		return err
	}

	// sync dir
	if err := dir.Sync(); err != nil {
		return err
	}

	// update wal state
	w.lsn = lsn

	// open active segment again
	if reloadActive {
		w.active, err = w.openSegment(lsn.Segment(w.opts.MaxSegmentSize), true)
		if err != nil {
			return err
		}
		w.wr.Reset(w.active)
	}

	return nil
}

// handle corruption
func (w *Wal) tryRecover(lsn LSN, err error) error {
	w.log.Errorf("wal: try recover: %v", err)
	switch w.opts.RecoveryMode {
	case RecoveryModeTruncate:
		// truncate to last good LSN
		err := w.truncate(lsn)
		if err != nil {
			w.log.Errorf("wal: truncate: %v", err)
		}
		return err
	case RecoveryModeFail:
		return err
	case RecoveryModeSkip, RecoveryModeIgnore:
		return nil
	default:
		return err
	}
}
