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
	"sync"
	"sync/atomic"
	"time"

	"github.com/echa/log"

	"blockwatch.cc/knoxdb/internal/hash/xxhash64"
	"blockwatch.cc/knoxdb/pkg/util"
)

var LE = binary.LittleEndian

const (
	WAL_DIR_MODE          = 0755
	WAL_MAX_SYNC_REQUESTS = 128
	WAL_BUFFER_SIZE       = 1 << 19 // 512k
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
	ReadOnly       bool
	SyncDelay      time.Duration
	RecoveryMode   RecoveryMode
	Logger         log.Logger
}

var DefaultOptions = WalOptions{
	Path:           "",
	SyncDelay:      time.Second, // sync at most each second
	MaxSegmentSize: 1 << 20,     // 1MB
	ReadOnly:       false,
	RecoveryMode:   RecoveryModeFail, // default in read-only mode
	Logger:         log.Disabled,
}

func (o WalOptions) IsValid() bool {
	return len(o.Path) > 0 && o.MaxSegmentSize >= SEG_FILE_MINSIZE && o.MaxSegmentSize <= SEG_FILE_MAXSIZE
}

func (o WalOptions) Merge(o2 WalOptions) WalOptions {
	o.Path = util.NonZero(o2.Path, o.Path)
	o.SyncDelay = util.NonZero(o2.SyncDelay, o.SyncDelay)
	o.MaxSegmentSize = util.NonZero(o2.MaxSegmentSize, o.MaxSegmentSize)
	o.ReadOnly = o2.ReadOnly
	if !o.ReadOnly {
		o.RecoveryMode = util.NonZero(o2.RecoveryMode, o.RecoveryMode)
	}
	o.Seed = o2.Seed
	if o2.Logger != nil {
		o.Logger = o2.Logger
	}
	return o
}

type Wal struct {
	mu      sync.RWMutex
	wg      sync.WaitGroup
	opts    WalOptions
	active  *segment
	wr      *bufio.Writer
	req     chan *util.Future
	close   chan struct{}
	csum    uint64
	hash    hash.Hash64
	nextLsn LSN
	lastLsn LSN
	log     log.Logger
	nBytes  int64
}

func Create(opts WalOptions) (*Wal, error) {
	opts = DefaultOptions.Merge(opts)
	if !opts.IsValid() {
		return nil, ErrInvalidWalOption
	}
	opts.Logger.Debugf("wal: creating files at %s", opts.Path)

	// create directory
	err := os.MkdirAll(opts.Path, WAL_DIR_MODE)
	if err != nil {
		return nil, err
	}

	wal := &Wal{
		opts:    opts,
		wr:      bufio.NewWriterSize(nil, WAL_BUFFER_SIZE),
		hash:    xxhash64.New(),
		csum:    opts.Seed,
		req:     make(chan *util.Future, WAL_MAX_SYNC_REQUESTS),
		close:   make(chan struct{}),
		nextLsn: 0,
		lastLsn: 0,
		log:     opts.Logger,
	}

	// create active segment
	wal.active, err = wal.createSegment(0)
	if err != nil {
		return nil, err
	}
	wal.wr.Reset(wal.active)

	// when all is ok, launch background sync
	wal.runSyncThread()

	return wal, nil
}

func Open(lsn LSN, opts WalOptions) (*Wal, error) {
	opts = DefaultOptions.Merge(opts)
	if !opts.IsValid() {
		return nil, ErrInvalidWalOption
	}
	opts.Logger.Debugf("wal: open files at %s", opts.Path)

	// guess possible min/max lsn based on segment names
	// used for only validating checksum
	// actual max lsn will be set after
	minLsn, maxLsn, err := possibleMaxLsn(opts)
	if err != nil {
		return nil, err
	}

	wal := &Wal{
		opts:    opts,
		wr:      bufio.NewWriterSize(nil, WAL_BUFFER_SIZE),
		hash:    xxhash64.New(),
		req:     make(chan *util.Future, WAL_MAX_SYNC_REQUESTS),
		close:   make(chan struct{}),
		csum:    opts.Seed,
		nextLsn: maxLsn,
		log:     opts.Logger,
		nBytes:  int64(maxLsn - minLsn),
	}
	wal.log.Debugf("wal: verifying from LSN 0x%x", lsn)

	r := wal.NewReader()
	defer r.Close()

	// validate wal contents starting at LSN (must be start or a checkpoint)
	if err = r.Seek(lsn); err != nil {
		return nil, err
	}

	// walk all records after the checkpoint and validate checksums
	last := lsn
scan:
	for {
		var rec *Record
		rec, err = r.Next()
		switch {
		case err == nil:
			// next record
		case err == io.EOF:
			lsn = maxLsn
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
		last = lsn
		lsn = lsn.Add(HeaderSize + rec.BodySize())
	}

	// after successful init check (or truncate)
	wal.nextLsn = lsn
	wal.lastLsn = last
	wal.csum = r.Checksum()
	wal.log.Debugf("wal: last record LSN 0x%x, next LSN 0x%x", wal.lastLsn, wal.nextLsn)

	// open active segment
	wal.active, err = wal.openSegment(wal.nextLsn.Segment(opts.MaxSegmentSize), !wal.opts.ReadOnly)
	if err != nil {
		return nil, err
	}
	wal.wr.Reset(wal.active)

	// when all is ok, launch background sync
	wal.runSyncThread()

	return wal, nil
}

func (w *Wal) Close() error {
	w.stopSyncThread()
	w.mu.Lock()
	defer w.mu.Unlock()
	var err error
	if !w.opts.ReadOnly {
		err = w.wr.Flush()
	}
	err2 := w.active.Close()
	if err == nil {
		err = err2
	}
	close(w.req)
	for fut := range w.req {
		fut.Close()
	}
	w.active = nil
	w.wr = nil
	w.csum = 0
	w.hash = nil
	w.nextLsn = 0
	w.lastLsn = 0
	w.log = nil
	w.nBytes = 0
	return err
}

func (w *Wal) ForceClose() error {
	w.stopSyncThread()
	w.mu.Lock()
	defer w.mu.Unlock()
	err := w.active.ForceClose()
	close(w.req)
	for fut := range w.req {
		fut.Close()
	}
	w.active = nil
	w.wr = nil
	w.csum = 0
	w.hash = nil
	w.nextLsn = 0
	w.lastLsn = 0
	w.log = nil
	w.nBytes = 0
	return err
}

func (w *Wal) IsClosed() bool {
	return w.hash == nil
}

func (w *Wal) Len() int64 {
	return int64(w.nextLsn)
}

func (w *Wal) Last() LSN {
	return w.lastLsn
}

func (w *Wal) Next() LSN {
	return w.nextLsn
}

func (w *Wal) Sync() error {
	w.mu.Lock()
	err := w.sync()
	w.mu.Unlock()
	// w.log.Debugf("wal: sync")
	return err
}

func (w *Wal) Schedule() *util.Future {
	fut := util.NewFuture()
	select {
	case <-w.close:
		fut.CloseErr(ErrWalClosed)
	case w.req <- fut:
		// may block when full
	}
	return fut
}

func (w *Wal) Write(rec *Record) (LSN, error) {
	w.mu.Lock()
	lsn, err := w.write(rec)
	w.mu.Unlock()
	if err == nil {
		rec.Lsn = lsn
		// w.log.Debugf("wal: write %s", rec)
	}
	return lsn, err
}

func (w *Wal) WriteAndSync(rec *Record) (LSN, error) {
	w.mu.Lock()
	lsn, err := w.write(rec)
	if err != nil {
		w.mu.Unlock()
		return 0, err
	}
	rec.Lsn = lsn
	// w.log.Debugf("wal: write_and_sync %s", rec)
	err = w.sync()
	w.mu.Unlock()
	return lsn, err
}

func (w *Wal) WriteAndSchedule(rec *Record) (LSN, *util.Future, error) {
	w.mu.Lock()
	lsn, err := w.write(rec)
	w.mu.Unlock()
	if err != nil {
		return 0, nil, err
	}
	rec.Lsn = lsn
	// w.log.Debugf("wal: write_and_schedule %s", rec)
	return lsn, w.Schedule(), nil
}

func (w *Wal) NumBytesSincdLastGC() int64 {
	return atomic.LoadInt64(&w.nBytes)
}

// remove all WAL segment files before lsn
func (w *Wal) GC(watermark LSN) error {
	if watermark > w.nextLsn {
		return ErrInvalidLSN
	}

	// cannot remove the active segment or drop any segment the
	// watermark points to. all removals must be strictly smaller segments.
	sid := watermark.Segment(w.opts.MaxSegmentSize) - 1
	if w.active.id == sid || sid < 0 {
		w.log.Tracef("wal: skip gc of active segment")
		return nil
	}

	// skip if we haven't made a segment worth of progress since last GC
	// (on startup we analyze segment files and set nBytes to the full range)
	if atomic.LoadInt64(&w.nBytes) < int64(w.opts.MaxSegmentSize) {
		w.log.Tracef("wal: skip gc due to too little progress")
		return nil
	}

	// walk in lexical order and remove segment files, stop at sid
	err := filepath.Walk(w.opts.Path, func(path string, d fs.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() && w.opts.Path != path {
			return filepath.SkipDir
		}
		name := d.Name()
		if filepath.Ext(name) != SEG_FILE_SUFFIX {
			return nil
		}
		id, err := decodeSegmentName(name)
		if err != nil {
			return err
		}
		if id > sid {
			return io.EOF
		}
		fname := w.segmentName(id)
		w.log.Infof("wal: gc removing segment %s", fname)
		return os.Remove(fname)
	})
	if err != nil && err != io.EOF {
		return err
	}

	// sync directory
	dir, err := os.Open(w.opts.Path)
	if err != nil {
		return err
	}
	err = dir.Sync()
	dir.Close()

	// reset write counter
	atomic.StoreInt64(&w.nBytes, 0)

	return err
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

	// create header
	head := rec.Header()

	// calculate chained checksum
	csum := checksum(w.hash, w.csum, &head, rec.Data)
	head.SetChecksum(csum)

	// remember current lsn and truncate on failed write
	lsn := w.nextLsn

	rec.Lsn = lsn
	w.log.Trace(rec.Trace)

	// write header
	sz, err := w.writeBuffer(head[:])
	if err != nil {
		if err2 := w.truncate(lsn); err2 != nil {
			return 0, err2
		}
		return 0, err
	}

	// write body
	for _, v := range rec.Data {
		n, err := w.writeBuffer(v)
		if err != nil {
			if err2 := w.truncate(lsn); err2 != nil {
				return 0, err2
			}
			return 0, err
		}
		sz += n
	}

	// all ok, update csum and next lsn
	w.csum = csum
	w.lastLsn = lsn
	w.nextLsn = lsn.Add(sz)
	atomic.AddInt64(&w.nBytes, int64(sz))

	return lsn, nil
}

// must hold exclusive lock
func (w *Wal) writeBuffer(buf []byte) (int, error) {
	space := w.active.Cap() - (WAL_BUFFER_SIZE - w.wr.Available())
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
	w.lastLsn = lsn // FIXME: should be start of last record
	w.nextLsn = lsn

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

func (w *Wal) runSyncThread() {
	var (
		queue   [WAL_MAX_SYNC_REQUESTS]*util.Future
		n       int
		tick    = time.NewTicker(w.opts.SyncDelay)
		closing bool
	)
	w.wg.Add(1)
	go func() {
		defer w.wg.Done()
		defer tick.Stop()
		for {
			select {
			case <-w.close:
				if n == 0 {
					return
				}
				closing = true

			case <-tick.C:
				// process waiting requests
				if n == 0 {
					continue
				}

			case fut := <-w.req:
				// record request if still valid
				if fut.IsValid() {
					queue[n] = fut
					n++
				}

				// only flush on tick, when req queue is full, or we're closing
				if n < WAL_MAX_SYNC_REQUESTS {
					continue
				}
			}

			// flush and sync under lock
			w.mu.Lock()
			err := w.sync()
			w.mu.Unlock()

			// signal to futures
			for _, fut := range queue[:n] {
				fut.CloseErr(err)
			}
			clear(queue[:n])
			n = 0

			if closing {
				return
			}
		}
	}()
}

func (w *Wal) stopSyncThread() {
	close(w.close)
	w.wg.Wait()
}

func possibleMaxLsn(opts WalOptions) (minLsn, maxLsn LSN, err error) {
	opts.Logger.Debugf("wal: walking %s for possible highest segment file", opts.Path)
	var (
		first, last string
		lastSz      int64
	)
	err = filepath.Walk(opts.Path, func(path string, d fs.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if filepath.Ext(d.Name()) == SEG_FILE_SUFFIX {
			last = d.Name()
			lastSz = d.Size()
			if first == "" {
				first = last
			}
		}
		if d.IsDir() && opts.Path != path {
			return filepath.SkipDir
		}
		return nil
	})
	if err != nil {
		return
	}
	opts.Logger.Debugf("wal: using last segment file %s (%d bytes)", last, lastSz)
	if last != "" {
		id, err := decodeSegmentName(last)
		if err != nil {
			return 0, 0, err
		}
		maxLsn = LSN(id*opts.MaxSegmentSize + int(lastSz))
	}
	if first != "" {
		id, err := decodeSegmentName(first)
		if err != nil {
			return 0, 0, err
		}
		minLsn = LSN(id * opts.MaxSegmentSize)

	}
	return minLsn, maxLsn, nil
}
