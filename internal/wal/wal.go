// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package wal

import (
	"encoding/binary"
	"hash"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/echa/log"
	"github.com/gofrs/flock"

	"blockwatch.cc/knoxdb/internal/hash/xxhash"
	"blockwatch.cc/knoxdb/pkg/util"
)

var LE = binary.LittleEndian

const (
	WAL_LOCK_NAME = "wal.lock"
	WAL_DIR_MODE  = 0755
)

type WalOptions struct {
	Seed           uint64
	Path           string
	MaxSegmentSize int
	Logger         log.Logger
}

var DefaultOptions = WalOptions{
	Path:           "./wal",
	MaxSegmentSize: 1 << 20, // 1MB
	Logger:         log.Disabled,
}

func (o WalOptions) IsValid() bool {
	return len(o.Path) > 0 && o.MaxSegmentSize >= SEG_FILE_MINSIZE && o.MaxSegmentSize <= SEG_FILE_MAXSIZE
}

func (o WalOptions) Merge(o2 WalOptions) WalOptions {
	o.Path = util.NonZero(o2.Path, o.Path)
	o.MaxSegmentSize = util.NonZero(o2.MaxSegmentSize, o.MaxSegmentSize)
	o.Seed = o2.Seed
	if o2.Logger != nil {
		o.Logger = o2.Logger
	}
	return o
}

type Wal struct {
	mu     sync.RWMutex
	lock   *flock.Flock
	opts   WalOptions
	active *segment
	xlog   *CommitLog
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
	if err := os.MkdirAll(opts.Path, WAL_DIR_MODE); err != nil {
		return nil, err
	}

	// set exclusive directory lock
	lock := flock.New(filepath.Join(opts.Path, WAL_LOCK_NAME))
	_, err := lock.TryLock()
	if err != nil {
		return nil, err
	}

	// cleanup lock file on error
	defer func() {
		if err != nil {
			lock.Unlock()
		}
	}()

	wal := &Wal{
		lock: lock,
		opts: opts,
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

	// init xlog
	wal.xlog = NewCommitLog(wal)
	if err = wal.xlog.Open(); err != nil {
		wal.Close()
		return nil, err
	}

	return wal, nil
}

func Open(lsn LSN, opts WalOptions) (*Wal, error) {
	opts = DefaultOptions.Merge(opts)
	if !opts.IsValid() {
		return nil, ErrInvalidWalOption
	}

	// set exclusive directory lock
	lock := flock.New(filepath.Join(opts.Path, WAL_LOCK_NAME))
	_, err := lock.TryLock()
	if err != nil {
		return nil, err
	}

	// cleanup lock file on error
	defer func() {
		if err != nil {
			lock.Unlock()
		}
	}()

	wal := &Wal{
		lock: lock,
		opts: opts,
		hash: xxhash.New(),
		csum: opts.Seed,
		log:  opts.Logger,
	}
	wal.log.Debugf("wal: verifying from LSN %d", lsn)

	r := wal.NewReader()
	defer r.Close()

	// validate wal contents starting at LSN (must be a checkpoint)
	if err = r.Seek(lsn); err != nil {
		return nil, err
	}

	// init first good lsn (following the checkpoint)
	wal.lsn = r.Lsn()

	// walk all records after the checkpoint and validate checksums
	for {
		var rec *Record
		rec, err = r.Next()
		if err != nil {
			if err != io.EOF {
				wal.log.Errorf("wal: init: %v", err)

				// truncate to last good LSN
				if err := wal.truncate(wal.lsn); err != nil {
					wal.log.Errorf("wal: truncate: %v", err)
					return nil, err
				}

				return nil, err
			}
			break
		}
		wal.lsn = wal.lsn.Add(HeaderSize + len(rec.Data))
	}
	wal.csum = r.Checksum()
	wal.log.Debugf("wal: last record LSN %d", wal.lsn)

	// open active segment
	wal.active, err = wal.openSegment(wal.lsn.Segment(opts.MaxSegmentSize), true)
	if err != nil {
		return nil, err
	}

	// init xlog
	wal.log.Debugf("wal: open xlog")
	wal.xlog = NewCommitLog(wal)
	if err = wal.xlog.Open(); err != nil {
		wal.Close()
		return nil, err
	}

	return wal, nil
}

func (w *Wal) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	err := w.active.Close()
	w.active = nil
	w.xlog.Close()
	w.xlog = nil
	w.lock.Close()
	w.lock = nil
	w.csum = 0
	w.hash = nil
	w.lsn = 0
	return err
}

func (w *Wal) Sync() error {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.active.Sync()
}

func (w *Wal) Write(rec *Record) (LSN, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	// create header
	head := rec.Header()

	// calculate chained checksum
	csum := checksum(w.hash, w.csum, &head, rec.Data)
	head.SetChecksum(csum)

	// remember current lsn and truncate on failed write
	lsn := w.lsn

	// write header
	_, err := w.write(head[:])
	if err != nil {
		if err2 := w.truncate(lsn); err2 != nil {
			return 0, err2
		}
		return 0, err
	}

	// write body
	_, err = w.write(rec.Data)
	if err != nil {
		if err2 := w.truncate(lsn); err2 != nil {
			return 0, err2
		}
		return 0, err
	}

	// all ok, update csum and next lsn
	w.csum = csum
	w.lsn = lsn.Add(HeaderSize + len(rec.Data))

	// write xlog on commits
	if rec.Type == RecordTypeCommit {
		if err := w.xlog.Append(rec); err != nil {
			w.log.Errorf("wal: xlog: %v", err)
		}
	}

	return lsn, nil
}

// must hold exclusive lock
func (w *Wal) write(buf []byte) (int, error) {
	space := w.active.Cap()
	if space >= len(buf) {
		return w.active.Write(buf)
	}

	// split and roll when active segment has not enough space
	var count int
	for len(buf) > 0 {
		n, err := w.active.Write(buf[:space])
		if err != nil {
			return count, err
		}
		buf = buf[n:]
		count += n

		// open next segment
		next, err := w.createSegment(w.active.Id() + 1)
		if err != nil {
			return count, err
		}

		// close active
		err = w.active.Close()
		if err != nil {
			return count, err
		}
		w.active = next

		// reinit capacity
		space = w.active.Cap()
	}

	return count, nil
}

// must hold exclusive lock
func (w *Wal) truncate(lsn LSN) error {
	w.log.Debugf("wal: truncating to LSN %d", lsn)

	// close active segment
	var reloadActive bool
	if w.active != nil {
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
	}

	return nil
}
