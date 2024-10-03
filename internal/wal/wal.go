// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc, abdul@blockwatch.cc

package wal

import (
	"encoding/binary"
	"errors"
	"hash"
	"io"
	"os"
	"path/filepath"

	"blockwatch.cc/knoxdb/internal/hash/xxhash"
	"blockwatch.cc/knoxdb/internal/types"
)

var (
	LE = binary.LittleEndian
)

const (
	HeaderSize     = 30
	MinSegmentSize = 8 << 10
)

type WalReader interface {
	Seek(LSN) error
	Next() (*Record, error)
	Close() error
	// NextN([]*Record) error

	Checksum() uint64
	ReadSegmentId() uint64
	ReadPosition() int64

	WithType(RecordType) WalReader
	WithTag(types.ObjectTag) WalReader
	WithEntity(uint64) WalReader
	WithTxID(uint64) WalReader
}

type WalOptions struct {
	Seed           uint64
	Path           string
	MaxSegmentSize int
	ReadOnly       bool
}

func (opt WalOptions) IsValid() bool {
	return len(opt.Path) > 0 && opt.MaxSegmentSize > 0
}

type Wal struct {
	opts   WalOptions
	active *segment
	csum   uint64
	hash   hash.Hash64
	sz     int64
}

func Create(opts WalOptions) (*Wal, error) {
	if !opts.IsValid() {
		return nil, ErrInvalidWalOption
	}
	// create directory
	err := os.MkdirAll(opts.Path, 0750)
	if err != nil && !errors.Is(err, os.ErrExist) {
		return nil, err
	}
	// create active wal segment
	// use the seed as first checksum
	id := NewLSN(0, int64(opts.MaxSegmentSize), 0)
	seg, err := createSegment(id, opts)
	if err != nil {
		return nil, err
	}
	return &Wal{
		opts:   opts,
		active: seg,
		hash:   xxhash.New(),
		csum:   opts.Seed,
	}, nil
}

func Open(id LSN, opts WalOptions) (*Wal, error) {
	if !opts.IsValid() {
		return nil, ErrInvalidWalOption
	}
	// try open directory
	// set exclusive lock
	// open last segment file
	// read hash of last record and init w.csum
	seg, err := openSegment(id, opts)
	if err != nil {
		return nil, err
	}

	// if we have been able to load the segment LSN
	// we can assume we the minimum size
	// of the wal is LSN - 1
	wal := &Wal{
		opts:   opts,
		active: seg,
		hash:   xxhash.New(),
		csum:   opts.Seed,
		sz:     int64(id) - 1,
	}
	var record *Record
	r := wal.NewReader()
	defer r.Close()
	for {
		record, err = r.Next()
		if err != nil {
			switch {
			case errors.Is(err, ErrChecksum):
				name := generateFilename(int64(r.ReadSegmentId()))
				filename := filepath.Join(opts.Path, name)
				f, err := os.OpenFile(filename, os.O_RDWR, 0666)
				if err != nil {
					return nil, err
				}
				defer f.Close()
				err = f.Truncate(r.ReadPosition() - HeaderSize + int64(len(record.Data)))
				if err != nil {
					return nil, err
				}
				// removing all segments after
				// truncated file as they are possibly corrupted
				dirEntries, err := os.ReadDir(opts.Path)
				if err != nil {
					return nil, err
				}
				seen := false
				for _, entry := range dirEntries {
					if !seen {
						entryName := entry.Name()
						if entryName == filename {
							seen = true
						}
						continue
					}
					err = os.Remove(entry.Name())
					if err != nil {
						return nil, err
					}

				}
				var dir *os.File
				dir, err = os.Open(opts.Path)
				if err != nil {
					return nil, err
				}
				defer dir.Close()
				if err = dir.Sync(); err != nil {
					return nil, err
				}
				fallthrough
			case errors.Is(err, io.EOF):
				err = nil
			}
			break
		}
	}
	if record != nil {
		wal.csum = r.Checksum()
	}
	return wal, err
}

func (w *Wal) Close() error {
	err := w.active.Close()
	w.active = nil
	return err
}

func (w *Wal) Sync() error {
	return w.active.Sync()
}

func (w *Wal) Write(rec *Record) (LSN, error) {
	if w.active == nil {
		return 0, ErrClosed
	}
	if !rec.IsValid() {
		return 0, ErrInvalidRecord
	}
	// write record to active segment
	// create header
	var head [HeaderSize]byte
	head[0] = byte(rec.Type)
	head[1] = byte(rec.Tag)
	LE.PutUint64(head[2:], rec.Entity)
	LE.PutUint64(head[10:], rec.TxID)
	LE.PutUint32(head[18:], uint32(len(rec.Data)))

	// calculate chained checksum
	w.hash.Reset()
	var b [8]byte
	LE.PutUint64(b[:], w.csum)
	w.hash.Write(b[:])
	w.hash.Write(head[:22])
	w.hash.Write(rec.Data)
	csum := w.hash.Sum64()
	LE.PutUint64(head[22:], csum)
	// remember current size and truncate on failed write
	// calculate the LSN
	lsn := NewLSN(w.active.id, int64(w.opts.MaxSegmentSize), w.active.pos)

	data := head[:]
	dataPos := int64(0)
	isHeaderWritten := false
	sizeOfRemainingDataToWrite := int64(HeaderSize)

	for {
		if w.opts.MaxSegmentSize == int(w.active.pos) {
			// make sure active file synced first
			err := w.nextSegment()
			if err != nil {
				return 0, err
			}
		}

		spaceLeft := int64(w.opts.MaxSegmentSize) - w.active.pos
		sizeOfDataToWriteToCurrentFile := sizeOfRemainingDataToWrite
		if sizeOfRemainingDataToWrite > spaceLeft {
			sizeOfDataToWriteToCurrentFile = spaceLeft
		}
		n, err := w.writeData(data[dataPos : dataPos+sizeOfDataToWriteToCurrentFile])
		if err != nil {
			return 0, err
		}
		w.sz += int64(n)
		sizeOfRemainingDataToWrite -= sizeOfDataToWriteToCurrentFile
		dataPos += sizeOfDataToWriteToCurrentFile

		if sizeOfRemainingDataToWrite == 0 {
			if isHeaderWritten {
				break
			}

			isHeaderWritten = true
			data = rec.Data
			dataPos = int64(0)
			sizeOfRemainingDataToWrite = int64(len(rec.Data))
		}
	}

	// update state
	w.csum = csum

	// TODO: mix in the segment id
	return lsn, nil
}

func (w *Wal) NewReader() WalReader {
	return &Reader{
		bufferedReader: newBufferedReader(w),
		prevCsum:       w.opts.Seed,
	}
}

// rolls the active segement
func (w *Wal) nextSegment() error {
	// close and fsync the current active segment
	// create new segment file
	// fsync the directory
	nextId := w.active.id + 1
	if err := w.active.Sync(); err != nil {
		return err
	}
	if err := w.active.Close(); err != nil {
		return err
	}
	lsn := NewLSN(nextId, int64(w.opts.MaxSegmentSize), 0)
	seg, err := createSegment(lsn, w.opts)
	if err != nil {
		return err
	}
	w.active = seg
	return nil
}

func (w *Wal) writeData(data []byte) (int, error) {
	pos, err := w.active.Write(data)
	if err != nil {
		_ = w.active.Truncate(w.active.pos)
		return 0, err
	}
	return pos, err
}
