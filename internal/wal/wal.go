// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package wal

import (
	"encoding/binary"
	"hash"

	"blockwatch.cc/knoxdb/internal/types"
)

var LE = binary.LittleEndian

type WalReader interface {
	Seek(LSN) error
	Next() (*Record, error)
	Close() error
	// NextN([]*Record) error

	WithType(RecordType) WalReader
	WithTag(types.ObjectTag) WalReader
	WithEntity(uint64) WalReader
	WithTxID(uint64) WalReader
}

type WalOptions struct {
	Seed           uint64
	Path           string
	MaxSegmentSize int
}

type Wal struct {
	opts   WalOptions
	active *segment
	csum   uint64
	hash   hash.Hash64
}

func Create(opts WalOptions) (*Wal, error) {
	// create directory
	// create active wal segment
	return &Wal{}, nil
}

func Open(opts WalOptions) (*Wal, error) {
	// try open directory
	// set exclusive lock
	// open last segment file
	// read hash of last record and init w.csum

	return &Wal{}, nil
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
	// Note: this is only an example to show how a record can be written
	//
	// create header
	var head [28]byte
	head[0] = byte(rec.Type)
	head[1] = byte(rec.Tag)
	LE.PutUint64(head[2:], rec.Entity)
	LE.PutUint64(head[10:], rec.TxID)
	LE.PutUint32(head[16:], uint32(len(rec.Data)))

	// calculate chained checksum
	w.hash.Reset()
	var b [8]byte
	LE.PutUint64(b[:], w.csum)
	w.hash.Write(b[:])
	w.hash.Write(head[:20])
	w.hash.Write(rec.Data)
	w.hash.Sum(head[20:])

	// remember current size and truncate on failed write
	segsz := w.active.pos

	// calculate the LSN
	lsn := LSN(w.active.id*w.opts.MaxSegmentSize + w.active.pos)

	// split record when active segment has not enough space
	spaceLeft := w.opts.MaxSegmentSize - w.active.pos
	switch {
	case spaceLeft < 28:
		// not even the header fits, then roll active and write
		// the full record into the next active segment

		// TODO: maybe pad the current active segment to full size before close

		err := w.nextSegment()
		if err != nil {
			return 0, err
		}
		lsn = LSN(w.active.id + w.active.pos)

		// write header
		_, err = w.active.Write(head[:])
		if err != nil {
			_ = w.active.Truncate(segsz)
			return 0, err
		}

		// write data
		_, err = w.active.Write(rec.Data)
		if err != nil {
			_ = w.active.Truncate(segsz)
			return 0, err
		}

	case spaceLeft < len(rec.Data)+28:
		// only a part of the data fits, write header and whatever
		// body data fits, then continue writing the remainder to
		// the next active segment

		// write header
		_, err := w.active.Write(head[:])
		if err != nil {
			_ = w.active.Truncate(segsz)
			return 0, err
		}

		// write first data part
		_, err = w.active.Write(rec.Data[:spaceLeft-28])
		if err != nil {
			_ = w.active.Truncate(segsz)
			return 0, err
		}

		// roll active segment
		err = w.nextSegment()
		if err != nil {
			return 0, err
		}

		// write second data part
		_, err = w.active.Write(rec.Data[spaceLeft-28:])
		if err != nil {
			_ = w.active.Truncate(segsz)
			return 0, err
		}

	default:
		// everything fits

		// write header
		_, err := w.active.Write(head[:])
		if err != nil {
			_ = w.active.Truncate(segsz)
			return 0, err
		}

		// write data
		_, err = w.active.Write(rec.Data)
		if err != nil {
			_ = w.active.Truncate(segsz)
			return 0, err
		}
	}

	// update state
	w.csum = w.hash.Sum64()

	// TODO: mix in the segment id
	return lsn, nil
}

func (w *Wal) NewReader() WalReader {
	return &Reader{
		wal: w,
	}
}

// rolls the active segement
func (w *Wal) nextSegment() error {
	// close and fsync the current active segment
	// create new segment file
	// fsync the directory
	return nil
}
