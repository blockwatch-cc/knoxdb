// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package wal

import (
	"io"

	"blockwatch.cc/knoxdb/internal/types"
)

type RecordFilter struct {
	Type   RecordType
	Tag    types.ObjectTag
	Entity uint64
	TxID   uint64
}

func (f *RecordFilter) Match(r *Record) bool {
	if f == nil {
		return true
	}
	if f.Type.IsValid() && r.Type != f.Type {
		return false
	}
	if f.Tag.IsValid() && r.Tag != f.Tag {
		return false
	}
	if f.Entity > 0 && r.Entity != f.Entity {
		return false
	}
	if f.TxID > 0 && r.TxID != f.TxID {
		return false
	}
	return true
}

var _ WalReader = (*Reader)(nil)

type Reader struct {
	flt            *RecordFilter
	bufferedReader *bufferedReader
	prevCsum       uint64
}

func (r *Reader) WithType(t RecordType) WalReader {
	if r.flt == nil {
		r.flt = &RecordFilter{}
	}
	r.flt.Type = t
	return r
}

func (r *Reader) WithTag(t types.ObjectTag) WalReader {
	if r.flt == nil {
		r.flt = &RecordFilter{}
	}
	r.flt.Tag = t
	return r
}

func (r *Reader) WithEntity(v uint64) WalReader {
	if r.flt == nil {
		r.flt = &RecordFilter{}
	}
	r.flt.Entity = v
	return r
}

func (r *Reader) WithTxID(v uint64) WalReader {
	if r.flt == nil {
		r.flt = &RecordFilter{}
	}
	r.flt.TxID = v
	return r
}

func (r *Reader) Close() error {
	err := r.bufferedReader.Close()
	r.flt = nil
	return err
}

func (r *Reader) Seek(lsn LSN) error {
	return r.bufferedReader.Seek(lsn)
}

func (r *Reader) Next() (*Record, error) {
	// read protocol
	// - read large chunks of data (to amortize i/o costs) into a buffer
	// - then iterate the buffer record by record
	// - if the remaining data in the buffer is < record header size
	//   or if the remaining data is < record body len, read more chunks
	//   until the next full record is assemled
	// - assembling a very large record may require to work across segement
	//   files
	// - after reading each record, check the chained checksum
	// - then decide whether we should skip based on filter match

	for {
		record := &Record{}
		head, err := r.bufferedReader.Read(30)
		if err != nil {
			return nil, err
		}
		record.Type = RecordType(head[0])
		record.Tag = types.ObjectTag(head[1])
		record.Entity = LE.Uint64(head[2:10])
		record.TxID = LE.Uint64(head[10:18])
		dataLength := LE.Uint32(head[18:22])

		data, err := r.bufferedReader.Read(int(dataLength))
		if err != nil {
			return nil, err
		}
		record.Data = data

		// check checksum
		r.bufferedReader.hash.Reset()
		var b [8]byte
		LE.PutUint64(b[:], r.prevCsum)
		r.bufferedReader.hash.Write(b[:])
		r.bufferedReader.hash.Write(head[:22])
		r.bufferedReader.hash.Write(record.Data)
		calculatedChecksum := r.bufferedReader.hash.Sum64()
		checksum := LE.Uint64(head[22:])

		if checksum != calculatedChecksum {
			return nil, ErrChecksum
		}

		if !r.flt.Match(record) {
			continue
		}

		r.prevCsum = checksum
		return record, nil
	}

	return nil, io.EOF
}

func (r *Reader) Checksum() uint64 {
	return r.prevCsum
}
