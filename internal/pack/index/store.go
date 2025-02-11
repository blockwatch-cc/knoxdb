// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package index

import (
	"encoding/binary"
	"hash/fnv"

	"blockwatch.cc/knoxdb/internal/engine"
	"blockwatch.cc/knoxdb/internal/store"
)

var BE = binary.BigEndian

func (idx *Index) dataBucket(tx store.Tx) store.Bucket {
	key := append([]byte(idx.schema.Name()), engine.DataKeySuffix...)
	b := tx.Bucket(key)
	if b != nil {
		b.FillPercent(idx.opts.PageFill)
	}
	return b
}

// Encode sortable keys for referencing data blocks on storage.
//
// Format:
// index key + primary key + block id
//
// We append primary keys for uniqueness in case the same
// non-unique index key spans multiple packs.
func (idx *Index) encodePackKey(ik, pk uint64, id int) []byte {
	var buf [17]byte
	BE.PutUint64(buf[:], ik)
	BE.PutUint64(buf[8:], pk)
	buf[16] = byte(id)
	return buf[:]
}

func (idx *Index) decodePackKey(buf []byte) (ik, pk uint64, id int, err error) {
	if len(buf) != 17 {
		err = engine.ErrDatabaseCorrupt
		return
	}
	ik = BE.Uint64(buf)
	pk = BE.Uint64(buf[8:])
	id = int(buf[16])
	return
}

func (idx *Index) encodeCacheKey(ik, pk uint64, id int) engine.CacheKeyType {
	var buf [8]byte
	h64 := fnv.New64a()
	BE.PutUint64(buf[:], ik)
	h64.Write(buf[:])
	BE.PutUint64(buf[:], pk)
	h64.Write(buf[:])
	buf[0] = byte(id)
	h64.Write(buf[:1])
	return engine.CacheKeyType{idx.id, h64.Sum64()}
}
