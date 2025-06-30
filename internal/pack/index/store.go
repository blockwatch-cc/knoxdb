// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package index

import (
	"encoding/binary"

	"blockwatch.cc/knoxdb/internal/engine"
	"blockwatch.cc/knoxdb/internal/hash/xxhash64"
	"blockwatch.cc/knoxdb/internal/store"
	"blockwatch.cc/knoxdb/pkg/num"
)

var BE = binary.BigEndian

func (idx *Index) dataBucket(tx store.Tx) store.Bucket {
	key := append([]byte(idx.name), engine.DataKeySuffix...)
	b := tx.Bucket(key)
	if b != nil {
		b.FillPercent(idx.opts.PageFill)
	}
	return b
}

// Encode sortable keys for referencing data blocks on storage.
//
// Format:
// varint(index key) + varint(primary key) + varint(block id)
//
// We append primary keys for uniqueness in case the same
// non-unique index key spans multiple packs.
func (idx *Index) encodePackKey(ik, pk uint64, id int) []byte {
	var b [2*num.MaxVarintLen64 + num.MaxVarintLen16]byte
	buf := num.AppendUvarint(b[:0], ik)
	buf = num.AppendUvarint(buf, pk)
	buf = num.AppendUvarint(buf, uint64(id))
	return buf
}

func (idx *Index) decodePackKey(buf []byte) (ik, pk uint64, id int) {
	var n int
	ik, n = num.Uvarint(buf)
	// if n == 0 {
	// 	panic(fmt.Errorf("invalid key %s", hex.EncodeToString(buf)))
	// }
	buf = buf[n:]
	pk, n = num.Uvarint(buf)
	// if n == 0 {
	// 	panic(fmt.Errorf("invalid key %s", hex.EncodeToString(buf)))
	// }
	v, _ := num.Uvarint(buf[n:])
	// if n == 0 {
	// 	panic(fmt.Errorf("invalid key %s", hex.EncodeToString(buf)))
	// }
	id = int(v)
	return
}

func (idx *Index) encodeCacheKey(ik, pk uint64, id int) uint64 {
	var buf [8]byte
	h64 := xxhash64.New()
	BE.PutUint64(buf[:], ik)
	h64.Write(buf[:])
	BE.PutUint64(buf[:], pk)
	h64.Write(buf[:])
	buf[0] = byte(id)
	h64.Write(buf[:1])
	return h64.Sum64()
}
