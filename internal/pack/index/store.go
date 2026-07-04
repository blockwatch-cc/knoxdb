// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package index

import (
	"context"
	"encoding/binary"

	"blockwatch.cc/knoxdb/internal/hash"
	"blockwatch.cc/knoxdb/internal/pack"
	"blockwatch.cc/knoxdb/pkg/sortx"
	"blockwatch.cc/knoxdb/pkg/store"
)

var BE = binary.BigEndian

func (idx *Index) dataBucket(tx store.Tx) store.Bucket {
	b, _ := tx.Bucket(idx.keys[DATA_KEY_IDX])
	return b
}

func (idx *Index) tombBucket(tx store.Tx) store.Bucket {
	b, _ := tx.Bucket(idx.keys[TOMB_KEY_IDX])
	return b
}

const PackKeySize = 2*store.MaxVarintLen64 + store.MaxVarintLen16

// Encode sortable keys for referencing data blocks on storage.
//
// Format:
// varint(index key) + varint(primary key) + varint(block id)
//
// We append primary keys for uniqueness in case the same
// non-unique index key spans multiple packs.
func (idx *Index) appendPackKey(buf []byte, ik, pk uint64, id int) []byte {
	buf = store.AppendUvarint(buf, ik)
	buf = store.AppendUvarint(buf, pk)
	buf = store.AppendUvarint(buf, uint64(id))
	return buf
}

func (idx *Index) decodePackKey(buf []byte) (ik, pk uint64, id int) {
	var n int
	ik, n = store.Uvarint(buf)
	// if n == 0 {
	// 	panic(fmt.Errorf("invalid key %s", hex.EncodeToString(buf)))
	// }
	buf = buf[n:]
	pk, n = store.Uvarint(buf)
	// if n == 0 {
	// 	panic(fmt.Errorf("invalid key %s", hex.EncodeToString(buf)))
	// }
	v, _ := store.Uvarint(buf[n:])
	// if n == 0 {
	// 	panic(fmt.Errorf("invalid key %s", hex.EncodeToString(buf)))
	// }
	id = int(v)
	return
}

func (idx *Index) encodeCacheKey(ik, pk uint64, id int) uint64 {
	var buf [8]byte
	h64 := hash.New()
	BE.PutUint64(buf[:], ik)
	h64.Write(buf[:])
	BE.PutUint64(buf[:], pk)
	h64.Write(buf[:])
	buf[0] = byte(id)
	h64.Write(buf[:1])
	return h64.Sum64()
}

func (idx *Index) storeTomb(ctx context.Context, epoch uint32) error {
	idx.log.Debugf("store tomb %d[v%d] with len=%d",
		idx.tomb.Key(), epoch, idx.tomb.Len())

	// set tomb version
	idx.tomb.WithVersion(epoch)

	// co-sort tomb columns in-place
	t0 := idx.tomb.Block(0).Uint64().Slice() // keys
	t1 := idx.tomb.Block(1).Uint64().Slice() // rowids
	sortx.SortPair(t0, t1)

	// write in storage tx
	err := idx.db.Update(func(tx store.Tx) error {
		_, err := idx.tomb.StoreToDisk(ctx, idx.tombBucket(tx))
		return err
	})
	if err != nil {
		return err
	}

	// clear tomb and rotate key
	idx.tomb.Clear()
	idx.tomb.WithKey(idx.tomb.Key() + 1)

	return nil
}

func (idx *Index) loadTomb(ctx context.Context, key, epoch uint32) (*pack.Package, error) {
	pkg := pack.New().
		WithMaxRows(idx.opts.JournalSize).
		WithSchema(idx.sstore).
		WithKey(key).
		WithVersion(epoch)
	err := idx.db.View(func(tx store.Tx) error {
		_, err := pkg.LoadFromDisk(ctx, idx.tombBucket(tx), nil, 0)
		return err
	})
	if err != nil {
		pkg.Release()
		return nil, err
	}

	// empty tomb (all blocks are nil) is expected when no more tomb vectors
	// are available for this epoch
	if pkg.IsNil() {
		pkg.Release()
		return nil, nil
	}

	idx.log.Debugf("load tomb %d[v%d]", key, epoch)

	return pkg, nil
}

func (idx *Index) dropTomb(_ context.Context, key, epoch uint32) error {
	idx.log.Debugf("drop tomb %d[v%d]", key, epoch)
	return idx.db.Update(func(tx store.Tx) error {
		b := idx.tombBucket(tx)
		var bkey [pack.BlockKeySize]byte
		for _, f := range idx.sstore.Fields {
			if err := b.Delete(pack.AppendBlockKey(bkey[:0], key, epoch, f.Id)); err != nil {
				return err
			}
		}
		return nil
	})
}
