// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package metadata

import (
	"context"
	"fmt"
	"sort"

	"blockwatch.cc/knoxdb/internal/store"
)

func (idx *MetadataIndex) Load(ctx context.Context, tx store.Tx, bucket []byte) (int, error) {
	packs, n, err := loadMetadata(ctx, tx, bucket)
	if err != nil {
		return 0, err
	}
	idx.packs = packs
	idx.minpks = make([]uint64, len(packs), cap(packs))
	idx.maxpks = make([]uint64, len(packs), cap(packs))
	idx.pos = make([]int32, len(packs), cap(packs))
	sort.Sort(idx.packs)
	for i := range idx.packs {
		idx.minpks[i] = idx.packs[i].Blocks[idx.pki].MinValue.(uint64)
		idx.maxpks[i] = idx.packs[i].Blocks[idx.pki].MaxValue.(uint64)
		idx.pos[i] = int32(i)
	}
	idx.Sort()
	return n, nil
}

func loadMetadata(ctx context.Context, tx store.Tx, bucket []byte) (PackMetadataList, int, error) {
	meta := tx.Bucket(bucket)
	if meta == nil {
		return nil, 0, fmt.Errorf("missing metadata bucket %q", string(bucket))
	}
	packs := make(PackMetadataList, 0)
	c := meta.Cursor()
	defer c.Close()
	var (
		n   int
		err error
	)
	for ok := c.First(); ok; ok = c.Next() {
		buf := c.Value()
		n += len(buf)
		info := &PackMetadata{}
		err = info.UnmarshalBinary(buf)
		if err != nil {
			break
		}
		packs = append(packs, info)
	}
	if err != nil {
		return nil, n, fmt.Errorf("meta decode pack 0x%08x: %v", c.Key(), err)
	}
	return packs, n, nil
}

// func BuildMetadata(ctx context.Context, tx store.Tx, bucket []byte) error {
// 	log.Warnf("pack: %s table has corrupt or missing statistics! Re-scanning table. This may take some time...", t.name)
// 	c := tx.Bucket(t.key).Cursor()
// 	pkg := NewPackage(maxPackSize, nil)
// 	if err := pkg.InitFieldsFrom(t.journal.DataPack()); err != nil {
// 		return err
// 	}
// 	for ok := c.First(); ok; ok = c.Next() {
// 		err := pkg.UnmarshalBinary(c.Value())
// 		if err != nil {
// 			return fmt.Errorf("pack: cannot read %s/%x: %v", t.name, c.Key(), err)
// 		}
// 		pkg.SetKey(c.Key())
// 		if pkg.IsJournal() || pkg.IsTomb() {
// 			pkg.Clear()
// 			continue
// 		}
// 		info := pkg.Info()
// 		_ = info.UpdateStats(pkg)
// 		packs = append(packs, info)
// 		atomic.AddInt64(&t.stats.MetaBytesRead, int64(len(c.Value())))
// 		pkg.Clear()
// 	}
// 	t.packidx = NewPackHeader(packs, t.fields.PkIndex(), maxPackSize)
// 	atomic.StoreInt64(&t.stats.PacksCount, int64(t.packidx.Len()))
// 	atomic.StoreInt64(&t.stats.MetaSize, int64(t.packidx.HeapSize()))
// 	atomic.StoreInt64(&t.stats.TotalSize, int64(t.packidx.TableSize()))
// 	log.Debugf("pack: %s table scanned %d packages", t.name, t.packidx.Len())
// 	return nil
// }

func (idx *MetadataIndex) Store(ctx context.Context, tx store.Tx, bucket []byte, fill float64) (int, error) {
	meta := tx.Bucket(bucket)
	if meta == nil {
		return 0, fmt.Errorf("missing metadata bucket %q", string(bucket))
	}
	meta.FillPercent(fill)

	// remove metadata for deleted packs, if any
	for _, v := range idx.removed.Values {
		var k [4]byte
		BE.PutUint32(k[:], v)
		meta.Delete(k[:])
	}
	idx.removed.Values = idx.removed.Values[:0]

	// store metadata for new/updated packs
	var n int
	for i := range idx.packs {
		if !idx.packs[i].Dirty {
			continue
		}
		buf, err := idx.packs[i].MarshalBinary()
		if err != nil {
			return n, err
		}
		if err := meta.Put(idx.packs[i].KeyBytes(), buf); err != nil {
			return n, err
		}
		n += len(buf)
		idx.packs[i].Dirty = false
	}
	return n, nil
}
