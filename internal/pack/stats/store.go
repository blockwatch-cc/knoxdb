// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package stats

import (
	"context"
	"fmt"
	"sort"

	"blockwatch.cc/knoxdb/internal/engine"
	"blockwatch.cc/knoxdb/internal/store"
)

func (s *StatsIndex) Load(ctx context.Context, bucket store.Bucket) (int, error) {
	packs, n, err := LoadStats(ctx, bucket)
	if err != nil {
		return 0, err
	}
	s.packs = packs
	s.minpks = make([]uint64, len(packs), cap(packs))
	s.maxpks = make([]uint64, len(packs), cap(packs))
	s.pos = make([]int32, len(packs), cap(packs))
	sort.Sort(s.packs)
	for i := range s.packs {
		s.minpks[i] = s.packs[i].Blocks[s.pki].MinValue.(uint64)
		s.maxpks[i] = s.packs[i].Blocks[s.pki].MaxValue.(uint64)
		s.pos[i] = int32(i)
	}
	s.Sort()
	return n, nil
}

func LoadStats(ctx context.Context, bucket store.Bucket) (PackStatsList, int, error) {
	if bucket == nil {
		return nil, 0, engine.ErrNoBucket
	}
	packs := make(PackStatsList, 0)
	c := bucket.Cursor()
	defer c.Close()
	var (
		n   int
		err error
	)
	for ok := c.First(); ok; ok = c.Next() {
		buf := c.Value()
		n += len(buf)
		stats := &PackStats{}
		err = stats.UnmarshalBinary(buf)
		if err != nil {
			break
		}
		packs = append(packs, stats)
	}
	if err != nil {
		return nil, n, fmt.Errorf("pack 0x%08x statistics decode: %v", c.Key(), err)
	}
	return packs, n, nil
}

// func BuildStats(ctx context.Context, tx store.Tx, bucket []byte) error {
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
// 	t.packs = NewPackStats(packs, t.fields.PkIndex(), maxPackSize)
// 	atomic.StoreInt64(&t.stats.PacksCount, int64(t.packs.Len()))
// 	atomic.StoreInt64(&t.stats.MetaSize, int64(t.packs.HeapSize()))
// 	atomic.StoreInt64(&t.stats.TotalSize, int64(t.packs.TableSize()))
// 	log.Debugf("pack: %s table scanned %d packages", t.name, t.packs.Len())
// 	return nil
// }

func (s *StatsIndex) Store(ctx context.Context, bucket store.Bucket) (int, error) {
	if bucket == nil {
		return 0, engine.ErrNoBucket
	}

	// remove statistics for deleted packs, if any
	for _, v := range s.removed.Values {
		var k [4]byte
		BE.PutUint32(k[:], v)
		bucket.Delete(k[:])
	}
	s.removed.Values = s.removed.Values[:0]

	// store statistics for new/updated packs
	var n int
	for i := range s.packs {
		if !s.packs[i].Dirty {
			continue
		}
		buf, err := s.packs[i].MarshalBinary()
		if err != nil {
			return n, err
		}
		if err := bucket.Put(s.packs[i].KeyBytes(), buf); err != nil {
			return n, err
		}
		n += len(buf)
		s.packs[i].Dirty = false
	}
	return n, nil
}
