// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package index

import (
	"context"
	"fmt"

	"blockwatch.cc/knoxdb/internal/block"
	"blockwatch.cc/knoxdb/internal/engine"
	"blockwatch.cc/knoxdb/internal/pack"
	"blockwatch.cc/knoxdb/internal/pack/stats"
	"blockwatch.cc/knoxdb/internal/store"
	"blockwatch.cc/knoxdb/internal/xroar"
)

func (idx *Index) ViewStats(i int) *stats.Record {
	switch {
	case i == -1:
		return stats.NewRecordFromPack(idx.journal, 0)
	case i == -2:
		return stats.NewRecordFromPack(idx.tomb, 0)
	default:
		pkg, n, err := idx.loadPack(i)
		if err != nil {
			idx.log.Debugf("index[%s]: pack %d: %v", idx.idxSchema.Name(), i, err)
			return nil
		}
		pkg.WithStats()
		defer pkg.Release()
		return stats.NewRecordFromPack(pkg, n)
	}
}

func (idx *Index) ViewPackage(ctx context.Context, i int) *pack.Package {
	switch i {
	case -1:
		return idx.journal
	case -2:
		return idx.tomb
	default:
		pkg, _, err := idx.loadPack(i)
		if err != nil {
			idx.log.Debugf("index[%s]: pack %d: %v", idx.idxSchema.Name(), i, err)
			return nil
		}
		return pkg
	}
}

func (idx *Index) ViewTomb(_ int) *xroar.Bitmap {
	bits := xroar.New()
	for _, v := range idx.tomb.Block(0).Uint64().Slice() {
		bits.Set(v)
	}
	return bits
}

func (idx *Index) loadPack(i int) (*pack.Package, int, error) {
	if i < 0 {
		return nil, 0, engine.ErrInvalidId
	}
	var (
		pkg    *pack.Package
		nBytes int
	)
	err := idx.db.View(func(tx store.Tx) error {
		bkt := idx.dataBucket(tx)
		if bkt == nil {
			return store.ErrNoBucket
		}
		cur := bkt.Cursor()
		defer cur.Close()
		for n, ok := 0, cur.First(); ok && n < i*2; n, ok = n+1, cur.Next() {
		}
		if cur.Key() == nil {
			return engine.ErrNoKey
		}
		ik, pk, _ := idx.decodePackKey(cur.Key())
		f1, _ := idx.idxSchema.FieldByIndex(0)
		blk1, err := block.Decode(f1.Type().BlockType(), cur.Value())
		if err != nil {
			return fmt.Errorf("loading block 0x%08x:%08x:%d: %v", ik, pk, 0, err)
		}
		nBytes += len(cur.Value())
		if !cur.Next() {
			return fmt.Errorf("loading block 0x%08x:%08x:%d: %v", ik, pk, 1, engine.ErrDatabaseCorrupt)
		}
		f2, _ := idx.idxSchema.FieldByIndex(1)
		blk2, err := block.Decode(f2.Type().BlockType(), cur.Value())
		if err != nil {
			return fmt.Errorf("loading block 0x%08x:%08x:%d: %v", ik, pk, 1, err)
		}
		nBytes += len(cur.Value())
		pkg = pack.New().
			WithKey(uint32(i)).
			WithSchema(idx.idxSchema).
			WithMaxRows(blk1.Cap()).
			WithBlock(0, blk1).
			WithBlock(1, blk2)
		return nil
	})
	return pkg, nBytes, err
}
