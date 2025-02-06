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
	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/bitmap"
)

func (idx *Index) ViewStats(i int) *stats.Record {
	switch {
	case i == int(pack.JournalKeyId):
		return stats.NewRecordFromPack(idx.schema, idx.journal)
	case i == int(pack.TombstoneKeyId):
		return stats.NewRecordFromPack(idx.schema, idx.tomb)
	default:
		pkg, err := idx.loadPack(i)
		if err != nil {
			idx.log.Error(err)
			return nil
		}
		defer pkg.Release()
		return stats.NewRecordFromPack(idx.schema, pkg)
	}
}

func (idx *Index) ViewPackage(ctx context.Context, i int) *pack.Package {
	switch i {
	case int(pack.JournalKeyId):
		return idx.journal
	case int(pack.TombstoneKeyId):
		return idx.tomb
	default:
		pkg, err := idx.loadPack(i)
		if err != nil {
			idx.log.Error(err)
			return nil
		}
		return pkg
	}
}

func (idx *Index) ViewTomb() bitmap.Bitmap {
	bits := bitmap.New()
	for _, v := range idx.tomb.Block(0).Uint64().Slice() {
		bits.Set(v)
	}
	return bits
}

func (idx *Index) loadPack(i int) (*pack.Package, error) {
	if i < 0 {
		return nil, engine.ErrInvalidId
	}
	var pkg *pack.Package
	err := idx.db.View(func(tx store.Tx) error {
		bkt := idx.dataBucket(tx)
		if bkt == nil {
			return engine.ErrNoBucket
		}
		cur := bkt.Cursor()
		defer cur.Close()
		for n, ok := 0, cur.First(); ok && n < i*2; n, ok = n+1, cur.Next() {
		}
		if cur.Key() == nil {
			return engine.ErrNoKey
		}
		ik, pk, _, err := idx.decodePackKey(cur.Key())
		if err != nil {
			return err
		}
		sz := idx.opts.PackSize
		blk1 := block.New(types.BlockTypes[idx.schema.Exported()[0].Type], sz)
		if err := blk1.Decode(cur.Value()); err != nil {
			return fmt.Errorf("loading block 0x%08x:%08x:%d: %v", ik, pk, 0, err)
		}
		if !cur.Next() {
			return fmt.Errorf("loading block 0x%08x:%08x:%d: %v", ik, pk, 1, engine.ErrDatabaseCorrupt)
		}
		blk2 := block.New(types.BlockTypes[idx.schema.Exported()[1].Type], sz)
		if err := blk2.Decode(cur.Value()); err != nil {
			return fmt.Errorf("loading block 0x%08x:%08x:%d: %v", ik, pk, 1, err)
		}
		pkg = pack.New().
			WithKey(uint32(i)).
			WithSchema(idx.schema).
			WithMaxRows(sz).
			WithBlock(0, blk1).
			WithBlock(1, blk2)
		return nil
	})
	return pkg, err
}
