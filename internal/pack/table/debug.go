// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package table

import (
	"context"

	"blockwatch.cc/knoxdb/internal/pack"
	"blockwatch.cc/knoxdb/internal/pack/stats"
	"blockwatch.cc/knoxdb/pkg/bitmap"
)

func (t *Table) ViewStats(i int) *stats.Record {
	switch {
	case i == int(pack.JournalKeyId):
		return stats.NewRecordFromPack(t.journal.Data, 0)
	default:
		info, _ := t.stats.Get(uint32(i))
		return info
	}
}

func (t *Table) ViewPackage(ctx context.Context, i int) *pack.Package {
	if i == int(pack.JournalKeyId) {
		return t.journal.Data
	}
	if i < 0 || i >= t.stats.Len() {
		return nil
	}
	rec, ok := t.stats.Get(uint32(i))
	if !ok {
		return nil
	}
	pkg, err := t.loadSharedPack(ctx, rec.Key, int(rec.NValues), false, t.schema)
	if err != nil {
		return nil
	}
	if pkg.IsNil() {
		pkg.Release()
		return nil
	}
	return pkg
}

func (t *Table) ViewTomb() bitmap.Bitmap {
	return t.journal.Tomb
}
