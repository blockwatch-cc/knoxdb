// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package table

import (
	"context"

	"blockwatch.cc/knoxdb/internal/pack"
	"blockwatch.cc/knoxdb/internal/pack/stats"
	"blockwatch.cc/knoxdb/internal/xroar"
)

func (t *Table) ViewStats(i int) *stats.Record {
	switch {
	case i == -1:
		// TODO: howto address multi-segment journal?
		return stats.NewRecordFromPack(t.journal.Active().Data(), 0)
	default:
		info, _ := t.stats.Load().(*stats.Index).Get(uint32(i))
		return info
	}
}

func (t *Table) ViewPackage(ctx context.Context, i int) *pack.Package {
	if i == -1 {
		return t.journal.Active().Data()
	}
	si := t.stats.Load().(*stats.Index)
	if i < 0 || i >= si.Len() {
		return nil
	}
	rec, ok := si.Get(uint32(i))
	if !ok {
		return nil
	}
	pkg, err := t.NewReader().Read(ctx, rec.Key)
	if err != nil {
		return nil
	}
	if pkg.IsNil() {
		pkg.Release()
		return nil
	}
	return pkg
}

func (t *Table) ViewTomb() *xroar.Bitmap {
	// TODO: howto address multi-segment journal?
	return t.journal.Active().Tomb().RowIds()
}
