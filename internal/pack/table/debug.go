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
		return stats.NewRecordFromPack(t.journal.Tip().Data(), 0)
	case i < -1:
		i = -i - 2
		segs := t.journal.Segments()
		if i < len(segs) {
			return stats.NewRecordFromPack(segs[i].Data(), 0)
		}
		return nil
	default:
		s := t.stats.Retain()
		info, _ := s.Get(uint32(i))
		s.Release(false)
		return info
	}
}

func (t *Table) ViewPackage(ctx context.Context, i int) *pack.Package {
	if i < 0 {
		if i == -1 {
			return t.journal.Tip().Data()
		} else {
			i = -i - 2
			segs := t.journal.Segments()
			if i < len(segs) {
				return segs[i].Data()
			}
			return nil
		}
	}
	si := t.stats.Retain()
	defer si.Release(false)
	if i < 0 || i >= si.Len() {
		return nil
	}
	rec, ok := si.Get(uint32(i))
	if !ok {
		return nil
	}
	pkg, err := t.NewReader().Read(ctx, rec.Key, rec.Version)
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
	return t.journal.Tip().Tomb().RowIds()
}
