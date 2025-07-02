// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package table

import (
	"context"

	"blockwatch.cc/knoxdb/internal/engine"
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
	ctx = engine.WithEngine(ctx, t.engine)
	r := t.NewReader()
	defer r.Close()
	pkg, err := r.Read(ctx, uint32(i))
	if err != nil {
		return nil
	}
	return pkg.Copy() // copy because we close reader on return
}

func (t *Table) ViewTomb(i int) *xroar.Bitmap {
	if i < 0 {
		i = -i - 1
	}
	if i == 0 {
		return t.journal.Tip().Tomb().RowIds()
	} else if i-1 < t.journal.NumSegments()-1 {
		return t.journal.Segments()[i-1].Tomb().RowIds()
	}
	return nil
}
