// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package table

import (
	"context"
	"io"

	"blockwatch.cc/knoxdb/internal/pack"
	"blockwatch.cc/knoxdb/internal/pack/metadata"
	"blockwatch.cc/knoxdb/pkg/bitmap"
)

func (t *Table) ValidateMetadata(w io.Writer) error {
	if errs := t.meta.Validate(); errs != nil {
		for _, v := range errs {
			w.Write([]byte(v.Error() + "\n"))
		}
	}
	return nil
}

func (t *Table) ViewMetadata(i int) *metadata.PackMetadata {
	switch {
	case i == int(pack.JournalKeyId):
		info := &metadata.PackMetadata{
			Key:      t.journal.Data.Key(),
			SchemaId: t.journal.Data.Schema().Hash(),
			NValues:  t.journal.Data.Len(),
			Blocks:   make([]metadata.BlockMetadata, 0, t.schema.NumFields()),
			Dirty:    true,
		}
		fields := t.schema.Fields()
		for i, b := range t.journal.Data.Blocks() {
			info.Blocks = append(info.Blocks, metadata.NewBlockMetadata(b, &fields[i]))
		}
		return info
	case i >= 0 && i < t.meta.Len():
		info, _ := t.meta.GetSorted(i)
		return info
	default:
		return nil
	}
}

func (t *Table) ViewPackage(ctx context.Context, i int) *pack.Package {
	if i == int(pack.JournalKeyId) {
		return t.journal.Data
	}
	if i < 0 || i >= t.meta.Len() {
		return nil
	}
	info, _ := t.meta.GetSorted(i)
	pkg, err := t.loadSharedPack(ctx, info.Key, info.NValues, false, t.schema)
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
