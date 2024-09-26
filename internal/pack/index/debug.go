// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package index

import (
	"context"
	"io"

	"blockwatch.cc/knoxdb/internal/pack"
	"blockwatch.cc/knoxdb/internal/pack/metadata"
	"blockwatch.cc/knoxdb/pkg/bitmap"
)

func (idx *Index) ValidateMetadata(w io.Writer) error {
	if errs := idx.meta.Validate(); errs != nil {
		for _, v := range errs {
			w.Write([]byte(v.Error() + "\n"))
		}
	}
	return nil
}

func (idx *Index) ViewMetadata(i int) *metadata.PackMetadata {
	switch {
	case i == int(pack.JournalKeyId):
		info := &metadata.PackMetadata{
			Key:      idx.journal.Key(),
			SchemaId: idx.journal.Schema().Hash(),
			NValues:  idx.journal.Len(),
			Blocks:   make([]metadata.BlockMetadata, 0, idx.schema.NumFields()),
			Dirty:    true,
		}
		fields := idx.schema.Fields()
		for i, b := range idx.journal.Blocks() {
			info.Blocks = append(info.Blocks, metadata.NewBlockMetadata(b, &fields[i]))
		}
		return info
	case i > 0 && i < idx.meta.Len():
		info, _ := idx.meta.GetSorted(i)
		return info
	default:
		return nil
	}
}

func (idx *Index) ViewPackage(ctx context.Context, i int) *pack.Package {
	if i == int(pack.JournalKeyId) {
		return idx.journal
	}
	if i == int(pack.TombstoneKeyId) {
		return idx.tomb
	}
	if i < 0 || i >= idx.meta.Len() {
		return nil
	}
	info, _ := idx.meta.GetSorted(i)
	pkg, err := idx.loadSharedPack(ctx, info.Key, info.NValues, false)
	if err != nil {
		return nil
	}
	if pkg.IsNil() {
		pkg.Release()
		return nil
	}
	return pkg
}

func (idx *Index) ViewTomb() bitmap.Bitmap {
	return bitmap.New()
}
