// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package pack

import (
	"context"
	"fmt"
	"io"

	"blockwatch.cc/knoxdb/internal/types"
)

func (idx *Index) ValidateMetadata(w io.Writer) error {
	if errs := idx.meta.Validate(); errs != nil {
		for _, v := range errs {
			w.Write([]byte(v.Error() + "\n"))
		}
	}
	return nil
}

func (idx *Index) DumpMetadata(w io.Writer, mode types.DumpMode, sorted bool) error {
	switch mode {
	case types.DumpModeDec, types.DumpModeHex:
		fmt.Fprintf(w, "%-3s %-10s %-7s %-7s %-21s %-21s %-10s\n",
			"#", "Key", "Fields", "Values", "Min", "Max", "Size")
	}
	var (
		i   int
		err error
	)
	for i = 0; i < idx.meta.Len(); i++ {
		switch mode {
		case types.DumpModeDec, types.DumpModeHex:
			fmt.Fprintf(w, "%-3d ", i)
		}
		if sorted {
			info, _ := idx.meta.GetSorted(i)
			err = info.Dump(w, mode, 2)
		} else {
			info, _ := idx.meta.GetPos(i)
			err = info.Dump(w, mode, 2)
		}
		if err != nil {
			return err
		}
	}
	switch mode {
	case types.DumpModeDec, types.DumpModeHex:
		fmt.Fprintf(w, "%-3d ", i)
		i++
	}
	return nil
}

func (idx *Index) DumpPack(ctx context.Context, w io.Writer, i, p int, mode types.DumpMode) error {
	if i >= idx.meta.Len() || i < 0 {
		return fmt.Errorf("pack not found")
	}
	tx, err := idx.db.Begin(false)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	info, _ := idx.meta.GetPos(i)
	pkg, err := idx.loadSharedPack(ctx, info.Key, info.NValues, false)
	if err != nil {
		return err
	}
	err = pkg.DumpData(w, mode)
	if err != nil {
		return err
	}
	pkg.Release()
	return nil
}
