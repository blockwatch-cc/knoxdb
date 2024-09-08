// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package pack

import (
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"strconv"

	"blockwatch.cc/knoxdb/internal/metadata"
	"blockwatch.cc/knoxdb/internal/pack"
	"blockwatch.cc/knoxdb/internal/types"
)

type DumpMode = types.DumpMode

const (
	DumpModeDec = types.DumpModeDec
	DumpModeHex = types.DumpModeHex
	DumpModeCSV = types.DumpModeCSV
)

func (t *Table) ValidateMetadata(w io.Writer) error {
	if errs := t.meta.Validate(); errs != nil {
		for _, v := range errs {
			w.Write([]byte(v.Error() + "\n"))
		}
	}
	return nil
}

type CSVHeader struct {
	Key   string `csv:"Pack Key"`
	Cols  int    `csv:"Columns"`
	Rows  int    `csv:"Rows"`
	MinPk uint64 `csv:"Min RowId"`
	MaxPk uint64 `csv:"Max RowId"`
	Size  int    `csv:"Pack Size"`
}

func (t *Table) DumpType(w io.Writer) error {
	return t.journal.Data.DumpType(w)
}

func (t *Table) DumpMetadata(w io.Writer, mode DumpMode) error {
	tx, err := t.db.Begin(false)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	switch mode {
	case DumpModeDec, DumpModeHex:
		fmt.Fprintf(w, "Package Metadata --------------------------------------------------------------- \n")
		fmt.Fprintf(w, "%-3s %-10s %-7s %-7s %-21s %-21s %-10s\n",
			"#", "Key", "Fields", "Values", "Min", "Max", "Size")
	}
	var i int
	for i = 0; i < t.meta.Len(); i++ {
		switch mode {
		case DumpModeDec, DumpModeHex:
			fmt.Fprintf(w, "%-3d ", i)
		}
		info, ok := t.meta.GetSorted(i)
		if !ok {
			return fmt.Errorf("Missing metadata for pack %d/%d", i, t.meta.Len())
		}
		info.Dump(w, mode, t.schema.NumFields())
	}
	// switch mode {
	// case DumpModeDec, DumpModeHex:
	// 	fmt.Fprintf(w, "%-3d ", i)
	// 	i++
	// }

	// journal metadata
	// create new metadata
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
	fmt.Fprintf(w, "%-3s ", "J")
	if err := info.Dump(w, mode, t.schema.NumFields()); err != nil {
		return err
	}
	return nil
}

func (t *Table) DumpJournal(w io.Writer, mode DumpMode) error {
	err := t.journal.Data.DumpData(w, pack.DumpMode(mode))
	if err != nil {
		return err
	}
	w.Write([]byte("keys:"))
	for _, v := range t.journal.Keys {
		w.Write([]byte(strconv.FormatUint(v.Pk, 10)))
		w.Write([]byte(">"))
		w.Write([]byte(strconv.Itoa(v.Idx)))
		w.Write([]byte(","))
	}
	w.Write([]byte("\n"))
	w.Write([]byte("tomb:"))
	for _, v := range t.journal.Tomb.Bitmap.ToArray() {
		w.Write([]byte(strconv.FormatUint(v, 10)))
		w.Write([]byte(","))
	}
	w.Write([]byte("\n"))
	w.Write([]byte("dbits:"))
	w.Write([]byte(hex.EncodeToString(t.journal.Deleted.Bytes())))
	w.Write([]byte("\n"))
	return nil
}

func (t *Table) DumpMetadataDetail(w io.Writer, mode DumpMode) error {
	switch mode {
	case DumpModeDec, DumpModeHex:
	default:
		// unsupported
		return nil
	}
	tx, err := t.db.Begin(false)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var i int
	for i = 0; i < t.meta.Len(); i++ {
		var info *metadata.PackMetadata
		info, _ = t.meta.GetSorted(i)
		info.DumpDetail(w)
	}
	return nil
}

func (t *Table) DumpPack(ctx context.Context, w io.Writer, i int, mode DumpMode) error {
	if i >= t.meta.Len() || i < 0 {
		return fmt.Errorf("pack not found")
	}
	tx, err := t.db.Begin(false)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	info, _ := t.meta.GetSorted(i)
	pkg, err := t.loadSharedPack(ctx, info.Key, info.NValues, false, t.schema)
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

func (t *Table) WalkPacks(ctx context.Context, fn func(*pack.Package) error) error {
	tx, err := t.db.Begin(false)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for i := 0; i < t.meta.Len(); i++ {
		info, _ := t.meta.GetSorted(i)
		pkg, err := t.loadSharedPack(ctx, info.Key, info.NValues, false, t.schema)
		if err != nil {
			return err
		}
		if err := fn(pkg); err != nil {
			return err
		}
		pkg.Release()
	}
	return nil
}

func (t *Table) WalkPacksRange(ctx context.Context, start, end int, fn func(*pack.Package) error) error {
	tx, err := t.db.Begin(false)
	if err != nil {
		return err
	}
	if start < 0 {
		start = 0
	}
	if end < 0 {
		end = t.meta.Len() - 1
	}
	defer tx.Rollback()
	for i := start; i <= end && i < t.meta.Len(); i++ {
		info, _ := t.meta.GetSorted(i)
		pkg, err := t.loadSharedPack(ctx, info.Key, info.NValues, false, t.schema)
		if err != nil {
			return err
		}
		if err := fn(pkg); err != nil {
			return err
		}
		pkg.Release()
	}
	return nil
}

// TODO
// func (t *Table) DumpBlocks(ctx context.Context, w io.Writer, mode DumpMode) error {
// 	tx, err := t.db.Begin(false)
// 	if err != nil {
// 		return err
// 	}
// 	defer tx.Rollback()
// 	switch mode {
// 	case DumpModeDec, DumpModeHex:
// 		fmt.Fprintf(w, "%-5s %-10s %-7s %-10s %-7s %-5s %-33s %-33s %-4s %-6s %-10s %-10s %7s %-10s\n",
// 			"#", "Key", "Block", "Type", "Rows", "Card", "Min", "Max", "Prec", "Comp", "Stored", "Heap", "Ratio", "GoType")
// 	}
// 	lineNo := 1
// 	for i := 0; i < t.meta.Len(); i++ {
// 		info, _ := t.meta.Get(i)
// 		pkg, err := t.loadSharedPack(ctx, info.Key, info.NValues, false, t.schema)
// 		if err != nil {
// 			return err
// 		}
// 		if n, err := pkg.DumpBlocks(w, mode, lineNo); err != nil {
// 			return err
// 		} else {
// 			lineNo = n
// 		}
// 		pkg.Release()
// 	}
// 	return nil
// }
