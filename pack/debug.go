// Copyright (c) 2018-2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package pack

import (
	"bytes"
	"fmt"
	"io"
	"strings"

	"blockwatch.cc/knoxdb/encoding/block"
	"blockwatch.cc/knoxdb/encoding/csv"
	"blockwatch.cc/knoxdb/util"
)

type DumpMode int

const (
	DumpModeDec DumpMode = iota
	DumpModeHex
	DumpModeCSV
)

type CSVHeader struct {
	Key   string `csv:"Pack Key"`
	Cols  int    `csv:"Columns"`
	Rows  int    `csv:"Rows"`
	MinPk uint64 `csv:"Min RowId"`
	MaxPk uint64 `csv:"Max RowId"`
	Size  int    `csv:"Pack Size"`
}

func (t *Table) DumpType(w io.Writer) error {
	return t.journal.DumpType(w)
}

func (t *Table) DumpPackHeaders(w io.Writer, mode DumpMode) error {
	tx, err := t.db.Tx(false)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	switch mode {
	case DumpModeDec, DumpModeHex:
		fmt.Fprintf(w, "%-3s %-10s %-7s %-7s %-21s %-21s %-10s\n",
			"#", "Key", "Fields", "Values", "Min", "Max", "Size")
	}
	var i int
	for i = 0; i < t.packs.Len(); i++ {
		switch mode {
		case DumpModeDec, DumpModeHex:
			fmt.Fprintf(w, "%-3d ", i)
		}
		if err := t.packs.heads[i].Dump(w, mode); err != nil {
			return err
		}
	}
	switch mode {
	case DumpModeDec, DumpModeHex:
		fmt.Fprintf(w, "%-3d ", i)
		i++
	}
	if err := t.journal.Header().Dump(w, mode); err != nil {
		return err
	}
	switch mode {
	case DumpModeDec, DumpModeHex:
		fmt.Fprintf(w, "%-3d ", i)
	}
	if err := t.tombstone.Header().Dump(w, mode); err != nil {
		return err
	}
	return nil
}

func (t *Table) DumpJournal(w io.Writer, mode DumpMode) error {
	return t.journal.DumpData(w, mode, t.fields.Aliases())
}

func (t *Table) DumpPack(w io.Writer, i int, mode DumpMode) error {
	if i >= t.packs.Len() || i < 0 {
		return ErrPackNotFound
	}
	tx, err := t.db.Tx(false)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	pkg, err := t.loadPack(tx, t.packs.heads[i].Key, false, nil)
	if err != nil {
		return err
	}
	return pkg.DumpData(w, mode, t.fields.Aliases())
}

func (t *Table) DumpIndexPack(w io.Writer, i, p int, mode DumpMode) error {
	if i >= len(t.indexes) || i < 0 {
		return ErrIndexNotFound
	}
	if p >= t.indexes[i].packs.Len() || p < 0 {
		return ErrPackNotFound
	}
	tx, err := t.db.Tx(false)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	pkg, err := t.indexes[i].loadPack(tx, t.indexes[i].packs.heads[p].Key, false)
	if err != nil {
		return err
	}
	return pkg.DumpData(w, mode, []string{"Hash", "Pk"})
}

func (t *Table) DumpPackBlocks(w io.Writer, mode DumpMode) error {
	tx, err := t.db.Tx(false)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	switch mode {
	case DumpModeDec, DumpModeHex:
		fmt.Fprintf(w, "%-5s %-10s %-7s %-10s %-7s %-33s %-33s %-4s %-5s %-6s %-10s %-12s %-10s %-10s\n",
			"#", "Key", "Block", "Type", "Rows", "Min", "Max", "Prec", "Flags", "Comp", "Compressed", "Uncompressed", "Memory", "GoType")
	}
	lineNo := 1
	for i := 0; i < t.packs.Len(); i++ {
		pkg, err := t.loadPack(tx, t.packs.heads[i].Key, false, nil)
		if err != nil {
			return err
		}
		if n, err := pkg.DumpBlocks(w, mode, lineNo); err != nil {
			return err
		} else {
			lineNo = n
		}
	}
	return nil
}

func (t *Table) DumpIndexPackHeaders(w io.Writer, mode DumpMode) error {
	tx, err := t.db.Tx(false)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for _, v := range t.indexes {
		if err := v.dumpPackHeaders(tx, w, mode); err != nil {
			return err
		}
	}
	return nil
}

func (idx *Index) dumpPackHeaders(tx *Tx, w io.Writer, mode DumpMode) error {
	switch mode {
	case DumpModeDec, DumpModeHex:
		fmt.Fprintf(w, "%-3s %-10s %-7s %-7s %-21s %-21s %-10s\n",
			"#", "Key", "Fields", "Values", "Min", "Max", "Size")
	}
	var i int
	for i = 0; i < idx.packs.Len(); i++ {
		switch mode {
		case DumpModeDec, DumpModeHex:
			fmt.Fprintf(w, "%-3d ", i)
		}
		if err := idx.packs.heads[i].Dump(w, mode); err != nil {
			return err
		}
	}
	switch mode {
	case DumpModeDec, DumpModeHex:
		fmt.Fprintf(w, "%-3d ", i)
		i++
	}
	if err := idx.journal.Header().Dump(w, mode); err != nil {
		return err
	}
	switch mode {
	case DumpModeDec, DumpModeHex:
		fmt.Fprintf(w, "%-3d ", i)
		i++
	}
	return idx.tombstone.Header().Dump(w, mode)
}

func (h PackageHeader) Dump(w io.Writer, mode DumpMode) error {
	var key string
	switch true {
	case bytes.Compare(h.Key, journalKey) == 0:
		key = string(h.Key)
	case bytes.Compare(h.Key, tombstoneKey) == 0:
		key = string(h.Key)
	default:
		key = util.ToString(h.Key)
	}
	head := h.BlockHeaders[0]
	min, max := head.MinValue.(uint64), head.MaxValue.(uint64)
	switch mode {
	case DumpModeDec:
		_, err := fmt.Fprintf(w, "%-10s %-7d %-7d %-21d %-21d %-10s\n",
			key,
			h.NFields,
			h.NValues,
			min,
			max,
			util.ByteSize(h.PackSize))
		return err
	case DumpModeHex:
		_, err := fmt.Fprintf(w, "%-10s %-7d %-7d %21x %21x %-10s\n",
			key,
			h.NFields,
			h.NValues,
			min,
			max,
			util.ByteSize(h.PackSize))
		return err
	case DumpModeCSV:
		enc, ok := w.(*csv.Encoder)
		if !ok {
			enc = csv.NewEncoder(w)
		}
		ch := CSVHeader{
			Key:   key,
			Cols:  h.NFields,
			Rows:  h.NValues,
			MinPk: min,
			MaxPk: max,
			Size:  h.PackSize,
		}
		return enc.EncodeRecord(ch)
	}
	return nil
}

func (p *Package) DumpType(w io.Writer) error {
	typname := "undefined"
	if p.tinfo != nil {
		typname = p.tinfo.name
	}
	var key string
	switch true {
	case bytes.Compare(p.key, journalKey) == 0:
		key = string(p.key)
	case bytes.Compare(p.key, tombstoneKey) == 0:
		key = string(p.key)
	default:
		key = util.ToString(p.key)
	}
	fmt.Fprintf(w, "Package ------------------------------------ \n")
	fmt.Fprintf(w, "Key:        %s\n", key)
	fmt.Fprintf(w, "Version:    %d\n", p.version)
	fmt.Fprintf(w, "Fields:     %d\n", p.nFields)
	fmt.Fprintf(w, "Values:     %d\n", p.nValues)
	fmt.Fprintf(w, "Pk index:   %d\n", p.pkindex)
	fmt.Fprintf(w, "Type:       %s\n", typname)
	fmt.Fprintf(w, "Size:       %s (%d) zipped, %s (%d) unzipped\n",
		util.ByteSize(p.packedsize), p.packedsize,
		util.ByteSize(p.rawsize), p.rawsize,
	)
	for i, v := range p.blocks {
		d, fi := "", ""
		if v.Dirty {
			d = "*"
		}
		if p.tinfo != nil {
			fi = p.tinfo.fields[i].String()
		}
		var sz int
		if i+1 < len(p.blocks) {
			sz = p.offsets[i+1] - p.offsets[i]
		} else {
			sz = p.rawsize - p.offsets[i]
		}
		head := v.CloneHeader()
		fmt.Fprintf(w, "Block %-02d:   %s typ=%d comp=%s flags=%d prec=%d len=%d min=%s max=%s sz=%s %s %s\n",
			i,
			p.names[i],
			head.Type,
			head.Compression,
			head.Flags,
			head.Precision,
			v.Len(),
			util.ToString(head.MinValue),
			util.ToString(head.MaxValue),
			util.PrettyInt(sz),
			fi,
			d)
	}
	return nil
}

func (p *Package) DumpBlocks(w io.Writer, mode DumpMode, lineNo int) (int, error) {
	var key string
	switch true {
	case bytes.Compare(p.key, journalKey) == 0:
		key = string(p.key)
	case bytes.Compare(p.key, tombstoneKey) == 0:
		key = string(p.key)
	default:
		key = util.ToString(p.key)
	}
	switch mode {
	case DumpModeDec, DumpModeHex:
		for i, v := range p.blocks {
			fi := ""
			if p.tinfo != nil {
				fi = p.tinfo.fields[i].String()
			}
			var sz int
			if i+1 < len(p.blocks) {
				sz = p.offsets[i+1] - p.offsets[i]
			} else {
				sz = p.rawsize - p.offsets[i]
			}
			head := v.CloneHeader()
			_, err := fmt.Fprintf(w, "%-5d %-10s %-7d %-10s %-7d %-33s %-33s %-4d %-5d %-6s %-10s %-12s %-10s %-10s\n",
				lineNo,
				key,       // pack key
				i,         // block id
				head.Type, // block type
				v.Len(),   // block values
				util.LimitStringEllipsis(util.ToString(head.MinValue), 33), // min val in block
				util.LimitStringEllipsis(util.ToString(head.MaxValue), 33), // max val in block
				head.Precision,
				head.Flags,
				head.Compression,
				util.PrettyInt(sz),                // compressed block size
				util.PrettyInt(v.MaxStoredSize()), // uncompressed storage size
				util.PrettyInt(v.Size()),          // in-memory block size
				fi,                                // type info type
			)
			lineNo++
			if err != nil {
				return lineNo, err
			}
		}
	case DumpModeCSV:
		enc, ok := w.(*csv.Encoder)
		if !ok {
			enc = csv.NewEncoder(w)
		}
		cols := make([]interface{}, 14)
		for i, v := range p.blocks {
			fi := ""
			if p.tinfo != nil {
				fi = p.tinfo.fields[i].String()
			}
			var sz int
			if i+1 < len(p.blocks) {
				sz = p.offsets[i+1] - p.offsets[i]
			} else {
				sz = p.rawsize - p.offsets[i]
			}
			head := v.CloneHeader()
			cols[0] = key
			cols[1] = i
			cols[2] = p.names[i]
			cols[3] = head.Type.String()
			cols[4] = v.Len()
			cols[5] = head.MinValue
			cols[6] = head.MaxValue
			cols[7] = head.Precision
			cols[8] = head.Flags
			cols[9] = head.Compression.String()
			cols[10] = sz
			cols[11] = v.Size()
			cols[12] = v.MaxStoredSize()
			cols[13] = fi
			if !enc.HeaderWritten() {
				if err := enc.EncodeHeader([]string{
					"Pack",
					"Block",
					"Name",
					"Type",
					"Columns",
					"Min",
					"Max",
					"Precision",
					"Flags",
					"Compression",
					"Compressed",
					"Uncompressed",
					"Memory",
					"GoType",
				}, nil); err != nil {
					return lineNo, err
				}
			}
			if err := enc.EncodeRecord(cols); err != nil {
				return lineNo, err
			}
			lineNo++
		}
	}
	return lineNo, nil
}
func (p *Package) DumpData(w io.Writer, mode DumpMode, aliases []string) error {
	names := p.names
	if len(aliases) == p.nFields && len(aliases[0]) > 0 {
		names = aliases
	}

	// estimate sizees from the first 500 values
	switch mode {
	case DumpModeDec, DumpModeHex:
		sz := make([]int, p.nFields)
		row := make([]string, p.nFields)
		for j := 0; j < p.nFields; j++ {
			sz[j] = len(names[j])
		}
		for i, l := 0, util.Min(500, p.nValues); i < l; i++ {
			for j := 0; j < p.nFields; j++ {
				var str string
				if p.blocks[j].Type == block.BlockIgnore {
					str = "[strip]"
				} else {
					val, _ := p.FieldAt(j, i)
					str = util.ToString(val)
				}
				sz[j] = util.Max(sz[j], len(str))
			}
		}
		for j := 0; j < p.nFields; j++ {
			row[j] = fmt.Sprintf("%[2]*[1]s", names[j], -sz[j])
		}
		var out string
		out = "| " + strings.Join(row, " | ") + " |\n"
		if _, err := w.Write([]byte(out)); err != nil {
			return err
		}
		for j := 0; j < p.nFields; j++ {
			row[j] = strings.Repeat("-", sz[j])
		}
		out = "|-" + strings.Join(row, "-|-") + "-|\n"
		if _, err := w.Write([]byte(out)); err != nil {
			return err
		}
		for i := 0; i < p.nValues; i++ {
			for j := 0; j < p.nFields; j++ {
				var str string
				if p.blocks[j].Type == block.BlockIgnore {
					str = "[strip]"
				} else {
					val, _ := p.FieldAt(j, i)
					str = util.ToString(val)
				}
				row[j] = fmt.Sprintf("%[2]*[1]s", str, -sz[j])
			}
			out = "| " + strings.Join(row, " | ") + " |\n"
			if _, err := w.Write([]byte(out)); err != nil {
				return err
			}
		}

	case DumpModeCSV:
		enc, ok := w.(*csv.Encoder)
		if !ok {
			enc = csv.NewEncoder(w)
		}
		if !enc.HeaderWritten() {
			if err := enc.EncodeHeader(names, nil); err != nil {
				return err
			}
		}
		// csv encoder supports []interface{} records
		for i := 0; i < p.nValues; i++ {
			row, _ := p.RowAt(i)
			if err := enc.EncodeRecord(row); err != nil {
				return err
			}
		}
	}
	return nil
}
