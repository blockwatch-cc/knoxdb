// Copyright (c) 2018-2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package pack

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"io"
	"strconv"
	"strings"

	logpkg "github.com/echa/log"

	"blockwatch.cc/knoxdb/encoding/block"
	"blockwatch.cc/knoxdb/encoding/csv"
	"blockwatch.cc/knoxdb/util"
)

var levelDebug = logpkg.LevelDebug

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
	return t.journal.DataPack().DumpType(w)
}

func (t *Table) DumpPackHeaders(w io.Writer, mode DumpMode, sorted bool) error {
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
	for i = 0; i < t.packidx.Len(); i++ {
		switch mode {
		case DumpModeDec, DumpModeHex:
			fmt.Fprintf(w, "%-3d ", i)
		}
		if sorted {
			err = t.packidx.GetSorted(i).Dump(w, mode, len(t.fields))
		} else {
			err = t.packidx.Get(i).Dump(w, mode, len(t.fields))
		}
		if err != nil {
			return err
		}
	}
	switch mode {
	case DumpModeDec, DumpModeHex:
		fmt.Fprintf(w, "%-3d ", i)
		i++
	}
	info := t.journal.DataPack().Info()
	info.UpdateStats(t.journal.DataPack())
	if err := info.Dump(w, mode, t.journal.DataPack().Cols()); err != nil {
		return err
	}
	return nil
}

func (t *Table) DumpJournal(w io.Writer, mode DumpMode) error {
	err := t.journal.DataPack().DumpData(w, mode, t.fields.Aliases())
	if err != nil {
		return err
	}
	w.Write([]byte("keys:"))
	for _, v := range t.journal.keys {
		w.Write([]byte(strconv.FormatUint(v.pk, 10)))
		w.Write([]byte(">"))
		w.Write([]byte(strconv.Itoa(v.idx)))
		w.Write([]byte(","))
	}
	w.Write([]byte("\n"))
	w.Write([]byte("tomb:"))
	for _, v := range t.journal.tomb {
		w.Write([]byte(strconv.FormatUint(v, 10)))
		w.Write([]byte(","))
	}
	w.Write([]byte("\n"))
	w.Write([]byte("dbits:"))
	w.Write([]byte(hex.EncodeToString(t.journal.deleted.Bytes())))
	w.Write([]byte("\n"))
	return nil
}

func (t *Table) DumpPack(w io.Writer, i int, mode DumpMode) error {
	if i >= t.packidx.Len() || i < 0 {
		return ErrPackNotFound
	}
	tx, err := t.db.Tx(false)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	pkg, err := t.loadSharedPack(tx, t.packidx.Get(i).Key, false, nil)
	if err != nil {
		return err
	}
	return pkg.DumpData(w, mode, t.fields.Aliases())
}

func (t *Table) WalkPacks(fn func(*Package) error) error {
	tx, err := t.db.Tx(false)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for i := 0; i < t.packidx.Len(); i++ {
		pkg, err := t.loadSharedPack(tx, t.packidx.Get(i).Key, false, nil)
		if err != nil {
			return err
		}
		if err := fn(pkg); err != nil {
			return err
		}
	}
	return nil
}

func (t *Table) WalkPacksRange(start, end int, fn func(*Package) error) error {
	tx, err := t.db.Tx(false)
	if err != nil {
		return err
	}
	if start < 0 {
		start = 0
	}
	if end < 0 {
		end = t.packidx.Len() - 1
	}
	defer tx.Rollback()
	for i := start; i <= end && i < t.packidx.Len(); i++ {
		pkg, err := t.loadSharedPack(tx, t.packidx.Get(i).Key, false, nil)
		if err != nil {
			return err
		}
		if err := fn(pkg); err != nil {
			return err
		}
	}
	return nil
}

func (t *Table) DumpIndexPack(w io.Writer, i, p int, mode DumpMode) error {
	if i >= len(t.indexes) || i < 0 {
		return ErrIndexNotFound
	}
	if p >= t.indexes[i].packidx.Len() || p < 0 {
		return ErrPackNotFound
	}
	tx, err := t.db.Tx(false)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	pkg, err := t.indexes[i].loadPack(tx, t.indexes[i].packidx.Get(p).Key, false)
	if err != nil {
		return fmt.Errorf("pack %d not found: %v", t.indexes[i].packidx.Get(p).Key, err)
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
		fmt.Fprintf(w, "%-5s %-10s %-7s %-10s %-7s %-7s %-33s %-33s %-4s %-6s %-10s %-10s %7s %-10s\n",
			"#", "Key", "Block", "Type", "Rows", "Card", "Min", "Max", "Prec", "Comp", "Stored", "Heap", "Ratio", "GoType")
	}
	lineNo := 1
	for i := 0; i < t.packidx.Len(); i++ {
		pkg, err := t.loadSharedPack(tx, t.packidx.Get(i).Key, false, nil)
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

func (t *Table) DumpIndexPackHeaders(w io.Writer, mode DumpMode, sorted bool) error {
	tx, err := t.db.Tx(false)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for _, v := range t.indexes {
		if err := v.dumpPackHeaders(tx, w, mode, sorted); err != nil {
			return err
		}
	}
	return nil
}

func (idx *Index) dumpPackHeaders(tx *Tx, w io.Writer, mode DumpMode, sorted bool) error {
	switch mode {
	case DumpModeDec, DumpModeHex:
		fmt.Fprintf(w, "%-3s %-10s %-7s %-7s %-21s %-21s %-10s\n",
			"#", "Key", "Fields", "Values", "Min", "Max", "Size")
	}
	var (
		i   int
		err error
	)
	for i = 0; i < idx.packidx.Len(); i++ {
		switch mode {
		case DumpModeDec, DumpModeHex:
			fmt.Fprintf(w, "%-3d ", i)
		}
		if sorted {
			err = idx.packidx.GetSorted(i).Dump(w, mode, 2)
		} else {
			err = idx.packidx.Get(i).Dump(w, mode, 2)
		}
		if err != nil {
			return err
		}
	}
	switch mode {
	case DumpModeDec, DumpModeHex:
		fmt.Fprintf(w, "%-3d ", i)
		i++
	}
	info := idx.journal.Info()
	info.UpdateStats(idx.journal)
	if err := info.Dump(w, mode, 2); err != nil {
		return err
	}
	switch mode {
	case DumpModeDec, DumpModeHex:
		fmt.Fprintf(w, "%-3d ", i)
		i++
	}
	info = idx.tombstone.Info()
	info.UpdateStats(idx.tombstone)
	return info.Dump(w, mode, 2)
}

func (h PackInfo) Dump(w io.Writer, mode DumpMode, nfields int) error {
	var key string
	switch true {
	case h.Key == journalKey:
		key = "journal"
	case h.Key == tombstoneKey:
		key = "tombstone"
	default:
		key = util.ToString(h.KeyBytes())
	}
	head := h.Blocks[0]
	min, max := head.MinValue.(uint64), head.MaxValue.(uint64)
	switch mode {
	case DumpModeDec:
		_, err := fmt.Fprintf(w, "%-10s %-7d %-7d %-21d %-21d %-10s\n",
			key,
			nfields,
			h.NValues,
			min,
			max,
			util.ByteSize(h.Packsize))
		return err
	case DumpModeHex:
		_, err := fmt.Fprintf(w, "%-10s %-7d %-7d %21x %21x %-10s\n",
			key,
			nfields,
			h.NValues,
			min,
			max,
			util.ByteSize(h.Packsize))
		return err
	case DumpModeCSV:
		enc, ok := w.(*csv.Encoder)
		if !ok {
			enc = csv.NewEncoder(w)
		}
		ch := CSVHeader{
			Key:   key,
			Cols:  nfields,
			Rows:  h.NValues,
			MinPk: min,
			MaxPk: max,
			Size:  h.Packsize,
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
	case p.key == journalKey:
		key = "journal"
	case p.key == tombstoneKey:
		key = "tombstone"
	default:
		key = util.ToString(p.key)
	}
	fmt.Fprintf(w, "Package ------------------------------------ \n")
	fmt.Fprintf(w, "Key:        %s\n", key)
	// fmt.Fprintf(w, "Version:    %d\n", p.version)
	fmt.Fprintf(w, "Fields:     %d\n", p.nFields)
	fmt.Fprintf(w, "Values:     %d\n", p.nValues)
	fmt.Fprintf(w, "Pk index:   %d\n", p.pkindex)
	fmt.Fprintf(w, "Type:       %s\n", typname)
	fmt.Fprintf(w, "Size:       %s (%d) stored, %s (%d) heap\n",
		util.ByteSize(p.size), p.size,
		util.ByteSize(p.HeapSize()), p.HeapSize(),
	)
	pinfo := p.Info()
	pinfo.UpdateStats(p)
	for i, v := range p.blocks {
		d, fi := "", ""
		if v.IsDirty() {
			d = "*"
		}
		if p.tinfo != nil {
			fi = p.tinfo.fields[i].String()
		}
		field := p.fields[i]
		blockinfo := pinfo.Blocks[i]
		fmt.Fprintf(w, "Block %-02d:   %s typ=%s comp=%s scale=%d len=%d min=%s max=%s sz=%s %s %s\n",
			i,
			field.Name,
			field.Type,
			v.Compression(),
			field.Scale,
			v.Len(),
			util.ToString(blockinfo.MinValue),
			util.ToString(blockinfo.MaxValue),
			util.PrettyInt(v.CompressedSize()),
			fi,
			d)
	}
	return nil
}

func (p *Package) DumpBlocks(w io.Writer, mode DumpMode, lineNo int) (int, error) {
	var key string
	switch true {
	case p.key == journalKey:
		key = "journal"
	case p.key == tombstoneKey:
		key = "tombstone"
	default:
		key = util.ToString(p.Key())
	}
	info := p.Info()
	info.UpdateStats(p)
	switch mode {
	case DumpModeDec, DumpModeHex:
		for i, v := range p.blocks {
			gotype := "-"
			if p.tinfo != nil && p.tinfo.gotype {
				gotype = p.tinfo.fields[i].typname
			}
			blockinfo := info.Blocks[i]
			// reconstruct cardinality of missing
			if blockinfo.Cardinality == 0 && v.Len() > 0 {
				blockinfo.Cardinality = p.fields[i].Type.EstimateCardinality(v, 15)
			}
			_, err := fmt.Fprintf(w, "%-5d %-10s %-7d %-10s %-7d %-5d %-33s %-33s %-4d %-6s %-10s %-10s %7s %-10s\n",
				lineNo,
				key,      // pack key
				i,        // block id
				v.Type(), // block type
				v.Len(),  // block values
				blockinfo.Cardinality,
				util.LimitStringEllipsis(util.ToString(blockinfo.MinValue), 33), // min val in block
				util.LimitStringEllipsis(util.ToString(blockinfo.MaxValue), 33), // max val in block
				blockinfo.Scale,
				v.Compression(),
				util.PrettyInt(v.CompressedSize()), // compressed block size
				util.PrettyInt(v.HeapSize()),       // in-memory block size
				util.PrettyFloat64N(100-float64(v.CompressedSize())/float64(v.HeapSize())*100, 2)+"%",
				gotype, // type info type
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
		cols := make([]interface{}, 13)
		for i, v := range p.blocks {
			fi := ""
			if p.tinfo != nil {
				fi = p.tinfo.fields[i].String()
			}
			field := p.fields[i]
			blockinfo := info.Blocks[i]
			cols[0] = key
			cols[1] = i
			cols[2] = field.Name
			cols[3] = v.Type().String()
			cols[4] = v.Len()
			cols[5] = blockinfo.MinValue
			cols[6] = blockinfo.MaxValue
			cols[7] = blockinfo.Scale
			cols[8] = v.Compression().String()
			cols[9] = v.CompressedSize()
			cols[10] = v.HeapSize()
			cols[11] = v.MaxStoredSize()
			cols[12] = fi
			if !enc.HeaderWritten() {
				if err := enc.EncodeHeader([]string{
					"Pack",
					"Block",
					"Name",
					"Type",
					"Columns",
					"Min",
					"Max",
					"Flags",
					"Compression",
					"Compressed",
					"Heap",
					"Max",
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
	names := p.fields.Names()
	if len(aliases) == p.nFields && len(aliases[0]) > 0 {
		names = aliases
	}

	// estimate sizes from the first 500 values
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
				if p.blocks[j].Type() == block.BlockIgnore {
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
				if p.blocks[j].Type() == block.BlockIgnore {
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

func (n ConditionTreeNode) Dump() string {
	buf := bytes.NewBuffer(nil)
	n.dump(0, buf)
	return string(buf.Bytes())
}

func (n ConditionTreeNode) dump(level int, w io.Writer) {
	if n.Leaf() {
		fmt.Fprintln(w, strings.Repeat("  ", level), n.Cond.String())
	}
	if len(n.Children) > 0 {
		kind := "AND"
		if n.OrKind {
			kind = "OR"
		}
		fmt.Fprintln(w, strings.Repeat("  ", level), kind)
	}
	for _, v := range n.Children {
		v.dump(level+1, w)
	}
}

func (q Query) Dump() string {
	buf := bytes.NewBuffer(nil)
	fmt.Fprintln(buf, "Query:", q.Name, "=>")
	q.Conditions.dump(0, buf)
	return string(buf.Bytes())
}

func (j Join) Dump() string {
	buf := bytes.NewBuffer(nil)
	fmt.Fprintln(buf, "Join:", j.Type.String(), "=>")
	fmt.Fprintln(buf, "  Predicate:", j.Predicate.Left.Alias, j.Predicate.Mode.String(), j.Predicate.Right.Alias)
	fmt.Fprintln(buf, "  Left:", j.Left.Table.Name())
	fmt.Fprintln(buf, "  Where:")
	j.Left.Where.dump(0, buf)
	fmt.Fprintln(buf, "  Fields:", strings.Join(j.Left.Fields.Names(), ","))
	fmt.Fprintln(buf, "  AS:", strings.Join(j.Left.FieldsAs, ","))
	fmt.Fprintln(buf, "  Limit:", j.Left.Limit)
	fmt.Fprintln(buf, "  Right:", j.Right.Table.Name())
	fmt.Fprintln(buf, "  Where:")
	j.Right.Where.dump(0, buf)
	fmt.Fprintln(buf, "  Fields:", strings.Join(j.Right.Fields.Names(), ","))
	fmt.Fprintln(buf, "  AS:", strings.Join(j.Right.FieldsAs, ","))
	fmt.Fprintln(buf, "  Limit:", j.Right.Limit)
	return string(buf.Bytes())
}

func (r Result) Dump() string {
	buf := bytes.NewBuffer(nil)
	fmt.Fprintf(buf, "Result ------------------------------------ \n")
	fmt.Fprintf(buf, "Rows:       %d\n", r.Rows())
	fmt.Fprintf(buf, "Cols:       %d\n", len(r.fields))
	fmt.Fprintf(buf, "%-2s  %-15s  %-15s  %-10s  %-4s  %s\n", "No", "Name", "Alias", "Type", "Scale", "Flags")
	for _, v := range r.fields {
		fmt.Fprintf(buf, "%02d  %-15s  %-15s  %-10s  %2d    %s\n",
			v.Index, v.Name, v.Alias, v.Type, v.Scale, v.Flags)
	}
	return buf.String()
}

func (p *Package) Validate() error {
	if a, b := len(p.fields), p.nFields; a != b {
		return fmt.Errorf("pack: mismatch len %d/%d", a, b)
	}
	if a, b := len(p.fields), len(p.blocks); a != b {
		return fmt.Errorf("pack: mismatch block len %d/%d", a, b)
	}

	for i, f := range p.fields {
		b := p.blocks[i]
		if a, b := b.Type(), f.Type.BlockType(); a != b {
			return fmt.Errorf("pack: mismatch block type %s/%s", a, b)
		}
		if a, b := p.nValues, b.Len(); a != b {
			return fmt.Errorf("pack: mismatch block len %d/%d", a, b)
		}
		switch f.Type {
		case FieldTypeBytes:
			if b.Bytes == nil {
				return fmt.Errorf("pack: nil %s block", f.Type)
			} else if a, b := len(b.Bytes), p.nValues; a != b {
				return fmt.Errorf("pack: mismatch %s block slice len %d/%d", f.Type, a, b)
			}

		case FieldTypeString:
			if b.Strings == nil {
				return fmt.Errorf("pack: nil %s block", f.Type)
			} else if a, b := len(b.Strings), p.nValues; a != b {
				return fmt.Errorf("pack: mismatch %s block slice len %d/%d", f.Type, a, b)
			}

		case FieldTypeBoolean:
			if b.Bits == nil {
				return fmt.Errorf("pack: nil %s block", f.Type)
			} else if a, b := b.Bits.Len(), p.nValues; a != b {
				return fmt.Errorf("pack: mismatch %s block slice len %d/%d", f.Type, a, b)
			}

		case FieldTypeFloat64:
			if b.Float64 == nil {
				return fmt.Errorf("pack: nil %s block", f.Type)
			} else if a, b := len(b.Float64), p.nValues; a != b {
				return fmt.Errorf("pack: mismatch %s block slice len %d/%d", f.Type, a, b)
			}

		case FieldTypeFloat32:
			if b.Float32 == nil {
				return fmt.Errorf("pack: nil %s block", f.Type)
			} else if a, b := len(b.Float32), p.nValues; a != b {
				return fmt.Errorf("pack: mismatch %s block slice len %d/%d", f.Type, a, b)
			}

		case FieldTypeInt256, FieldTypeDecimal256:
			if b.Int256.IsNil() {
				return fmt.Errorf("pack: nil %s block", f.Type)
			} else if a, b := b.Int256.Len(), p.nValues; a != b {
				return fmt.Errorf("pack: mismatch %s block slice len %d/%d", f.Type, a, b)
			}

		case FieldTypeInt128, FieldTypeDecimal128:
			if b.Int128.IsNil() {
				return fmt.Errorf("pack: nil %s block", f.Type)
			} else if a, b := b.Int128.Len(), p.nValues; a != b {
				return fmt.Errorf("pack: mismatch %s block slice len %d/%d", f.Type, a, b)
			}

		case FieldTypeInt64, FieldTypeDatetime, FieldTypeDecimal64:
			if b.Int64 == nil {
				return fmt.Errorf("pack: nil %s block", f.Type)
			} else if a, b := len(b.Int64), p.nValues; a != b {
				return fmt.Errorf("pack: mismatch %s block slice len %d/%d", f.Type, a, b)
			}

		case FieldTypeInt32, FieldTypeDecimal32:
			if b.Int32 == nil {
				return fmt.Errorf("pack: nil %s block", f.Type)
			} else if a, b := len(b.Int32), p.nValues; a != b {
				return fmt.Errorf("pack: mismatch %s block slice len %d/%d", f.Type, a, b)
			}

		case FieldTypeInt16:
			if b.Int16 == nil {
				return fmt.Errorf("pack: nil %s block", f.Type)
			} else if a, b := len(b.Int16), p.nValues; a != b {
				return fmt.Errorf("pack: mismatch %s block slice len %d/%d", f.Type, a, b)
			}

		case FieldTypeInt8:
			if b.Int8 == nil {
				return fmt.Errorf("pack: nil %s block", f.Type)
			} else if a, b := len(b.Int8), p.nValues; a != b {
				return fmt.Errorf("pack: mismatch %s block slice len %d/%d", f.Type, a, b)
			}

		case FieldTypeUint64:
			if b.Uint64 == nil {
				return fmt.Errorf("pack: nil %s block", f.Type)
			} else if a, b := len(b.Uint64), p.nValues; a != b {
				return fmt.Errorf("pack: mismatch %s block slice len %d/%d", f.Type, a, b)
			}

		case FieldTypeUint32:
			if b.Uint32 == nil {
				return fmt.Errorf("pack: nil %s block", f.Type)
			} else if a, b := len(b.Uint32), p.nValues; a != b {
				return fmt.Errorf("pack: mismatch %s block slice len %d/%d", f.Type, a, b)
			}
		case FieldTypeUint16:
			if b.Uint16 == nil {
				return fmt.Errorf("pack: nil %s block", f.Type)
			} else if a, b := len(b.Uint16), p.nValues; a != b {
				return fmt.Errorf("pack: mismatch %s block slice len %d/%d", f.Type, a, b)
			}

		case FieldTypeUint8:
			if b.Uint8 == nil {
				return fmt.Errorf("pack: nil %s block", f.Type)
			} else if a, b := len(b.Uint8), p.nValues; a != b {
				return fmt.Errorf("pack: mismatch %s block slice len %d/%d", f.Type, a, b)
			}

		}
	}
	return nil
}

func (l PackIndex) Validate() []error {
	errs := make([]error, 0)
	for i := range l.packs {
		head := l.packs[i]
		if head.NValues == 0 {
			errs = append(errs, fmt.Errorf("%03d empty pack", head.Key))
		}
		// check min <= max
		min, max := l.minpks[i], l.maxpks[i]
		if min > max {
			errs = append(errs, fmt.Errorf("%03d min %d > max %d", head.Key, min, max))
		}
		// check invariant
		// - id's don't overlap between packs
		// - same key can span many packs, so min_a == max_b
		// - for long rows of same keys min_a == max_a
		for j := range l.packs {
			if i == j {
				continue
			}
			jmin, jmax := l.minpks[j], l.maxpks[j]
			dist := jmax - jmin + 1

			// single key packs are allowed
			if min == max {
				// check the signle key is not between any other pack (exclusing)
				if jmin < min && jmax > max {
					errs = append(errs, fmt.Errorf("%03d overlaps %03d - key %d E [%d:%d]",
						head.Key, l.packs[j].Key, min, jmin, jmax))
				}
			} else {
				// check min val is not contained in any other pack unless continued
				if min != jmin && min != jmax && min-jmin < dist {
					errs = append(errs, fmt.Errorf("%03d overlaps %03d - min %d E [%d:%d]",
						head.Key, l.packs[j].Key, min, jmin, jmax))
				}

				// check max val is not contained in any other pack unless continued
				if max != jmin && max-jmin < dist {
					errs = append(errs, fmt.Errorf("%03d overlaps %03d - max %d E [%d:%d]",
						head.Key, l.packs[j].Key, max, jmin, jmax))
				}
			}
		}
	}
	if len(errs) == 0 {
		return nil
	}
	return errs
}

func (t *Table) ValidatePackHeaders(w io.Writer) error {
	if errs := t.packidx.Validate(); errs != nil {
		for _, v := range errs {
			w.Write([]byte(v.Error() + "\n"))
		}
	}
	return nil
}

func (t *Table) ValidateIndexPackHeaders(w io.Writer) error {
	for _, idx := range t.indexes {
		if errs := idx.packidx.Validate(); errs != nil {
			for _, v := range errs {
				w.Write([]byte(fmt.Sprintf("%s: %v\n", idx.Name, v.Error())))
			}
		}
	}
	return nil
}
