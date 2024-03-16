// Copyright (c) 2018-2024 Blockwatch Data Inc.
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

func (t *PackTable) DumpType(w io.Writer) error {
	return t.journal.DataPack().DumpType(w)
}

func (t *PackTable) DumpPackInfo(w io.Writer, mode DumpMode, sorted bool) error {
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

func (t *PackTable) DumpJournal(w io.Writer, mode DumpMode) error {
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

func (t *PackTable) DumpPackInfoDetail(w io.Writer, mode DumpMode, sorted bool) error {
	switch mode {
	case DumpModeDec, DumpModeHex:
	default:
		// unsupported
		return nil
	}
	tx, err := t.db.Tx(false)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var i int
	for i = 0; i < t.packidx.Len(); i++ {
		var info PackInfo
		if sorted {
			info = t.packidx.GetSorted(i)
		} else {
			info = t.packidx.Get(i)
		}
		info.DumpDetail(w)
	}
	return nil
}

func (t *PackTable) DumpPack(w io.Writer, i int, mode DumpMode) error {
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
	err = pkg.DumpData(w, mode, t.fields.Aliases())
	if err != nil {
		return err
	}
	t.releaseSharedPack(pkg)
	return nil
}

func (t *PackTable) WalkPacks(fn func(*Package) error) error {
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
		t.releaseSharedPack(pkg)
	}
	return nil
}

func (t *PackTable) WalkPacksRange(start, end int, fn func(*Package) error) error {
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
		t.releaseSharedPack(pkg)
	}
	return nil
}

func (t *PackTable) DumpIndexPack(w io.Writer, i, p int, mode DumpMode) error {
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
	pkg, err := t.indexes[i].loadSharedPack(tx, t.indexes[i].packidx.Get(p).Key, false)
	if err != nil {
		return fmt.Errorf("pack %d not found: %v", t.indexes[i].packidx.Get(p).Key, err)
	}

	err = pkg.DumpData(w, mode, t.fields.Aliases())
	if err != nil {
		return err
	}
	t.indexes[i].releaseSharedPack(pkg)
	return nil
}

func (t *PackTable) DumpPackBlocks(w io.Writer, mode DumpMode) error {
	tx, err := t.db.Tx(false)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	switch mode {
	case DumpModeDec, DumpModeHex:
		fmt.Fprintf(w, "%-5s %-10s %-7s %-10s %-7s %-5s %-33s %-33s %-4s %-6s %-10s %-10s %7s %-10s\n",
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
		t.releaseSharedPack(pkg)
	}
	return nil
}

func (t *PackTable) DumpIndexPackInfo(w io.Writer, idx int, mode DumpMode, sorted bool) error {
	if len(t.indexes) <= idx {
		return ErrNoIndex
	}
	tx, err := t.db.Tx(false)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	return t.indexes[idx].dumpPackInfo(tx, w, mode, sorted)
}

func (idx *PackIndex) dumpPackInfo(tx *Tx, w io.Writer, mode DumpMode, sorted bool) error {
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

func (i PackInfo) DumpDetail(w io.Writer) error {
	fmt.Fprintf(w, "Pack Key   %08x ------------------------------------\n", i.Key)
	fmt.Fprintf(w, "Values     %s\n", util.PrettyInt(i.NValues))
	fmt.Fprintf(w, "Pack Size  %s\n", util.ByteSize(i.Packsize))
	fmt.Fprintf(w, "Meta Size  %s\n", util.ByteSize(i.HeapSize()))
	fmt.Fprintf(w, "Blocks     %d\n", len(i.Blocks))
	fmt.Fprintf(w, "%-3s %-10s %-7s %-7s %-7s %-33s %-33s %-10s\n",
		"#", "Type", "Comp", "Scale", "Card", "Min", "Max", "Bloom")
	for id, head := range i.Blocks {
		var bloom string
		if head.Bloom != nil {
			bloom = strconv.Itoa(int(head.Bloom.Len()))
		} else {
			bloom = "--"
		}
		fmt.Fprintf(w, "%-3d %-10s %-7s %-7d %-7d %-33s %-33s %-10s\n",
			id,
			head.Type,
			head.Compression,
			head.Scale,
			head.Cardinality,
			util.LimitStringEllipsis(util.ToString(head.MinValue), 33),
			util.LimitStringEllipsis(util.ToString(head.MaxValue), 33),
			bloom,
		)
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
				gotype = p.tinfo.fields[i].typ.String()
			}
			blockinfo := info.Blocks[i]
			// reconstruct cardinality when missing
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
		for i, l := 0, min(500, p.nValues); i < l; i++ {
			for j := 0; j < p.nFields; j++ {
				var str string
				if p.blocks[j].IsIgnore() {
					str = "[strip]"
				} else {
					val, _ := p.FieldAt(j, i)
					str = util.ToString(val)
				}
				sz[j] = max(sz[j], len(str))
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
				if p.blocks[j].IsIgnore() {
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

func (n UnboundCondition) Dump() string {
	buf := bytes.NewBuffer(nil)
	n.dump(0, buf)
	return string(buf.Bytes())
}

func (n UnboundCondition) dump(level int, w io.Writer) {
	if n.Leaf() {
		fmt.Fprintln(w, strings.Repeat("  ", level), n.String())
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
	if q.IsBound() {
		fmt.Fprintln(buf, "Q>", q.Name, "=>", "SELECT(", strings.Join(q.fout.Aliases(), ", "), ") WHERE")
		q.conds.dump(0, buf)
		fmt.Fprintln(buf, ">> fields:", strings.Join(q.freq.Aliases(), ", "))
	} else {
		fmt.Fprintln(buf, "Q>", q.Name, "=>", "SELECT(", strings.Join(q.Fields, ", "), ") WHERE")
		q.Conditions.dump(0, buf)
		if q.Order == OrderDesc {
			fmt.Fprintf(buf, "ORDER BY ID DESC ")
		}
		if q.Limit > 0 || q.Offset > 0 {
			fmt.Fprintf(buf, "LIMIT %d OFFSET %d", q.Limit, q.Offset)
		}
		if q.NoCache || q.NoIndex {
			nc, ni := " NOCACHE", " NOINDEX"
			if !q.NoCache {
				nc = ""
			}
			if !q.NoIndex {
				ni = ""
			}
			fmt.Fprintf(buf, "%s%s", nc, ni)
		}
		fmt.Fprintln(buf)
	}
	return string(buf.Bytes())
}

func (j Join) Dump() string {
	buf := bytes.NewBuffer(nil)
	fmt.Fprintln(buf, "Join:", j.Type.String(), "=>")
	fmt.Fprintln(buf, "  Predicate:", j.Predicate.Left.Alias, j.Predicate.Mode.String(), j.Predicate.Right.Alias)
	fmt.Fprintln(buf, "  Left:", j.Left.Table.Name())
	fmt.Fprintln(buf, "  Where:")
	j.Left.Where.dump(0, buf)
	fmt.Fprintln(buf, "  Fields:", strings.Join(j.Left.Fields, ","))
	fmt.Fprintln(buf, "  AS:", strings.Join(j.Left.FieldsAs, ","))
	fmt.Fprintln(buf, "  Limit:", j.Left.Limit)
	fmt.Fprintln(buf, "  Right:", j.Right.Table.Name())
	fmt.Fprintln(buf, "  Where:")
	j.Right.Where.dump(0, buf)
	fmt.Fprintln(buf, "  Fields:", strings.Join(j.Right.Fields, ","))
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
