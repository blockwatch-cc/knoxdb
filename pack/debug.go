// Copyright (c) 2018-2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package pack

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"math/bits"
	"reflect"
	"strconv"
	"strings"
	"time"
	"unsafe"

	logpkg "github.com/echa/log"

	"blockwatch.cc/knoxdb/encoding/block"
	"blockwatch.cc/knoxdb/encoding/compress"
	"blockwatch.cc/knoxdb/encoding/csv"
	"blockwatch.cc/knoxdb/encoding/s8bVec"
	"blockwatch.cc/knoxdb/util"
	"github.com/golang/snappy"
	"github.com/pierrec/lz4"
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

func (t *Table) DumpPackInfo(w io.Writer, mode DumpMode, sorted bool) error {
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

func (t *Table) DumpPackInfoDetail(w io.Writer, mode DumpMode, sorted bool) error {
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
	pkg, err := t.indexes[i].loadSharedPack(tx, t.indexes[i].packidx.Get(p).Key, false)
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
	}
	return nil
}

func (t *Table) DumpIndexPackInfo(w io.Writer, idx int, mode DumpMode, sorted bool) error {
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

func (idx *Index) dumpPackInfo(tx *Tx, w io.Writer, mode DumpMode, sorted bool) error {
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
		var blen uint
		if head.Bloom != nil {
			blen = head.Bloom.Len()
		}
		fmt.Fprintf(w, "%-3d %-10s %-7s %-7d %-7d %-33s %-33s %-10d\n",
			id,
			head.Type,
			head.Compression,
			head.Scale,
			head.Cardinality,
			util.LimitStringEllipsis(util.ToString(head.MinValue), 33),
			util.LimitStringEllipsis(util.ToString(head.MaxValue), 33),
			blen,
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
				gotype = p.tinfo.fields[i].typname
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

func ReintepretUint64ToByteSlice(src []uint64) []byte {
	header := *(*reflect.SliceHeader)(unsafe.Pointer(&src))
	header.Len *= 8
	header.Cap *= 8
	return *(*[]byte)(unsafe.Pointer(&header))
}

func ReintepretAnySliceToByteSlice(src interface{}) []byte {
	var header reflect.SliceHeader
	v := reflect.ValueOf(src)
	so := int(reflect.TypeOf(src).Elem().Size())
	header.Data = v.Pointer()
	header.Len = so * v.Len()
	header.Cap = so * v.Cap()
	return *(*[]byte)(unsafe.Pointer(&header))
}

func ReintepretByteSliceToAnySlice(src []byte, dst interface{}) interface{} {
	t := reflect.TypeOf(dst)
	so := int(t.Elem().Size())

	slice := reflect.NewAt(t, unsafe.Pointer(&src))
	slice2 := slice.Elem().Slice3(0, len(src)/so, cap(src)/so)

	return slice2.Interface()
}

func convertBlockToByteSlice(b *block.Block) []byte {
	var buf []byte
	switch b.Type() {
	case block.BlockBool:
		buf = b.Bits.Bytes()
	case block.BlockUint64:
		buf = ReintepretAnySliceToByteSlice(b.Uint64)
	case block.BlockUint32:
		buf = ReintepretAnySliceToByteSlice(b.Uint32)
	case block.BlockUint16:
		buf = ReintepretAnySliceToByteSlice(b.Uint16)
	case block.BlockUint8:
		buf = ReintepretAnySliceToByteSlice(b.Uint8)
	case block.BlockInt64, block.BlockTime:
		buf = ReintepretAnySliceToByteSlice(b.Int64)
	case block.BlockInt32:
		buf = ReintepretAnySliceToByteSlice(b.Int32)
	case block.BlockInt16:
		buf = ReintepretAnySliceToByteSlice(b.Int16)
	case block.BlockInt8:
		buf = ReintepretAnySliceToByteSlice(b.Int8)
	}
	return buf
}

func convertByteSliceToBlock(b *block.Block, src []byte) {
	switch b.Type() {
	case block.BlockBool:
		b.Bits = b.Bits.SetFromBytes(src, 8*len(src))
	case block.BlockUint64:
		b.Uint64 = ReintepretByteSliceToAnySlice(src, b.Uint64).([]uint64)
	case block.BlockUint32:
		b.Uint32 = ReintepretByteSliceToAnySlice(src, b.Uint32).([]uint32)
	case block.BlockUint16:
		b.Uint16 = ReintepretByteSliceToAnySlice(src, b.Uint16).([]uint16)
	case block.BlockUint8:
		b.Uint8 = ReintepretByteSliceToAnySlice(src, b.Uint8).([]uint8)
	case block.BlockInt64, block.BlockTime:
		b.Int64 = ReintepretByteSliceToAnySlice(src, b.Int64).([]int64)
	case block.BlockInt32:
		b.Int32 = ReintepretByteSliceToAnySlice(src, b.Int32).([]int32)
	case block.BlockInt16:
		b.Int16 = ReintepretByteSliceToAnySlice(src, b.Int16).([]int16)
	case block.BlockInt8:
		b.Int8 = ReintepretByteSliceToAnySlice(src, b.Int8).([]int8)
	}
}

func convertBlockToUint64(b *block.Block) []uint64 {
	buf := make([]uint64, b.Len())
	switch b.Type() {
	case block.BlockUint64:
		for i, v := range b.Uint64 {
			buf[i] = uint64(v)
		}
	case block.BlockUint32:
		for i, v := range b.Uint32 {
			buf[i] = uint64(v)
		}
	case block.BlockUint16:
		for i, v := range b.Uint16 {
			buf[i] = uint64(v)
		}
	case block.BlockUint8:
		for i, v := range b.Uint8 {
			buf[i] = uint64(v)
		}
	case block.BlockInt64, block.BlockTime:
		for i, v := range b.Int64 {
			buf[i] = uint64(v)
		}
	case block.BlockInt32:
		for i, v := range b.Int32 {
			buf[i] = uint64(v)
		}
	case block.BlockInt16:
		for i, v := range b.Int16 {
			buf[i] = uint64(v)
		}
	case block.BlockInt8:
		for i, v := range b.Int8 {
			buf[i] = uint64(v)
		}
	}
	return buf
}

func convertInt64ToBlock(b *block.Block, src []int64) {
	switch b.Type() {
	case block.BlockUint64:
		b.Uint64 = b.Uint64[:len(src)]
		for i, v := range src {
			b.Uint64[i] = uint64(v)
		}
	case block.BlockUint32:
		b.Uint32 = b.Uint32[:len(src)]
		for i, v := range src {
			b.Uint32[i] = uint32(v)
		}
	case block.BlockUint16:
		b.Uint16 = b.Uint16[:len(src)]
		for i, v := range src {
			b.Uint16[i] = uint16(v)
		}
	case block.BlockUint8:
		b.Uint8 = b.Uint8[:len(src)]
		for i, v := range src {
			b.Uint8[i] = uint8(v)
		}
	case block.BlockInt64, block.BlockTime:
		b.Int64 = b.Int64[:len(src)]
		for i, v := range src {
			b.Int64[i] = int64(v)
		}
	case block.BlockInt32:
		b.Int32 = b.Int32[:len(src)]
		for i, v := range src {
			b.Int32[i] = int32(v)
		}
	case block.BlockInt16:
		b.Int16 = b.Int16[:len(src)]
		for i, v := range src {
			b.Int16[i] = int16(v)
		}
	case block.BlockInt8:
		b.Int8 = b.Int8[:len(src)]
		for i, v := range src {
			b.Int8[i] = int8(v)
		}
	}
}

func convertUint64ToBlock(b *block.Block, src []uint64) {
	switch b.Type() {
	case block.BlockUint64:
		b.Uint64 = b.Uint64[:len(src)]
		for i, v := range src {
			b.Uint64[i] = uint64(v)
		}
	case block.BlockUint32:
		b.Uint32 = b.Uint32[:len(src)]
		for i, v := range src {
			b.Uint32[i] = uint32(v)
		}
	case block.BlockUint16:
		b.Uint16 = b.Uint16[:len(src)]
		for i, v := range src {
			b.Uint16[i] = uint16(v)
		}
	case block.BlockUint8:
		b.Uint8 = b.Uint8[:len(src)]
		for i, v := range src {
			b.Uint8[i] = uint8(v)
		}
	case block.BlockInt64, block.BlockTime:
		b.Int64 = b.Int64[:len(src)]
		for i, v := range src {
			b.Int64[i] = int64(v)
		}
	case block.BlockInt32:
		b.Int32 = b.Int32[:len(src)]
		for i, v := range src {
			b.Int32[i] = int32(v)
		}
	case block.BlockInt16:
		b.Int16 = b.Int16[:len(src)]
		for i, v := range src {
			b.Int16[i] = int16(v)
		}
	case block.BlockInt8:
		b.Int8 = b.Int8[:len(src)]
		for i, v := range src {
			b.Int8[i] = int8(v)
		}
	}
}

func compressSnappy(b *block.Block) ([]byte, int, error) {
	src := convertBlockToByteSlice(b)
	if src == nil {
		return nil, -1, nil
	}
	dst := snappy.Encode(nil, src)
	if dst != nil {
		return dst, len(dst), nil
	}
	return nil, -1, nil
}

func uncompressSnappy(b *block.Block, src []byte) (int, error) {
	if src == nil {
		return -1, nil
	}
	dst, err := snappy.Decode(nil, src)
	convertByteSliceToBlock(b, dst)
	if err != nil {
		return -1, err
	}
	return b.Len(), nil
}

func compressLz4(b *block.Block) ([]byte, int, error) {
	src := convertBlockToByteSlice(b)
	if src == nil {
		return nil, -1, nil
	}

	dst := make([]byte, len(src))
	ht := make([]int, 64<<10) // buffer for the compression table

	n, err := lz4.CompressBlock(src, dst, ht)
	if err != nil {
		return nil, -1, err
	}

	return dst[:n], n, nil
}

func uncompressLz4(b *block.Block, src []byte, size int) (int, error) {
	if src == nil {
		return -1, nil
	}
	dst := make([]byte, size)
	_, err := lz4.UncompressBlock(src, dst)
	if err != nil {
		return -1, err
	}
	convertByteSliceToBlock(b, dst)
	return b.Len(), nil
}

type CompressedHashBlock struct {
	hash_size   int
	hash_nbytes int
	hash_data   []byte
	pk_nbytes   int
	pk_data     []byte
}

func compressHashBlock(pkg Package, hash_size int) (CompressedHashBlock, error) {
	// compress hash block
	b := pkg.blocks[0]
	deltas := make([]uint64, len(b.Uint64))
	shift := 64 - hash_size
	for i := range b.Uint64 {
		deltas[i] = b.Uint64[i] >> shift
	}

	// delta encoding

	/* maxdelta := uint64(0)
	for i := len(deltas) - 1; i > 7; i-- {
		deltas[i] = deltas[i] - deltas[i-8]
		maxdelta |= deltas[i]
	}*/

	maxdelta := compress.Delta8AVX2(deltas)
	for i := len(deltas)%8 + 7; i > 7; i-- {
		deltas[i] = deltas[i] - deltas[i-8]
		maxdelta |= deltas[i]
	}

	var nbytes, nbytes2 int
	if maxdelta == 0 {
		nbytes = 1 // all number zero -> use 1 byte
	} else {
		lz := bits.LeadingZeros64(maxdelta)
		nbytes = (71 - lz) >> 3 // = (64 - tz + 8 - 1) / 8 = ceil((64 - tz)/8)
	}

	buf := make([]byte, nbytes*(len(deltas)-8)+64)

	for i := 0; i < 8; i++ {
		binary.BigEndian.PutUint64(buf[8*i:], deltas[i])
	}

	_, err := compressBytes(deltas[8:], nbytes, buf[64:])
	if err != nil {
		return CompressedHashBlock{0, 0, nil, 0, nil}, err
	}

	// compress hash block
	b = pkg.blocks[1]

	src_max := uint64(0)
	for _, v := range b.Uint64 {
		src_max |= v
	}
	if src_max == 0 {
		nbytes2 = 1 // all number zero -> use 1 byte
	} else {
		lz := bits.LeadingZeros64(src_max)
		nbytes2 = (71 - lz) >> 3 // = (64 - tz + 8 - 1) / 8 = ceil((64 - tz)/8)
	}

	buf2 := make([]byte, nbytes2*len(b.Uint64))

	_, err = compressBytes(b.Uint64, nbytes2, buf2)
	if err != nil {
		return CompressedHashBlock{0, 0, nil, 0, nil}, err
	}

	return CompressedHashBlock{hash_size, nbytes, buf, nbytes2, buf2}, nil
}

func compressBytes(src []uint64, nbytes int, buf []byte) ([]byte, error) {
	var tmp []byte

	if len(buf) < nbytes*len(src) {
		return nil, fmt.Errorf("compressBytes: write buffer to small")
	}

	switch nbytes {
	case 1:
		for i, v := range src {
			buf[i] = byte(v & 0xff)
		}
	case 2:
		/*		for i, v := range src {
				buf[2*i] = byte((v >> 8) & 0xff)
				buf[1+2*i] = byte(v & 0xff)
			}*/

		len_head := len(src) & 0x7ffffffffffffff0
		compress.PackIndex16BitAVX2(src, buf)

		tmp = buf[len_head*2:]

		for i, v := range src[len_head:] {
			tmp[2*i] = byte((v >> 8) & 0xff)
			tmp[1+2*i] = byte(v & 0xff)
		}

	case 3:
		for i, v := range src {
			buf[3*i] = byte((v >> 16) & 0xff)
			buf[1+3*i] = byte((v >> 8) & 0xff)
			buf[2+3*i] = byte(v & 0xff)
		}
	case 4:

		len_head := len(src) & 0x7ffffffffffffff8
		compress.PackIndex32BitAVX2(src, buf)

		tmp = buf[len_head*4:]

		for i, v := range src[len_head:] {
			tmp[4*i] = byte((v >> 24) & 0xff)
			tmp[1+4*i] = byte((v >> 16) & 0xff)
			tmp[2+4*i] = byte((v >> 8) & 0xff)
			tmp[3+4*i] = byte(v & 0xff)
		}
		/*for i, v := range deltas[8:] {
			buf[4*i] = byte((v >> 24) & 0xff)
			buf[1+4*i] = byte((v >> 16) & 0xff)
			buf[2+4*i] = byte((v >> 8) & 0xff)
			buf[3+4*i] = byte(v & 0xff)
		}*/
	default:
		return nil, fmt.Errorf("hash size (%d bytes) not yet implemented", nbytes)
	}
	return buf, nil
}

func uncompressBytes(src []byte, nbytes int, res []uint64) ([]uint64, error) {
	rlen := len(src) / nbytes

	if len(res) < rlen {
		return nil, fmt.Errorf("uncompressBytes: write buffer to small")
	}

	switch nbytes {
	case 1:
		for i, j := 0, 0; i < rlen; i++ {
			res[i] = uint64(src[j])
			j++
		}
	case 2:
		/*		for i, j := 0, 0; i < len; i++ {
				res[i] = uint64(src[j])<<8 | uint64(src[1+j])
				j += 2
			}*/

		len_head := rlen & 0x7ffffffffffffff0
		compress.UnpackIndex16BitAVX2(src, res)

		tmp := src[len_head*2:]

		for i, j := len_head, 0; i < rlen; i++ {
			res[i] = uint64(tmp[j])<<8 | uint64(tmp[1+j])
			j += 2
		}

	case 3:
		for i, j := 0, 0; i < rlen; i++ {
			res[i] = uint64(src[j])<<16 | uint64(src[1+j])<<8 | uint64(src[2+j])
			j += 3
		}
	case 4:
		/*for i, j := 0, 0; i < rlen; i++ {
			res[i] = uint64(src[j])<<24 | uint64(src[1+j])<<16 | uint64(src[2+j])<<8 | uint64(src[3+j])
			j += 4
		}*/

		len_head := rlen & 0x7ffffffffffffff8
		compress.UnpackIndex32BitAVX2(src, res)

		tmp := src[len_head*4:]

		for i, j := len_head, 0; i < rlen; i++ {
			res[i] = uint64(tmp[j])<<24 | uint64(tmp[1+j])<<16 | uint64(tmp[2+j])<<8 | uint64(tmp[3+j])
			j += 4
		}

	default:
		return nil, fmt.Errorf("hash size (%d bytes) not yet implemented", nbytes)
	}
	return res, nil
}

func uncompressHashBlock(chb CompressedHashBlock) ([]uint64, []uint64, error) {
	// uncompress hashes
	lenr := (len(chb.hash_data)-64)/chb.hash_nbytes + 8
	res1 := make([]uint64, lenr)
	for i := 0; i < 8; i++ {
		res1[i] = binary.BigEndian.Uint64(chb.hash_data[8*i:])
	}

	_, err := uncompressBytes(chb.hash_data[64:], chb.hash_nbytes, res1[8:])

	if err != nil {
		return nil, nil, err
	}

	len_head := lenr & 0x7ffffffffffffff8
	compress.Undelta8AVX2(res1)
	for i := len_head; i < lenr; i++ {
		res1[i] += res1[i-8]
	}

	/*for i := 8; i < len; i++ {
		res[i] += res[i-8]
	}*/

	// uncompress pks
	lenr = len(chb.pk_data) / chb.pk_nbytes
	res2 := make([]uint64, lenr)

	_, err = uncompressBytes(chb.pk_data, chb.pk_nbytes, res2)

	if err != nil {
		return nil, nil, err
	}

	return res1, res2, nil
}

func (p *Package) compressIdx(cmethod string) ([]float64, []float64, []float64, error) {
	cr := make([]float64, p.nFields)
	ct := make([]float64, p.nFields)
	dt := make([]float64, p.nFields)

	var tcomp float64 = -1
	var tdecomp float64 = -1
	var hashlen int
	var err error

	switch {
	case len(cmethod) > 10 && cmethod[:10] == "delta-hash":
		hashlen, err = strconv.Atoi(cmethod[10:])
	case len(cmethod) > 11 && cmethod[:11] == "linear-hash":
		hashlen, err = strconv.Atoi(cmethod[11:])
	default:
		return p.compress(cmethod)
	}

	if err != nil || hashlen < 1 || hashlen > 64 {
		return nil, nil, nil, fmt.Errorf("unknown compression method %s", cmethod)
	}

	// build new Hash
	data := make([]uint64, p.blocks[0].Len())
	for i, v := range p.blocks[0].Uint64 {
		data[i] = v >> (64 - hashlen)
	}

	var csize1, csize2 int
	var res1, res2 []uint64

	switch {
	case cmethod[:10] == "delta-hash":
		start := time.Now()
		chb, err := compressHashBlock(*p, hashlen)
		csize1 = len(chb.hash_data)
		csize2 = len(chb.pk_data)

		tcomp = time.Since(start).Seconds()
		if err == nil {
			start = time.Now()
			res1, _, err = uncompressHashBlock(chb)
			tdecomp = time.Since(start).Seconds()
		}

	default:
		return nil, nil, nil, fmt.Errorf("not yet implemented %s", cmethod)

	}

	if err != nil {
		return nil, nil, nil, err
	}
	for i := range res1 {
		if res1[i] != data[i] {
			fmt.Printf("hash compression: error at position %d\n", i)
		}
	}
	for i := range res2 {
		if res2[i] != p.blocks[1].Uint64[i] {
			fmt.Printf("pk compression: error at position %d\n", i)
		}
	}

	if csize1 < 0 {
		ct[0] = -1
		dt[0] = -1
	} else {
		ct[0] = float64(p.blocks[0].HeapSize()+p.blocks[1].HeapSize()) / tcomp / 1000000
		dt[0] = float64(p.blocks[0].HeapSize()+p.blocks[1].HeapSize()) / tdecomp / 1000000
	}
	cr[0] = float64(csize1) / float64(p.blocks[0].HeapSize())
	cr[1] = float64(csize2) / float64(p.blocks[0].HeapSize())

	return cr, ct, dt, nil
}

func (p *Package) compress(cmethod string) ([]float64, []float64, []float64, error) {
	cr := make([]float64, p.nFields)
	ct := make([]float64, p.nFields)
	dt := make([]float64, p.nFields)

	for j := 0; j < p.nFields; j++ {
		b := p.blocks[j]
		if !b.IsInt() {
			cr[j] = -1
			ct[j] = -1
			dt[j] = -1
			continue
		}
		b2 := block.NewBlock(b.Type(), b.Compression(), b.Len())
		check := true
		var csize int = -1
		var tcomp float64 = -1
		var tdecomp float64 = -1
		var err error
		switch cmethod {
		case "legacy", "legacy-no", "legacy-lz4", "legacy-snappy":
			buf := bytes.NewBuffer(make([]byte, 0, b.MaxStoredSize()))
			switch cmethod {
			case "legacy-no":
				b.SetCompression(block.NoCompression)
			case "legacy-snappy":
				b.SetCompression(block.SnappyCompression)
			case "legacy-lz4":
				b.SetCompression(block.LZ4Compression)
			}
			start := time.Now()
			csize, err = b.Encode(buf)
			tcomp = time.Since(start).Seconds()
			if err == nil {
				start = time.Now()
				err = b2.Decode(buf.Bytes(), b.Len(), b2.MaxStoredSize())
				tdecomp = time.Since(start).Seconds()
			}
		case "delta-s8b":
			src := convertBlockToUint64(b)
			start := time.Now()
			if compress.ZzDeltaEncodeUint64(src) >= 1<<60 {
				fmt.Printf("\nCannot s8b compress pack %v block %v\n", p.key, j)
				continue
			}
			src, err = s8bVec.EncodeAll(src)
			csize = 8 * len(src)
			tcomp = time.Since(start).Seconds()
			if err == nil {
				dst := make([]uint64, b.Len())
				start := time.Now()
				s8bVec.DecodeAll(dst, src)
				compress.ZzDeltaDecodeUint64AVX2(dst)
				tdecomp = time.Since(start).Seconds()
				convertUint64ToBlock(b2, dst)
			}
		case "s8b":
			src := convertBlockToUint64(b)
			dst := make([]uint64, b.Len())
			zz := false
			if b.IsUint() {
				start := time.Now()
				if compress.MaxUint64(src) >= 1<<60 {
					fmt.Printf("\nCannot s8b compress pack %v block %v\n", p.key, j)
					continue
				}
				src, err = s8bVec.EncodeAll(src)
				csize = 8 * len(src)
				tcomp = time.Since(start).Seconds()
				if err == nil {
					start := time.Now()
					s8bVec.DecodeAll(dst, src)
					tdecomp = time.Since(start).Seconds()
				}
			} else {
				start := time.Now()
				if compress.HasNegUint64(src) {
					if compress.ZzEncodeUint64(src) >= 1<<60 {
						fmt.Printf("\nCannot s8b compress pack %v block %v\n", p.key, j)
						continue
					}
					zz = true
				} else {
					if compress.MaxUint64(src) >= 1<<60 {
						fmt.Printf("\nCannot s8b compress pack %v block %v\n", p.key, j)
						continue
					}
				}
				src, err = s8bVec.EncodeAll(src)
				csize = 8 * len(src)
				tcomp = time.Since(start).Seconds()
				if err == nil {
					start := time.Now()
					s8bVec.DecodeAll(dst, src)
					if zz {
						compress.ZzDecodeUint64AVX2(dst)
					}
					tdecomp = time.Since(start).Seconds()
				}
			}
			convertUint64ToBlock(b2, dst)
		case "snappy":
			var buf []byte
			start := time.Now()
			buf, csize, err = compressSnappy(p.blocks[j])
			tcomp = time.Since(start).Seconds()
			if err == nil {
				start = time.Now()
				_, err = uncompressSnappy(b2, buf)
				tdecomp = time.Since(start).Seconds()
			}
		case "lz4":
			var buf []byte
			start := time.Now()
			buf, csize, err = compressLz4(p.blocks[j])
			tcomp = time.Since(start).Seconds()
			if err == nil {
				start = time.Now()
				_, err = uncompressLz4(b2, buf, cap(buf))
				tdecomp = time.Since(start).Seconds()
			}
		default:
			return nil, nil, nil, fmt.Errorf("unknown compression method %s", cmethod)
		}
		if err != nil {
			return nil, nil, nil, err
		}
		if check && !reflect.DeepEqual(b.RawSlice(), b2.RawSlice()) {
			return nil, nil, nil, fmt.Errorf("Compression/Decompression error pack %v block %v", p.key, j)
		}
		if csize < 0 {
			ct[j] = -1
			dt[j] = -1
		} else {
			ct[j] = float64(p.blocks[j].HeapSize()) / tcomp / 1000000
			dt[j] = float64(p.blocks[j].HeapSize()) / tdecomp / 1000000
		}
		cr[j] = float64(csize) / float64(p.blocks[j].HeapSize())
	}
	return cr, ct, dt, nil
}

func (t *Table) CompressPack(cmethod string, w io.Writer, i int, mode DumpMode) error {
	if i >= t.packidx.Len() || i < 0 {
		return ErrPackNotFound
	}
	tx, err := t.db.Tx(false)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	pkg, err := t.loadSharedPack(tx, t.packidx.packs[i].Key, false, nil)
	if err != nil {
		return err
	}

	cratios := make([][]float64, 1)
	ctimes := make([][]float64, 1)
	dtimes := make([][]float64, 1)
	cr, ct, dt, err := pkg.compress(cmethod)
	if err != nil {
		return err
	}
	cratios[0] = cr
	ctimes[0] = ct
	dtimes[0] = dt

	return DumpCompressResults(t.fields, cratios, ctimes, dtimes, w, mode, false)
}

func (t *Table) CompressIndexPack(cmethod string, w io.Writer, i, p int, mode DumpMode) error {
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
	pkg, err := t.indexes[i].loadSharedPack(tx, t.indexes[i].packidx.packs[p].Key, false)
	if err != nil {
		return err
	}

	cratios := make([][]float64, 1)
	ctimes := make([][]float64, 1)
	dtimes := make([][]float64, 1)
	cr, ct, dt, err := pkg.compressIdx(cmethod)
	if err != nil {
		return err
	}
	cratios[0] = cr
	ctimes[0] = ct
	dtimes[0] = dt
	fl := FieldList{{Name: "Hash", Type: "uint64"}, {Name: "PK", Type: "uint64"}}

	return DumpCompressResults(fl, cratios, ctimes, dtimes, w, mode, false)
}

func (t *Table) CompressIndexAll(cmethod string, i int, w io.Writer, mode DumpMode, verbose bool) error {
	tx, err := t.db.Tx(false)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	cratios := make([][]float64, t.indexes[i].packidx.Len())
	ctimes := make([][]float64, t.indexes[i].packidx.Len())
	dtimes := make([][]float64, t.indexes[i].packidx.Len())

	for p := 0; p < t.indexes[i].packidx.Len(); p++ {
		pkg, err := t.indexes[i].loadSharedPack(tx, t.indexes[i].packidx.packs[p].Key, false)
		if err != nil {
			return err
		}

		cr, ct, dt, err := pkg.compressIdx(cmethod)
		if err != nil {
			return err
		}
		cratios[p] = cr
		ctimes[p] = ct
		dtimes[p] = dt
	}
	fl := FieldList{{Name: "Hash", Type: "uint64"}, {Name: "PK", Type: "uint64"}}

	return DumpCompressResults(fl, cratios, ctimes, dtimes, w, mode, verbose)
}

func (t *Table) IndexCollisions(cmethod string, i int, w io.Writer, mode DumpMode, verbose bool) error {
	tx, err := t.db.Tx(false)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if cmethod[:4] != "hash" {
		return fmt.Errorf("unknown compression method %s", cmethod)
	}

	hashlen, err := strconv.Atoi(cmethod[4:])

	if err != nil || hashlen < 1 || hashlen > 64 {
		return fmt.Errorf("unknown compression method %s", cmethod)
	}

	var collisions uint64

	for p := 0; p < t.indexes[i].packidx.Len(); p++ {
		pkg, err := t.indexes[i].loadSharedPack(tx, t.indexes[i].packidx.packs[p].Key, false)
		if err != nil {
			return err
		}

		data := pkg.blocks[0].Uint64
		shift := 64 - hashlen
		for i := 1; i < len(data); i++ {
			if data[i] != data[i-1] && (data[i]>>shift) == (data[i-1]>>shift) {
				collisions++
			}
		}
	}

	fmt.Printf("Index contains %d additional collisions\n", collisions)

	return nil
}

func (t *Table) CompressAll(cmethod string, w io.Writer, mode DumpMode, verbose bool) error {
	tx, err := t.db.Tx(false)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	cratios := make([][]float64, t.packidx.Len())
	ctimes := make([][]float64, t.packidx.Len())
	dtimes := make([][]float64, t.packidx.Len())

	for i := 0; i < t.packidx.Len(); i++ {
		// fmt.Printf("Pack %d\n", i)
		pkg, err := t.loadSharedPack(tx, t.packidx.packs[i].Key, false, nil)
		if err != nil {
			return err
		}

		cr, ct, dt, err := pkg.compress(cmethod)
		if err != nil {
			return err
		}
		cratios[i] = cr
		ctimes[i] = ct
		dtimes[i] = dt
		fmt.Printf(".")
	}
	fmt.Printf("\nProcessed %d packs\n", t.packidx.Len())
	return DumpCompressResults(t.fields, cratios, ctimes, dtimes, w, mode, verbose)
}

func (t *Table) ShowCompression(cmethod string, w io.Writer, mode DumpMode, verbose bool) error {
	tx, err := t.db.Tx(false)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	cratios := make([][]float64, t.packidx.Len())
	ctype := make([][]int8, t.packidx.Len())

	var csize int
	for i := 0; i < t.packidx.Len(); i++ {
		// fmt.Printf("Pack %d\n", i)
		pkg, err := t.loadSharedPack(tx, t.packidx.packs[i].Key, false, nil)
		cr := make([]float64, pkg.nFields)
		ct := make([]int8, pkg.nFields)
		if err != nil {
			return err
		}
		for j := 0; j < pkg.nFields; j++ {
			b := pkg.blocks[j]
			if !b.IsInt() {
				cr[j] = -1
				ct[j] = -1
				continue
			}
			buf := bytes.NewBuffer(make([]byte, 0, b.MaxStoredSize()))

			b.SetCompression(block.NoCompression)
			csize, err = b.Encode(buf)
			cr[j] = float64(csize) / float64(b.HeapSize())
			ct[j] = int8(block.Compression(buf.Bytes()[1]) >> 4)
		}
		cratios[i] = cr
		ctype[i] = ct
		fmt.Printf(".")
	}
	fmt.Printf("\nProcessed %d packs\n", t.packidx.Len())
	return DumpInfos(t.fields, ctype, w, mode, verbose)
}

func DumpCompressResults(fl FieldList, cratios, ctimes, dtimes [][]float64, w io.Writer, mode DumpMode, verbose bool) error {
	out := "Compression ratios\n"
	if _, err := w.Write([]byte(out)); err != nil {
		return err
	}
	if err := DumpRatios(fl, cratios, w, mode, verbose); err != nil {
		return err
	}

	out = "\nCompression troughput [MB/s]\n"
	if _, err := w.Write([]byte(out)); err != nil {
		return err
	}
	if err := DumpTimes(fl, ctimes, w, mode, verbose); err != nil {
		return err
	}

	out = "\nUncompression troughput [MB/s]\n"
	if _, err := w.Write([]byte(out)); err != nil {
		return err
	}
	if err := DumpTimes(fl, dtimes, w, mode, verbose); err != nil {
		return err
	}

	return nil
}

func DumpRatios(fl FieldList, cratios [][]float64, w io.Writer, mode DumpMode, verbose bool) error {
	names := fl.Names()
	nFields := len(names)
	if len(fl.Aliases()) == nFields && len(fl.Aliases()[0]) > 0 {
		names = fl.Aliases()
	}

	names = append([]string{"Pack"}, names...)

	// estimate sizes from the first 500 values
	switch mode {
	case DumpModeDec, DumpModeHex:
		sz := make([]int, nFields+1)
		row := make([]string, nFields+1)
		for j := 0; j < nFields+1; j++ {
			sz[j] = len(names[j])
		}
		for j := 0; j < nFields; j++ {
			if len(fl[j].Type) > sz[j+1] {
				sz[j+1] = len(fl[j].Type)
			}
			if sz[j+1] < 6 {
				sz[j+1] = 6
			}
		}
		for j := 0; j < nFields+1; j++ {
			row[j] = fmt.Sprintf("%[2]*[1]s", names[j], -sz[j])
		}
		var out string
		out = "| " + strings.Join(row, " | ") + " |\n"
		if _, err := w.Write([]byte(out)); err != nil {
			return err
		}
		row[0] = fmt.Sprintf("%[2]*[1]s", "", -sz[0])
		for j := 0; j < nFields; j++ {
			row[j+1] = fmt.Sprintf("%[2]*[1]s", fl[j].Type, -sz[j+1])
		}
		out = "| " + strings.Join(row, " | ") + " |\n"
		if _, err := w.Write([]byte(out)); err != nil {
			return err
		}

		row[0] = fmt.Sprintf("%[2]*[1]s", "", -sz[0])
		for j := 0; j < nFields; j++ {
			row[j+1] = fmt.Sprintf("%[2]*[1]s", fl[j].Flags.Compression().String(), -sz[j+1])
		}
		out = "| " + strings.Join(row, " | ") + " |\n"
		if _, err := w.Write([]byte(out)); err != nil {
			return err
		}

		for j := 0; j < nFields+1; j++ {
			row[j] = strings.Repeat("-", sz[j])
		}
		out = "|-" + strings.Join(row, "-|-") + "-|\n"
		if _, err := w.Write([]byte(out)); err != nil {
			return err
		}
		avg := make([]float64, len(cratios[0]))
		for i := 0; i < len(cratios); i++ {
			row[0] = fmt.Sprintf("%[2]*[1]d", i, sz[0])
			for j := 0; j < len(cratios[0]); j++ {
				avg[j] += cratios[i][j]
				if cratios[i][j] < 0 {
					row[j+1] = fmt.Sprintf("%[2]*[1]s", "", sz[j+1])
				} else {
					row[j+1] = fmt.Sprintf("%[2]*.[1]f%%", 100*(1-cratios[i][j]), sz[j+1]-1)
				}
			}
			if verbose || len(cratios) == 1 {
				out = "| " + strings.Join(row, " | ") + " |\n"
				if _, err := w.Write([]byte(out)); err != nil {
					return err
				}
			}
		}
		if len(cratios) == 1 {
			return nil
		}
		if verbose {
			for j := 0; j < nFields+1; j++ {
				row[j] = strings.Repeat("-", sz[j])
			}
			out = "|-" + strings.Join(row, "-|-") + "-|\n"
			if _, err := w.Write([]byte(out)); err != nil {
				return err
			}
			for j := 0; j < nFields+1; j++ {
				row[j] = fmt.Sprintf("%[2]*[1]s", names[j], -sz[j])
			}
			out = "| " + strings.Join(row, " | ") + " |\n"
			if _, err := w.Write([]byte(out)); err != nil {
				return err
			}
			row[0] = fmt.Sprintf("%[2]*[1]s", "", -sz[0])
			for j := 0; j < nFields; j++ {
				row[j+1] = fmt.Sprintf("%[2]*[1]s", fl[j].Type, -sz[j+1])
			}
			out = "| " + strings.Join(row, " | ") + " |\n"
			if _, err := w.Write([]byte(out)); err != nil {
				return err
			}
			for j := 0; j < nFields+1; j++ {
				row[j] = strings.Repeat("-", sz[j])
			}
			out = "|-" + strings.Join(row, "-|-") + "-|\n"
			if _, err := w.Write([]byte(out)); err != nil {
				return err
			}
		}
		row[0] = fmt.Sprintf(" AVG")
		for j := 0; j < len(avg); j++ {
			if avg[j] < 0 {
				row[j+1] = fmt.Sprintf("%[2]*[1]s", "", sz[j+1])
			} else {
				row[j+1] = fmt.Sprintf("%[2]*.[1]f%%", 100*(1-avg[j]/float64(len(cratios))), sz[j+1]-1)
			}
		}
		out = "| " + strings.Join(row, " | ") + " |\n"
		if _, err := w.Write([]byte(out)); err != nil {
			return err
		}

		/*	case DumpModeCSV:
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
			}*/
	}
	return nil
}

func DumpInfos(fl FieldList, cinfos [][]int8, w io.Writer, mode DumpMode, verbose bool) error {
	names := fl.Names()
	nFields := len(names)
	if len(fl.Aliases()) == nFields && len(fl.Aliases()[0]) > 0 {
		names = fl.Aliases()
	}

	names = append([]string{"Pack"}, names...)

	// estimate sizes from the first 500 values
	switch mode {
	case DumpModeDec, DumpModeHex:
		sz := make([]int, nFields+1)
		row := make([]string, nFields+1)
		for j := 0; j < nFields+1; j++ {
			sz[j] = len(names[j])
		}
		for j := 0; j < nFields; j++ {
			if len(fl[j].Type) > sz[j+1] {
				sz[j+1] = len(fl[j].Type)
			}
			if sz[j+1] < 6 {
				sz[j+1] = 6
			}
		}
		for j := 0; j < nFields+1; j++ {
			row[j] = fmt.Sprintf("%[2]*[1]s", names[j], -sz[j])
		}
		var out string
		out = "| " + strings.Join(row, " | ") + " |\n"
		if _, err := w.Write([]byte(out)); err != nil {
			return err
		}
		row[0] = fmt.Sprintf("%[2]*[1]s", "", -sz[0])
		for j := 0; j < nFields; j++ {
			row[j+1] = fmt.Sprintf("%[2]*[1]s", fl[j].Type, -sz[j+1])
		}
		out = "| " + strings.Join(row, " | ") + " |\n"
		if _, err := w.Write([]byte(out)); err != nil {
			return err
		}

		for j := 0; j < nFields+1; j++ {
			row[j] = strings.Repeat("-", sz[j])
		}
		out = "|-" + strings.Join(row, "-|-") + "-|\n"
		if _, err := w.Write([]byte(out)); err != nil {
			return err
		}

		s1 := make([]int, len(cinfos[0]))
		s2 := make([]int, len(cinfos[0]))
		s3 := make([]int, len(cinfos[0]))
		for i := 0; i < len(cinfos); i++ {
			row[0] = fmt.Sprintf("%[2]*[1]d", i, sz[0])
			for j := 0; j < len(cinfos[0]); j++ {
				switch cinfos[i][j] {
				case 0:
					s1[j]++
				case 1:
					s2[j]++
				case 2:
					s3[j]++
				}
				if cinfos[i][j] < 0 {
					row[j+1] = fmt.Sprintf("%[2]*[1]s", "", sz[j+1])
				} else {
					row[j+1] = fmt.Sprintf("%[2]*.[1]d", cinfos[i][j], sz[j+1])
				}
			}
			if verbose || len(cinfos) == 1 {
				out = "| " + strings.Join(row, " | ") + " |\n"
				if _, err := w.Write([]byte(out)); err != nil {
					return err
				}
			}
		}
		if len(cinfos) == 1 {
			return nil
		}
		if verbose {
			for j := 0; j < nFields+1; j++ {
				row[j] = strings.Repeat("-", sz[j])
			}
			out = "|-" + strings.Join(row, "-|-") + "-|\n"
			if _, err := w.Write([]byte(out)); err != nil {
				return err
			}
			for j := 0; j < nFields+1; j++ {
				row[j] = fmt.Sprintf("%[2]*[1]s", names[j], -sz[j])
			}
			out = "| " + strings.Join(row, " | ") + " |\n"
			if _, err := w.Write([]byte(out)); err != nil {
				return err
			}
			row[0] = fmt.Sprintf("%[2]*[1]s", "", -sz[0])
			for j := 0; j < nFields; j++ {
				row[j+1] = fmt.Sprintf("%[2]*[1]s", fl[j].Type, -sz[j+1])
			}
			out = "| " + strings.Join(row, " | ") + " |\n"
			if _, err := w.Write([]byte(out)); err != nil {
				return err
			}
			for j := 0; j < nFields+1; j++ {
				row[j] = strings.Repeat("-", sz[j])
			}
			out = "|-" + strings.Join(row, "-|-") + "-|\n"
			if _, err := w.Write([]byte(out)); err != nil {
				return err
			}
		}
		row[0] = fmt.Sprintf("%[2]*[1]s", "No", -sz[0])
		for j := 0; j < len(s1); j++ {
			row[j+1] = fmt.Sprintf("%[2]*[1]d", s1[j], sz[j+1])
		}
		out = "| " + strings.Join(row, " | ") + " |\n"
		if _, err := w.Write([]byte(out)); err != nil {
			return err
		}
		row[0] = fmt.Sprintf("%[2]*[1]s", "S8B", -sz[0])
		for j := 0; j < len(s1); j++ {
			row[j+1] = fmt.Sprintf("%[2]*[1]d", s2[j], sz[j+1])
		}
		out = "| " + strings.Join(row, " | ") + " |\n"
		if _, err := w.Write([]byte(out)); err != nil {
			return err
		}
		row[0] = fmt.Sprintf("%[2]*[1]s", "RLE", -sz[0])
		for j := 0; j < len(s1); j++ {
			row[j+1] = fmt.Sprintf("%[2]*[1]d", s3[j], sz[j+1])
		}
		out = "| " + strings.Join(row, " | ") + " |\n"
		if _, err := w.Write([]byte(out)); err != nil {
			return err
		}

		/*	case DumpModeCSV:
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
			}*/
	}
	return nil
}

func DumpTimes(fl FieldList, ctimes [][]float64, w io.Writer, mode DumpMode, verbose bool) error {
	names := fl.Names()
	nFields := len(names)
	if len(fl.Aliases()) == nFields && len(fl.Aliases()[0]) > 0 {
		names = fl.Aliases()
	}

	names = append([]string{"Pack"}, names...)

	// estimate sizes from the first 500 values
	switch mode {
	case DumpModeDec, DumpModeHex:
		sz := make([]int, nFields+1)
		row := make([]string, nFields+1)
		for j := 0; j < nFields+1; j++ {
			sz[j] = len(names[j])
		}
		for j := 0; j < nFields; j++ {
			if len(fl[j].Type) > sz[j+1] {
				sz[j+1] = len(fl[j].Type)
			}
			if sz[j+1] < 5 {
				sz[j+1] = 5
			}
		}
		for j := 0; j < nFields+1; j++ {
			row[j] = fmt.Sprintf("%[2]*[1]s", names[j], -sz[j])
		}
		var out string
		out = "| " + strings.Join(row, " | ") + " |\n"
		if _, err := w.Write([]byte(out)); err != nil {
			return err
		}
		row[0] = fmt.Sprintf("%[2]*[1]s", "", -sz[0])
		for j := 0; j < nFields; j++ {
			row[j+1] = fmt.Sprintf("%[2]*[1]s", fl[j].Type, -sz[j+1])
		}
		out = "| " + strings.Join(row, " | ") + " |\n"
		if _, err := w.Write([]byte(out)); err != nil {
			return err
		}
		for j := 0; j < nFields+1; j++ {
			row[j] = strings.Repeat("-", sz[j])
		}
		out = "|-" + strings.Join(row, "-|-") + "-|\n"
		if _, err := w.Write([]byte(out)); err != nil {
			return err
		}
		avg := make([]float64, len(ctimes[0]))
		for i := 0; i < len(ctimes); i++ {
			row[0] = fmt.Sprintf("%[2]*[1]d", i, sz[0])
			for j := 0; j < len(ctimes[0]); j++ {
				avg[j] += ctimes[i][j]
				if ctimes[i][j] < 0 {
					row[j+1] = fmt.Sprintf("%[2]*[1]s", "", sz[j+1])
				} else {
					row[j+1] = fmt.Sprintf("%[2]*.[1]f", ctimes[i][j], sz[j+1])
				}
			}
			if verbose || len(ctimes) == 1 {
				out = "| " + strings.Join(row, " | ") + " |\n"
				if _, err := w.Write([]byte(out)); err != nil {
					return err
				}
			}
		}
		if len(ctimes) == 1 {
			return nil
		}
		if verbose {
			for j := 0; j < nFields+1; j++ {
				row[j] = strings.Repeat("-", sz[j])
			}
			out = "|-" + strings.Join(row, "-|-") + "-|\n"
			if _, err := w.Write([]byte(out)); err != nil {
				return err
			}
			for j := 0; j < nFields+1; j++ {
				row[j] = fmt.Sprintf("%[2]*[1]s", names[j], -sz[j])
			}
			out = "| " + strings.Join(row, " | ") + " |\n"
			if _, err := w.Write([]byte(out)); err != nil {
				return err
			}
			row[0] = fmt.Sprintf("%[2]*[1]s", "", -sz[0])
			for j := 0; j < nFields; j++ {
				row[j+1] = fmt.Sprintf("%[2]*[1]s", fl[j].Type, -sz[j+1])
			}
			out = "| " + strings.Join(row, " | ") + " |\n"
			if _, err := w.Write([]byte(out)); err != nil {
				return err
			}
			for j := 0; j < nFields+1; j++ {
				row[j] = strings.Repeat("-", sz[j])
			}
			out = "|-" + strings.Join(row, "-|-") + "-|\n"
			if _, err := w.Write([]byte(out)); err != nil {
				return err
			}
		}
		row[0] = fmt.Sprintf(" AVG")
		for j := 0; j < len(avg); j++ {
			if avg[j] < 0 {
				row[j+1] = fmt.Sprintf("%[2]*[1]s", "", sz[j+1])
			} else {
				row[j+1] = fmt.Sprintf("%[2]*.[1]f", avg[j]/float64(len(ctimes)), sz[j+1])
			}
		}
		out = "| " + strings.Join(row, " | ") + " |\n"
		if _, err := w.Write([]byte(out)); err != nil {
			return err
		}

		/*	case DumpModeCSV:
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
			}*/
	}
	return nil
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
			} else if a, b := b.Bytes.Len(), p.nValues; a != b {
				return fmt.Errorf("pack: mismatch %s block slice len %d/%d", f.Type, a, b)
			}

		case FieldTypeString:
			if b.Bytes == nil {
				return fmt.Errorf("pack: nil %s block", f.Type)
			} else if a, b := b.Bytes.Len(), p.nValues; a != b {
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

func (t *Table) ValidatePackIndex(w io.Writer) error {
	if errs := t.packidx.Validate(); errs != nil {
		for _, v := range errs {
			w.Write([]byte(v.Error() + "\n"))
		}
	}
	return nil
}

func (t *Table) ValidateIndexPackIndex(w io.Writer) error {
	for _, idx := range t.indexes {
		if errs := idx.packidx.Validate(); errs != nil {
			for _, v := range errs {
				w.Write([]byte(fmt.Sprintf("%s: %v\n", idx.Name, v.Error())))
			}
		}
	}
	return nil
}
