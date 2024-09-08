// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package pack

import (
	"fmt"
	"io"
	"strings"

	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/csv"
	"blockwatch.cc/knoxdb/pkg/util"
)

type DumpMode = types.DumpMode

const (
	DumpModeDec = types.DumpModeDec
	DumpModeHex = types.DumpModeHex
	DumpModeCSV = types.DumpModeCSV
)

func (p *Package) DumpType(w io.Writer) error {
	kind := "data"
	switch {
	case p.IsJournal():
		kind = "journal"
	case p.IsTomb():
		kind = "tombstone"
	}
	fmt.Fprintf(w, "Package --------------------------------------------------------------- \n")
	fmt.Fprintf(w, "Key:        %08x (%s)\n", p.Key(), kind)
	fmt.Fprintf(w, "Schema:     %08x (%s)\n", p.Schema().Hash(), p.Schema().Name())
	fmt.Fprintf(w, "Version:    %d\n", p.Schema().Version())
	fmt.Fprintf(w, "Cols:       %d\n", p.Cols())
	fmt.Fprintf(w, "Rows:       %d\n", p.Len())
	fmt.Fprintf(w, "Cap:        %d\n", p.Cap())
	fmt.Fprintf(w, "Pki:        %d\n", p.PkIdx())
	// fmt.Fprintf(w, "Size:       %s (%d) stored, %s (%d) heap\n",
	// 	util.ByteSize(p.DiskSize()), p.DiskSize(),
	// 	util.ByteSize(p.HeapSize()), p.HeapSize(),
	// )
	// meta := p.NewMetadata()
	for i, v := range p.blocks {
		field := p.schema.Fields()[i]
		// min=%s max=%s card=%d dirty=%t
		fmt.Fprintf(w, "Block #%-02d:   %s typ=%s comp=%d scale=%d fixed=%d len=%d cap=%d flags=%s\n",
			field.Id(),
			field.Name(),
			field.Type(),
			field.Compress(),
			field.Scale(),
			field.Fixed(),
			v.Len(),
			v.Cap(),
			field.Flags(),
			// util.ToString(meta.Blocks[i].MinValue),
			// util.ToString(meta.Blocks[i].MaxValue),
			// util.ToString(meta.Blocks[i].Cardinality),
			// util.PrettyInt(meta.Blocks[i].StoredSize),
		)
	}
	return nil
}

func (p *Package) DumpData(w io.Writer, mode DumpMode) error {
	fields := p.schema.Fields()
	names := p.schema.FieldNames()
	nFields := len(names)

	// estimate sizes from the first 500 values
	switch mode {
	case DumpModeDec, DumpModeHex:
		sz := make([]int, len(names))
		row := make([]string, nFields)
		for j := 0; j < nFields; j++ {
			sz[j] = len(names[j])
		}
		for i, l := 0, util.Min(500, p.Len()); i < l; i++ {
			for j := 0; j < nFields; j++ {
				var str string
				if p.blocks[j] == nil {
					str = "[strip]"
				} else {
					val := p.ReadValue(j, i, fields[j].Type(), fields[j].Scale())
					str = util.ToString(val)
				}
				sz[j] = util.Max(sz[j], len(str))
			}
		}
		for j := 0; j < nFields; j++ {
			row[j] = fmt.Sprintf("%[2]*[1]s", names[j], -sz[j])
		}
		var out string
		out = "| " + strings.Join(row, " | ") + " |\n"
		if _, err := w.Write([]byte(out)); err != nil {
			return err
		}
		for j := 0; j < nFields; j++ {
			row[j] = strings.Repeat("-", sz[j])
		}
		out = "|-" + strings.Join(row, "-|-") + "-|\n"
		if _, err := w.Write([]byte(out)); err != nil {
			return err
		}
		for i := 0; i < p.Len(); i++ {
			for j := 0; j < nFields; j++ {
				var str string
				if p.blocks[j] == nil {
					str = "[strip]"
				} else {
					val := p.ReadValue(j, i, fields[j].Type(), fields[j].Scale())
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
		var row []any
		for i := 0; i < p.Len(); i++ {
			row = p.ReadRow(i, row)
			if err := enc.EncodeRecord(row); err != nil {
				return err
			}
		}
	}
	return nil
}
