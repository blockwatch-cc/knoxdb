// Copyright (c) 2013 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package pack

import (
	"fmt"
	"io"
)

type DumpMode byte

const (
	DumpModeDec DumpMode = iota
	DumpModeHex
	DumpModeCSV
)

// func (p *Package) Validate() error {
// 	if a, b := p.schema.NumFields(), len(p.blocks); a != b {
// 		return fmt.Errorf("knox: mismatch block len %d/%d", a, b)
// 	}

// 	for i, f := range p.schema.Fields() {
// 		b := p.blocks[i]
// 		if a, b := b.Type(), f.Type.BlockType(); a != b {
// 			return fmt.Errorf("knox: mismatch block type %s/%s", a, b)
// 		}
// 		if a, b := p.Len(), b.Len(); a != b {
// 			return fmt.Errorf("knox: mismatch block len %d/%d", a, b)
// 		}
// 		switch f.Type {
// 		case FieldTypeBytes:
// 			if b.Bytes() == nil {
// 				return fmt.Errorf("knox: nil %s block", f.Type)
// 			} else if a, b := b.Bytes().Len(), p.Len(); a != b {
// 				return fmt.Errorf("knox: mismatch %s block slice len %d/%d", f.Type, a, b)
// 			}

// 		case FieldTypeString:
// 			if b.Bytes() == nil {
// 				return fmt.Errorf("knox: nil %s block", f.Type)
// 			} else if a, b := b.Bytes().Len(), p.Len(); a != b {
// 				return fmt.Errorf("knox: mismatch %s block slice len %d/%d", f.Type, a, b)
// 			}

// 		case FieldTypeBoolean:
// 			if b.Bool() == nil {
// 				return fmt.Errorf("knox: nil %s block", f.Type)
// 			} else if a, b := b.Bool().Len(), p.Len(); a != b {
// 				return fmt.Errorf("knox: mismatch %s block slice len %d/%d", f.Type, a, b)
// 			}

// 		case FieldTypeFloat64:
// 			if b.Float64() == nil {
// 				return fmt.Errorf("knox: nil %s block", f.Type)
// 			} else if a, b := len(b.Float64()), p.Len(); a != b {
// 				return fmt.Errorf("knox: mismatch %s block slice len %d/%d", f.Type, a, b)
// 			}

// 		case FieldTypeFloat32:
// 			if b.Float32() == nil {
// 				return fmt.Errorf("knox: nil %s block", f.Type)
// 			} else if a, b := len(b.Float32()), p.Len(); a != b {
// 				return fmt.Errorf("knox: mismatch %s block slice len %d/%d", f.Type, a, b)
// 			}

// 		case FieldTypeInt256, FieldTypeDecimal256:
// 			if b.Int256().IsNil() {
// 				return fmt.Errorf("knox: nil %s block", f.Type)
// 			} else if a, b := b.Int256().Len(), p.Len(); a != b {
// 				return fmt.Errorf("knox: mismatch %s block slice len %d/%d", f.Type, a, b)
// 			}

// 		case FieldTypeInt128, FieldTypeDecimal128:
// 			if b.Int128().IsNil() {
// 				return fmt.Errorf("knox: nil %s block", f.Type)
// 			} else if a, b := b.Int128().Len(), p.Len(); a != b {
// 				return fmt.Errorf("knox: mismatch %s block slice len %d/%d", f.Type, a, b)
// 			}

// 		case FieldTypeInt64, FieldTypeDatetime, FieldTypeDecimal64:
// 			if b.Int64() == nil {
// 				return fmt.Errorf("knox: nil %s block", f.Type)
// 			} else if a, b := len(b.Int64()), p.Len(); a != b {
// 				return fmt.Errorf("knox: mismatch %s block slice len %d/%d", f.Type, a, b)
// 			}

// 		case FieldTypeInt32, FieldTypeDecimal32:
// 			if b.Int32() == nil {
// 				return fmt.Errorf("knox: nil %s block", f.Type)
// 			} else if a, b := len(b.Int32()), p.Len(); a != b {
// 				return fmt.Errorf("knox: mismatch %s block slice len %d/%d", f.Type, a, b)
// 			}

// 		case FieldTypeInt16:
// 			if b.Int16() == nil {
// 				return fmt.Errorf("knox: nil %s block", f.Type)
// 			} else if a, b := len(b.Int16()), p.Len(); a != b {
// 				return fmt.Errorf("knox: mismatch %s block slice len %d/%d", f.Type, a, b)
// 			}

// 		case FieldTypeInt8:
// 			if b.Int8() == nil {
// 				return fmt.Errorf("knox: nil %s block", f.Type)
// 			} else if a, b := len(b.Int8()), p.Len(); a != b {
// 				return fmt.Errorf("knox: mismatch %s block slice len %d/%d", f.Type, a, b)
// 			}

// 		case FieldTypeUint64:
// 			if b.Uint64() == nil {
// 				return fmt.Errorf("knox: nil %s block", f.Type)
// 			} else if a, b := len(b.Uint64()), p.Len(); a != b {
// 				return fmt.Errorf("knox: mismatch %s block slice len %d/%d", f.Type, a, b)
// 			}

// 		case FieldTypeUint32:
// 			if b.Uint32() == nil {
// 				return fmt.Errorf("knox: nil %s block", f.Type)
// 			} else if a, b := len(b.Uint32()), p.Len(); a != b {
// 				return fmt.Errorf("knox: mismatch %s block slice len %d/%d", f.Type, a, b)
// 			}
// 		case FieldTypeUint16:
// 			if b.Uint16() == nil {
// 				return fmt.Errorf("knox: nil %s block", f.Type)
// 			} else if a, b := len(b.Uint16()), p.Len(); a != b {
// 				return fmt.Errorf("knox: mismatch %s block slice len %d/%d", f.Type, a, b)
// 			}

// 		case FieldTypeUint8:
// 			if b.Uint8() == nil {
// 				return fmt.Errorf("knox: nil %s block", f.Type)
// 			} else if a, b := len(b.Uint8()), p.Len(); a != b {
// 				return fmt.Errorf("knox: mismatch %s block slice len %d/%d", f.Type, a, b)
// 			}

// 		}
// 	}
// 	return nil
// }

func (p *Package) DumpType(w io.Writer) error {
	kind := "data"
	switch {
	case p.IsJournal():
		kind = "journal"
	case p.IsTomb():
		kind = "tombstone"
	}
	fmt.Fprintf(w, "Package ------------------------------------ \n")
	fmt.Fprintf(w, "Key:        %08x (%s)\n", p.KeyU32(), kind)
	fmt.Fprintf(w, "Schema:     %08x\n", p.Schema().Hash())
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
		fmt.Fprintf(w, "Block %-02d:   %s typ=%s comp=%d scale=%d fixed=%d len=%d cap=%d\n",
			i,
			field.Name(),
			field.Type(),
			field.Compress(),
			field.Scale(),
			field.Fixed(),
			v.Len(),
			v.Cap(),
			// util.ToString(meta.Blocks[i].MinValue),
			// util.ToString(meta.Blocks[i].MaxValue),
			// util.ToString(meta.Blocks[i].Cardinality),
			// util.PrettyInt(meta.Blocks[i].StoredSize),
			// v.IsDirty(),
		)
	}
	return nil
}

// func (p *Package) DumpBlocks(w io.Writer, mode DumpMode, lineNo int) (int, error) {
// 	var key string
// 	switch true {
// 	case p.IsJournal():
// 		key = "journal"
// 	case p.IsTomb():
// 		key = "tombstone"
// 	default:
// 		key = util.ToString(p.Key())
// 	}
// 	meta := p.NewMetadata()
// 	switch mode {
// 	case DumpModeDec, DumpModeHex:
// 		for i, v := range p.blocks {
// 			bi := meta.Blocks[i]
// 			field := p.schema.Field(i)
// 			// reconstruct cardinality when missing
// 			// if bi.Cardinality == 0 && v.Len() > 0 {
// 			// 	bi.Cardinality = p.fields[i].Type.EstimateCardinality(v, 15)
// 			// }
// 			_, err := fmt.Fprintf(w, "%-5d %-10s %-7d %-10s %-7d %-5d %-33s %-33s %-4d %-6s %-10s %-10s %7s\n",
// 				lineNo,
// 				key,      // pack key
// 				i,        // block id
// 				v.Type(), // block type
// 				v.Len(),  // block values
// 				bi.Cardinality,
// 				util.LimitStringEllipsis(util.ToString(bi.MinValue), 33), // min val in block
// 				util.LimitStringEllipsis(util.ToString(bi.MaxValue), 33), // max val in block
// 				field.Scale,
// 				field.Compress,
// 				util.PrettyInt(v.CompressedSize()), // compressed block size
// 				util.PrettyInt(v.HeapSize()),       // in-memory block size
// 				util.PrettyFloat64N(100-float64(v.CompressedSize())/float64(v.HeapSize())*100, 2)+"%",
// 			)
// 			lineNo++
// 			if err != nil {
// 				return lineNo, err
// 			}
// 		}
// 	case DumpModeCSV:
// 		enc, ok := w.(*csv.Encoder)
// 		if !ok {
// 			enc = csv.NewEncoder(w)
// 		}
// 		cols := make([]interface{}, 13)
// 		for i, v := range p.blocks {
// 			fi := ""
// 			if p.tinfo != nil {
// 				fi = p.tinfo.fields[i].String()
// 			}
// 			field := p.fields[i]
// 			bi := info.Blocks[i]
// 			cols[0] = key
// 			cols[1] = i
// 			cols[2] = field.Name
// 			cols[3] = v.Type().String()
// 			cols[4] = v.Len()
// 			cols[5] = bi.MinValue
// 			cols[6] = bi.MaxValue
// 			cols[7] = bi.Scale
// 			cols[8] = v.Compression().String()
// 			cols[9] = v.CompressedSize()
// 			cols[10] = v.HeapSize()
// 			cols[11] = v.MaxStoredSize()
// 			cols[12] = fi
// 			if !enc.HeaderWritten() {
// 				if err := enc.EncodeHeader([]string{
// 					"Pack",
// 					"Block",
// 					"Name",
// 					"Type",
// 					"Columns",
// 					"Min",
// 					"Max",
// 					"Flags",
// 					"Compression",
// 					"Compressed",
// 					"Heap",
// 					"Max",
// 					"GoType",
// 				}, nil); err != nil {
// 					return lineNo, err
// 				}
// 			}
// 			if err := enc.EncodeRecord(cols); err != nil {
// 				return lineNo, err
// 			}
// 			lineNo++
// 		}
// 	}
// 	return lineNo, nil
// }

// func (p *Package) DumpData(w io.Writer, mode DumpMode, aliases []string) error {
// 	names := p.fields.Names()
// 	if len(aliases) == p.nFields && len(aliases[0]) > 0 {
// 		names = aliases
// 	}

// 	// estimate sizes from the first 500 values
// 	switch mode {
// 	case DumpModeDec, DumpModeHex:
// 		sz := make([]int, p.nFields)
// 		row := make([]string, p.nFields)
// 		for j := 0; j < p.nFields; j++ {
// 			sz[j] = len(names[j])
// 		}
// 		for i, l := 0, util.Min(500, p.nValues); i < l; i++ {
// 			for j := 0; j < p.nFields; j++ {
// 				var str string
// 				if p.blocks[j].IsIgnore() {
// 					str = "[strip]"
// 				} else {
// 					val, _ := p.FieldAt(j, i)
// 					str = util.ToString(val)
// 				}
// 				sz[j] = util.Max(sz[j], len(str))
// 			}
// 		}
// 		for j := 0; j < p.nFields; j++ {
// 			row[j] = fmt.Sprintf("%[2]*[1]s", names[j], -sz[j])
// 		}
// 		var out string
// 		out = "| " + strings.Join(row, " | ") + " |\n"
// 		if _, err := w.Write([]byte(out)); err != nil {
// 			return err
// 		}
// 		for j := 0; j < p.nFields; j++ {
// 			row[j] = strings.Repeat("-", sz[j])
// 		}
// 		out = "|-" + strings.Join(row, "-|-") + "-|\n"
// 		if _, err := w.Write([]byte(out)); err != nil {
// 			return err
// 		}
// 		for i := 0; i < p.nValues; i++ {
// 			for j := 0; j < p.nFields; j++ {
// 				var str string
// 				if p.blocks[j].IsIgnore() {
// 					str = "[strip]"
// 				} else {
// 					val, _ := p.FieldAt(j, i)
// 					str = util.ToString(val)
// 				}
// 				row[j] = fmt.Sprintf("%[2]*[1]s", str, -sz[j])
// 			}
// 			out = "| " + strings.Join(row, " | ") + " |\n"
// 			if _, err := w.Write([]byte(out)); err != nil {
// 				return err
// 			}
// 		}

// 	case DumpModeCSV:
// 		enc, ok := w.(*csv.Encoder)
// 		if !ok {
// 			enc = csv.NewEncoder(w)
// 		}
// 		if !enc.HeaderWritten() {
// 			if err := enc.EncodeHeader(names, nil); err != nil {
// 				return err
// 			}
// 		}
// 		// csv encoder supports []interface{} records
// 		for i := 0; i < p.nValues; i++ {
// 			row, _ := p.RowAt(i)
// 			if err := enc.EncodeRecord(row); err != nil {
// 				return err
// 			}
// 		}
// 	}
// 	return nil
// }
