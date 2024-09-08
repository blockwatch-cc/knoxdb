// Copyright (c) 2023 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package metadata

import (
	"fmt"
	"io"
	"strconv"
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

func (l MetadataIndex) Validate() []error {
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

type CSVHeader struct {
	Key   string `csv:"Key"`
	Cols  int    `csv:"Cols"`
	Rows  int    `csv:"Rows"`
	MinPk uint64 `csv:"MinPk"`
	MaxPk uint64 `csv:"MaxPk"`
	Size  int    `csv:"Size"`
}

func (m PackMetadata) Dump(w io.Writer, mode DumpMode, nfields int) error {
	pk := m.Blocks[0]
	min, max := pk.MinValue.(uint64), pk.MaxValue.(uint64)
	switch mode {
	case DumpModeDec:
		_, err := fmt.Fprintf(w, "%-10x %-7d %-7d %-21d %-21d %-10s\n",
			m.Key,
			len(m.Blocks),
			m.NValues,
			min,
			max,
			util.ByteSize(m.StoredSize),
		)
		return err
	case DumpModeHex:
		_, err := fmt.Fprintf(w, "%-10x %-7d %-7d %-21x %-21x %-10s\n",
			m.Key,
			len(m.Blocks),
			m.NValues,
			min,
			max,
			util.ByteSize(m.StoredSize),
		)
		return err
	case DumpModeCSV:
		enc, ok := w.(*csv.Encoder)
		if !ok {
			enc = csv.NewEncoder(w)
		}
		ch := CSVHeader{
			Key:   fmt.Sprintf("%08x", m.Key),
			Cols:  len(m.Blocks),
			Rows:  m.NValues,
			MinPk: min,
			MaxPk: max,
			Size:  m.StoredSize,
		}
		return enc.EncodeRecord(ch)
	}
	return nil
}

func (m PackMetadata) DumpDetail(w io.Writer) error {
	fmt.Fprintf(w, "Pack Key   %08x %s\n", m.Key, strings.Repeat("-", 100))
	fmt.Fprintf(w, "Values     %s\n", util.PrettyInt(m.NValues))
	fmt.Fprintf(w, "Pack Size  %s\n", util.ByteSize(m.StoredSize))
	fmt.Fprintf(w, "Meta Size  %s\n", util.ByteSize(m.HeapSize()))
	fmt.Fprintf(w, "Blocks     %d\n", len(m.Blocks))
	fmt.Fprintf(w, "%-3s %-10s %-7s %-33s %-33s %-10s %-10s\n",
		"#", "Type", "Card", "Min", "Max", "Bloom", "Bits")
	for id, binfo := range m.Blocks {
		bloomSz, bitSz := "--", "--"
		if binfo.Bloom != nil {
			bloomSz = strconv.Itoa(int(binfo.Bloom.Len() / 8))
		}
		if binfo.Bits != nil {
			bitSz = strconv.Itoa(len(binfo.Bits.ToBuffer()))
		}
		fmt.Fprintf(w, "%-3d %-10s %-7d %-33s %-33s %-10s %-10s\n",
			id+1,
			binfo.Type,
			binfo.Cardinality,
			util.LimitStringEllipsis(util.ToString(binfo.MinValue), 33),
			util.LimitStringEllipsis(util.ToString(binfo.MaxValue), 33),
			bloomSz,
			bitSz,
		)
	}
	return nil
}

// TODO: mix of metadata and storage values
// func (m PackMetadata) DumpBlocks(w io.Writer, mode DumpMode, lineNo int, ) (int, error) {
// 	switch mode {
// 	case DumpModeDec, DumpModeHex:
// 		for i, bi := range m.Blocks {
// 			field, _ := p.schema.FieldById(i)
// 			// reconstruct cardinality when missing
// 			// if bi.Cardinality == 0 && v.Len() > 0 {
// 			// 	bi.Cardinality = p.fields[i].Type.EstimateCardinality(v, 15)
// 			// }
// 			_, err := fmt.Fprintf(w, "%-5d %08x %-7d %-10s %-7d %-5d %-33s %-33s %-4d %-6s %-10s %-10s %7s\n",
// 				lineNo,
// 				p.Key,      // pack key
// 				i,        // block id
// 				bi.Type, // block type
// 				m.NValues,  // block values
// 				bi.Cardinality,
// 				util.LimitStringEllipsis(util.ToString(bi.MinValue), 33), // min val in block
// 				util.LimitStringEllipsis(util.ToString(bi.MaxValue), 33), // max val in block
// 				// field.Scale,
// 				// field.Compress,
// 				// util.PrettyInt(v.CompressedSize()), // compressed block size
// 				// util.PrettyInt(v.HeapSize()),       // in-memory block size
// 				// util.PrettyFloat64N(100-float64(v.CompressedSize())/float64(v.HeapSize())*100, 2)+"%",
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
