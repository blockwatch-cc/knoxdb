// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package stats

import (
	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/schema"
)

// Construct a union schema over pack stats and table min/max.
// The schema has no primary key. It starts with four pack
// metadata columns (see Record) and continues with pairs
// of min/max columns in order of table schema s. This guarantees
// that when the table is extended we always add new statistics columns
// at the end. Stats column positions can be calculated from the original
// data schema column position as follows:
//
// I_min_col_x = I_col_x * 2 + STATS_DATA_COL_OFFSET
// I_max_col_x = I_col_x * 2 + STATS_DATA_COL_OFFSET +1
//
// The statistics schema ignores (i.e. removes) all flags and enums
// from the original table schema except FieldFlagDeleted which may be
// used to skip/remove statistics when columns are marked as deleted.
func MakeSchema(s *schema.Schema) *schema.Schema {
	// add pack stats fields
	fields := RecordSchema.Fields

	// TODO:
	// - convert string/byte to [n]byte type (n = min(f.fixed||8, 8))
	// - exclude text/blob fields or limit to first 8 bytes as well

	// add min/max fields interleaved
	for _, src := range s.Fields {
		// generate clean field from source
		minField := schema.FieldOf(src.Type,
			schema.WithName("min_"+src.Name),
			// add scale or fixed array len
			schema.WithScale(src.Scale),
			// only keep deleted flag
			schema.WithFlags(src.Flags&types.F_DELETED),
			// keep filter (in case its bloom)
			schema.WithFilter(src.Filter),
			// keep enum for validation
			schema.WithEnum(src.Enum),
		)
		maxField := minField.Clone()
		maxField.Name = "max_" + src.Name
		fields = append(fields, minField, maxField)
	}

	return schema.SchemaOf(fields, schema.Name(s.Name), schema.Version(s.Version))
}

func minColIndex(i int) int {
	return 2*i + STATS_DATA_COL_OFFSET
}

func maxColIndex(i int) int {
	return 2*i + STATS_DATA_COL_OFFSET + 1
}

func leftChildIndex(i int) int {
	return 2*i + 1
}

func rightChildIndex(i int) int {
	return 2*i + 2
}

func parentIndex(i int) int {
	if i == 0 {
		return -1
	}
	return (i - 1) / 2
}
