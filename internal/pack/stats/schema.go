// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package stats

import (
	"math/bits"

	"blockwatch.cc/knoxdb/internal/engine"
	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/schema"
	"golang.org/x/exp/constraints"
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
	statsSchema := schema.NewSchema().
		WithName(s.Name() + string(engine.StatsKeySuffix)).
		WithVersion(s.Version())

	// add pack stats fields
	for _, f := range schema.MustSchemaOf(&Record{}).Fields() {
		statsSchema.WithField(f)
	}

	// TODO:
	// - convert string/byte to [n]byte type (n = min(f.fixed||8, 8))
	// - use schema builder

	// add min/max fields interleaved
	for _, src := range s.Fields() {
		// generate clean field from source
		f := schema.NewField(src.Type()).
			WithName("min_" + src.Name()).
			WithScale(src.Scale()).
			WithFixed(src.Fixed()).
			WithFlags(src.Flags() & types.FieldFlagDeleted). // only keep deleted flag
			WithIndex(src.Index())                           // keep index (in case its bloom)
		statsSchema.WithField(f)
		statsSchema.WithField(f.WithName("max_" + src.Name()))
	}

	return statsSchema.Finalize()
}

func minColIndex[T constraints.Signed | constraints.Unsigned](i T) T {
	return 2*i + STATS_DATA_COL_OFFSET
}

func maxColIndex[T constraints.Signed | constraints.Unsigned](i T) T {
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

func log2(i int) int {
	return bits.UintSize - bits.LeadingZeros(uint(i)) - 1
}

func log2ceil(i int) int {
	v := log2(i)
	if i&(i-1) > 0 {
		v++
	}
	return v
}
