// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package stats

const (
	STATS_ROW_KEY = iota
	STATS_ROW_SCHEMA
	STATS_ROW_NVALS
	STATS_ROW_SIZE
)

// field usage in meta wire encoding
// Key: represents the min key across this subtree
// SchemaId: reused to count total number of data packs (i.e. stats rows)
// NValues: sum nvalues across all data packs, i.e. total table size
// DiskSize: sum disk sizes of all data packs, i.e. total table storage size
// data columns: min & max represent min/max over the subtree (inode) or stats pack (dnode)
type PackStats struct {
	Key      uint32 `knox:"key"`       // data pack key
	SchemaId uint64 `knox:"schema_id"` // data pack schema identifier
	NValues  int64  `knox:"n_values"`  // rows in data pack
	DiskSize int64  `knox:"disk_size"` // total data pack size on disk
}
