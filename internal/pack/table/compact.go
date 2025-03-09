// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package table

import (
	"context"
)

// Compact rewrites table data packs to rebalance vector sizes and elliminate
// fragmentation after deleting or updating records. In addition compacted data
// is written to a new backend file to reclaim unused/freed space from
// b+tree and free-lists. After completion the new file atomically replaces the
// previous database file.
//
// - rewrites table data, re-compacting vectors
// - rewrites per pack metadata statistics
// - copies pending/stored journal segments
// - copies table state
func (t *Table) Compact(ctx context.Context) error {
	return nil

	// TODO
	// exclusive table lock to prevent concurrent write or background merge
	// create new db file
	// init TableReader to read all data from current table file
	// init TableWriter with new file as target and fresh stats index
	// clone all table data
	// clone journal segments
	// clone table state
	// clear caches
	// install new db file, atomic rename
	// close old db backend, replace by new backend
}
