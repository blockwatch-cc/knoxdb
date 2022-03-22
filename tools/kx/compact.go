// Copyright (c) 2018-2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package main

import (
	"context"

	"blockwatch.cc/knoxdb/pack"
)

// compact compacts a table and its indexes to remove pack fragmentation
func compact(ctx context.Context, data interface{}) error {
	table := data.(*pack.Table)
	if err := table.Flush(ctx); err != nil {
		return err
	}
	if err := table.Compact(ctx); err != nil {
		return err
	}
	return nil
}

// gc runs garbace collection on a bolt kv store by creating a new boltdb file
// and copying all nested buckets and key/value pairs from the original. Replaces
// the original file on success. this operation needs up to twice the amount of
// disk space.
func gc(ctx context.Context, data interface{}) error {
	db := data.(*pack.DB)
	return db.GC(ctx, 1.0)
}

// flush flushes journal data to packs
func flush(ctx context.Context, data interface{}) error {
	table := data.(*pack.Table)
	return table.Flush(ctx)
}
