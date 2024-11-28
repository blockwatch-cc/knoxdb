// Copyright (c) 2018-2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package main

import (
	"context"
	"fmt"

	"blockwatch.cc/knoxdb/pack"
)

// compact compacts a table and its indexes to remove pack fragmentation
func compact(ctx context.Context, data interface{}) (err error) {
	switch t := data.(type) {
	case *pack.PackTable:
		err = t.Flush(ctx)
		if err == nil {
			err = t.Compact(ctx)
		}
	case *pack.KeyValueTable:
		err = t.Sync(ctx)
		if err == nil {
			err = t.Compact(ctx)
		}
	default:
		err = fmt.Errorf("compaction for %T tables not implemented", data)
	}
	return
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
	table := data.(*pack.PackTable)
	return table.Flush(ctx)
}
