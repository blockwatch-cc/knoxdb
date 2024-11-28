// Copyright (c) 2018-2022 Blockwatch Data Inc.
// Author: alex@blockwatch.cc
//go:build ignore
// +build ignore

package main

import (
	"context"
	"time"

	"github.com/echa/log"
)

// reindex drops and re-creates all indexes defined for a given table.
func rebuildStatistics(args Args) error {
	start := time.Now()

	// open database file
	db, err := openDatabase(args)
	if err != nil {
		return err
	}
	defer db.Close()
	log.Infof("Using database %s", db.Path())

	// check table
	table, err := db.OpenTable(args.table)
	if err != nil {
		return err
	}

	if !noflush {
		// make sure source table journals are flushed
		if err := table.Engine().(*pack.Table).Flush(context.Background()); err != nil {
			return err
		}
	}
	stats := table.Stats()
	table.Close()

	log.Infof("Rebuilding metadata for %d rows / %d packs with statistics size %d bytes",
		stats.TupleCount, stats.PacksCount, stats.MetaSize)

	// // Delete table metadata bucket
	// log.Info("Dropping table statistics")
	// err = db.Update(func(dbTx *pack.Tx) error {
	// 	meta := dbTx.Bucket([]byte(args.table + "_meta"))
	// 	if meta == nil {
	// 		return fmt.Errorf("missing table metdata bucket")
	// 	}
	// 	err := meta.DeleteBucket([]byte("_packinfo"))
	// 	if !store.IsError(err, store.ErrBucketNotFound) {
	// 		return err
	// 	}
	// 	return nil
	// })
	// if err != nil {
	// 	return err
	// }

	// Open table, this will automatically rebuild all metadata
	log.Info("Rebuilding table statistics")
	table, err = db.OpenTable(args.table, opts)
	if err != nil {
		return err
	}

	// Close table, this will automatically store the new metadata
	stats = table.Stats()
	log.Info("Storing table statistics")
	err = table.Close()
	if err != nil {
		return err
	}

	log.Infof("Rebuild took %s, new statistics size %d bytes", time.Since(start), stats[0].MetaSize)
	return nil
}
