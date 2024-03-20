// Copyright (c) 2018-2022 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package main

import (
	"context"

	"github.com/echa/log"

	"blockwatch.cc/knoxdb/pack"
)

// reindex drops and re-creates all indexes defined for a given table.
func reindex(ctx context.Context, data interface{}) error {
	table := data.(*pack.PackTable)

	// make sure source table journals are flushed
	log.Infof("Flushing source table %s", table.Name())
	if err := table.Flush(ctx); err != nil {
		return err
	}

	// walk source table in packs and bulk-insert data into target
	stats := table.Stats()
	log.Infof("Rebuild indexes over %d rows / %d packs from table %s",
		stats[0].TupleCount, stats[0].PacksCount, table.Name())

	// rebuild indexes
	for _, idx := range table.Indexes() {
		log.Infof("Rebuilding %s index on field %s (%s)", idx.Name(), idx.Field().Name, idx.Field().Type)
		prog := make(chan float64, 100)
		go func() {
			for {
				select {
				case <-ctx.Done():
					return
				case f := <-prog:
					log.Infof("Index build progress %.2f%%", f)
					if f == 100 {
						return
					}
				}
			}
		}()
		// flush every 128 packs
		err := idx.Reindex(ctx, 128, prog)
		close(prog)
		if err != nil {
			return err
		}
	}
	return nil
}
