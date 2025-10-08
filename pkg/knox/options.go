// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package knox

import (
	"blockwatch.cc/knoxdb/internal/engine"
	"github.com/echa/log"
)

var (
	DefaultDatabaseOptions = engine.DatabaseOptions{
		Driver:    "bolt",
		PageSize:  16 * 1024,
		PageFill:  1.0,
		CacheSize: 16 * 1 << 20,
		Logger:    log.New(nil).SetLevel(log.LevelInfo),
	}
	ReadonlyDatabaseOptions = engine.DatabaseOptions{
		Driver:    "bolt",
		PageSize:  16 * 1024,
		PageFill:  1.0,
		CacheSize: 16 * 1 << 20,
		ReadOnly:  true,
		Logger:    log.New(nil).SetLevel(log.LevelInfo),
	}
)

func NewDatabaseOptions() engine.DatabaseOptions {
	return DefaultDatabaseOptions
}

func NewTableOptions() engine.TableOptions {
	return engine.TableOptions{}
}

func NewIndexOptions() engine.IndexOptions {
	return engine.IndexOptions{}
}

func NewStoreOptions() engine.StoreOptions {
	return engine.StoreOptions{}
}
