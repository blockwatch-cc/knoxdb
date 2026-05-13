// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package knox

import (
	"blockwatch.cc/knoxdb/internal/engine"
	"github.com/echa/log"
)

var (
	defaultDatabaseOptions = engine.Options{
		Driver:    "bolt",
		PageSize:  16 * 1024,
		PageFill:  1.0,
		CacheSize: 16 * 1 << 20,
		Log:       log.New(nil).SetLevel(log.LevelInfo),
	}
	readonlyDatabaseOptions = engine.Options{
		Driver:    "bolt",
		PageSize:  16 * 1024,
		PageFill:  1.0,
		CacheSize: 16 * 1 << 20,
		ReadOnly:  true,
		Log:       log.New(nil).SetLevel(log.LevelInfo),
	}
)

func NewDefaultOptions() []engine.Option {
	return defaultDatabaseOptions.DatabaseOptions()
}

func NewReadOnlyOptions() []engine.Option {
	return readonlyDatabaseOptions.DatabaseOptions()
}

func NewTableOptions() []engine.Option {
	return defaultDatabaseOptions.TableOptions()
}

func NewIndexOptions() []engine.Option {
	return defaultDatabaseOptions.IndexOptions()
}
