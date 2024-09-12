// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package knox

import (
	"blockwatch.cc/knoxdb/internal/engine"
	"github.com/echa/log"
)

var (
	DefaultDatabaseOptions = engine.DatabaseOptions{
		PageSize:  1024,
		PageFill:  0.8,
		CacheSize: 16 * 1 << 20,
		Logger:    log.New(nil).SetLevel(log.LevelInfo),
	}
	ReadonlyDatabaseOptions = engine.DatabaseOptions{
		PageSize:  1024,
		PageFill:  0.8,
		CacheSize: 16 * 1 << 20,
		ReadOnly:  true,
		Logger:    log.New(nil).SetLevel(log.LevelInfo),
	}
)
