// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package knox

import "blockwatch.cc/knoxdb/internal/engine"

var (
	DefaultDatabaseOptions = engine.DatabaseOptions{
		PageSize:  1024,
		PageFill:  0.8,
		CacheSize: 16 * 1 << 20,
	}
	ReadonlyDatabaseOptions = engine.DatabaseOptions{
		PageSize:  1024,
		PageFill:  0.8,
		CacheSize: 16 * 1 << 20,
		ReadOnly:  true,
	}
)
