// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package tests

import (
	"context"
	"testing"

	"blockwatch.cc/knoxdb/internal/engine"
	"github.com/echa/log"
	"github.com/stretchr/testify/require"
)

const TEST_DB_NAME = "test"

func NewTestDatabaseOptions(t *testing.T, driver string) engine.DatabaseOptions {
	return engine.DatabaseOptions{
		Path:       t.TempDir(),
		Namespace:  "cx.bwd.knoxdb.testdb",
		Driver:     driver,
		PageSize:   4096,
		PageFill:   1.0,
		CacheSize:  1 << 20,
		NoSync:     false,
		NoGrowSync: false,
		ReadOnly:   false,
		Logger:     log.Log,
	}
}

func NewTestTableOptions(t *testing.T, driver string) engine.TableOptions {
	return engine.TableOptions{
		Driver:     driver,
		PageSize:   4096,
		PageFill:   0.9,
		NoSync:     false,
		NoGrowSync: false,
		ReadOnly:   false,
		Logger:     log.Log,
	}
}

func NewTestEngine(t *testing.T, ctx context.Context, opts engine.DatabaseOptions) *engine.Engine {
	eng, err := engine.Create(context.Background(), "test-engine", opts)
	require.NoError(t, err)
	return eng
}

func OpenTestEngine(t *testing.T, opts engine.DatabaseOptions) *engine.Engine {
	eng, err := engine.Open(context.Background(), "test-engine", opts)
	require.NoError(t, err)
	return eng
}
