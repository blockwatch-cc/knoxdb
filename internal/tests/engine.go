// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package tests

import (
	"context"
	"os"
	"testing"

	"blockwatch.cc/knoxdb/internal/engine"
	"blockwatch.cc/knoxdb/pkg/knox"
	"blockwatch.cc/knoxdb/pkg/schema"
	"blockwatch.cc/knoxdb/pkg/util"
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

func NewTestTableOptions(t *testing.T, driver, eng string) engine.TableOptions {
	return engine.TableOptions{
		Driver:     driver,
		Engine:     engine.TableKind(eng),
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

// NewDatabase sets up a fresh database and creates tables from struct types.
func NewDatabase(t *testing.T, typs ...any) (knox.Database, func()) {
	ctx := context.Background()

	dbPath := t.TempDir()
	eng := util.NonEmptyString(os.Getenv("WORKFLOW_ENGINE"), "pack")
	driver := util.NonEmptyString(os.Getenv("WORKFLOW_DRIVER"), "bolt")

	db, err := knox.CreateDatabase(ctx, "db", NewTestDatabaseOptions(t, driver).
		WithPath(dbPath).
		WithNamespace("cx.bwd.knox.scenarios").
		WithCacheSize(16*(1<<20)).
		WithLogger(log.Log))
	require.NoError(t, err, "Failed to create database")

	log.Infof("Creating enum 'my_enum'")
	_, err = db.CreateEnum(ctx, "my_enum")
	require.NoError(t, err, "Failed to create enum")

	log.Infof("Extending enum 'my_enum' with values: %+v", myEnums)
	err = db.ExtendEnum(ctx, "my_enum", myEnums...)
	require.NoError(t, err, "Failed to extend enum")

	// Create tables for given types
	for _, typ := range typs {
		s, err := schema.SchemaOf(typ)
		require.NoError(t, err, "Failed to generate schema for type %T", typ)
		_, err = db.CreateTable(ctx, s, knox.TableOptions{
			Engine:      engine.TableKind(eng),
			Driver:      driver,
			PackSize:    1 << 11,
			JournalSize: 1 << 10,
			PageFill:    1.0,
		})
		require.NoError(t, err, "Failed to create table for Types")
	}

	return db, func() { db.Close(ctx) }
}
