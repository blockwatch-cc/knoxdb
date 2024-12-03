// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package tests

import (
	"context"
	"os"
	"testing"

	"blockwatch.cc/knoxdb/internal/engine"
	"blockwatch.cc/knoxdb/pkg/schema"
	"blockwatch.cc/knoxdb/pkg/util"
	"github.com/echa/log"
	"github.com/stretchr/testify/require"
)

const TEST_DB_NAME = "testdb"

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
		Driver:      driver,
		Engine:      engine.TableKind(eng),
		PageSize:    4096,
		PageFill:    0.9,
		PackSize:    1 << 11,
		JournalSize: 1 << 10,
		NoSync:      false,
		NoGrowSync:  false,
		ReadOnly:    false,
		Logger:      log.Log,
	}
}

func NewTestEngine(t *testing.T, opts engine.DatabaseOptions) *engine.Engine {
	eng, err := engine.Create(context.Background(), TEST_DB_NAME, opts)
	require.NoError(t, err, "Failed to create database")
	return eng
}

func OpenTestEngine(t *testing.T, opts engine.DatabaseOptions) *engine.Engine {
	eng, err := engine.Open(context.Background(), TEST_DB_NAME, opts)
	require.NoError(t, err, "Failed to open database")
	return eng
}

// NewDatabase sets up a fresh database and creates tables from struct types.
func NewDatabase(t *testing.T, typs ...any) (*engine.Engine, func()) {
	ctx := context.Background()

	eng := util.NonEmptyString(os.Getenv("WORKFLOW_ENGINE"), "pack")
	driver := util.NonEmptyString(os.Getenv("WORKFLOW_DRIVER"), "bolt")

	db := NewTestEngine(t, NewTestDatabaseOptions(t, driver))

	log.Infof("Creating enum 'my_enum'")
	_, err := db.CreateEnum(ctx, "my_enum")
	require.NoError(t, err, "Failed to create enum")

	log.Infof("Extending enum 'my_enum' with values: %+v", myEnums)
	err = db.ExtendEnum(ctx, "my_enum", myEnums...)
	require.NoError(t, err, "Failed to extend enum")

	// Create tables for given types
	for _, typ := range typs {
		s, err := schema.SchemaOf(typ)
		require.NoError(t, err, "Failed to generate schema for type %T", typ)
		_, err = db.CreateTable(ctx, s, NewTestTableOptions(t, driver, eng))
		require.NoError(t, err, "Failed to create table for Types")
	}

	return db, func() { db.Close(ctx) }
}
