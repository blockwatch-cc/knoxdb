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
	t.Helper()
	driver = util.NonEmptyString(driver, os.Getenv("WORKFLOW_DRIVER"), "bolt")
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
	t.Helper()
	driver = util.NonEmptyString(driver, os.Getenv("WORKFLOW_DRIVER"), "bolt")
	eng = util.NonEmptyString(eng, os.Getenv("WORKFLOW_ENGINE"), "pack")
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
	t.Helper()
	eng, err := engine.Create(context.Background(), TEST_DB_NAME, opts)
	require.NoError(t, err, "Failed to create database")
	return eng
}

func OpenTestEngine(t *testing.T, opts engine.DatabaseOptions) *engine.Engine {
	t.Helper()
	eng, err := engine.Open(context.Background(), TEST_DB_NAME, opts)
	require.NoError(t, err, "Failed to open database at %s", opts.Path)
	return eng
}

// NewDatabase sets up a fresh database and creates tables from struct types.
func NewDatabase(t *testing.T, typs ...any) (*engine.Engine, func()) {
	t.Helper()
	dbo := NewTestDatabaseOptions(t, "")
	db := NewTestEngine(t, dbo)
	t.Logf("NEW DB catalog driver=%s at %s", dbo.Driver, dbo.Path)

	ctx := context.Background()
	_, err := db.CreateEnum(ctx, "my_enum")
	require.NoError(t, err, "Failed to create enum")

	err = db.ExtendEnum(ctx, "my_enum", myEnums...)
	require.NoError(t, err, "Failed to extend enum")

	// Create tables for given types
	for _, typ := range typs {
		s, err := schema.SchemaOf(typ)
		require.NoError(t, err, "Failed to generate schema for type %T", typ)
		opts := NewTestTableOptions(t, "", "")
		_, err = db.CreateTable(ctx, s, opts)
		require.NoError(t, err, "Failed to create table for type %T", typ)
		t.Logf("NEW table=%s driver=%s engine=%s", s.Name(), opts.Driver, opts.Engine)
	}

	return db, func() { db.Close(ctx) }
}
