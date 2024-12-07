// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package tests

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"blockwatch.cc/knoxdb/internal/engine"
	"blockwatch.cc/knoxdb/pkg/schema"
	"blockwatch.cc/knoxdb/pkg/util"
	"github.com/echa/log"
	"github.com/stretchr/testify/require"
)

const (
	TEST_DB_NAME = "testdb"
	SAVE_PATH    = "./data"
)

func NewTestDatabaseOptions(t *testing.T, driver string) engine.DatabaseOptions {
	t.Helper()
	driver = util.NonEmptyString(driver, os.Getenv("KNOX_DRIVER"), "bolt")
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
	driver = util.NonEmptyString(driver, os.Getenv("KNOX_DRIVER"), "bolt")
	eng = util.NonEmptyString(eng, os.Getenv("KNOX_ENGINE"), "pack")
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
	t.Logf("NEW DB catalog driver=%s at %s", dbo.Driver, dbo.Path)
	db := NewTestEngine(t, dbo)

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

func SaveDatabaseFiles(t *testing.T, e *engine.Engine) {
	t.Helper()

	// skip on successful tests or when running in wasm
	if !t.Failed() || runtime.GOARCH == "wasm" {
		return
	}

	srcPath := e.Options().Path
	dstPath, _ := filepath.Abs(SAVE_PATH)
	dstPath = filepath.Join(dstPath, fmt.Sprintf("db-%s-%s", t.Name(), time.Now().UTC().Format("2006-01-02_15-04-05")))
	err := os.CopyFS(dstPath, os.DirFS(srcPath))
	if err != nil {
		t.Logf("Error saving database files: %v", err)
	} else {
		t.Logf("Saved database files to %s", dstPath)
	}
}
