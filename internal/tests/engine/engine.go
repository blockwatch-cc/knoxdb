// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package engine_tests

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
	"blockwatch.cc/knoxdb/pkg/schema/enum"
	"blockwatch.cc/knoxdb/pkg/schema/reflect"
	"github.com/echa/log"
	"github.com/stretchr/testify/require"
)

const (
	TEST_DB_NAME = "testdb"
	SAVE_PATH    = "./data"
)

func firstOf(s ...string) string {
	for _, v := range s {
		if v != "" {
			return v
		}
	}
	return ""
}

func NewTestDatabaseOptions(t testing.TB, driver string) engine.Options {
	t.Helper()
	driver = firstOf(driver, os.Getenv("KNOX_DRIVER"), "bolt")
	return engine.Options{
		Path:       t.TempDir(),
		MaxWorkers: 2,
		MaxTasks:   4,
		Driver:     driver,
		PageSize:   4096,
		PageFill:   1.0,
		CacheSize:  1 << 20,
		// NoSync:     true, // required for table wal tests
		ReadOnly: false,
		Log:      log.Log.Clone(""),
		IsTemp:   false,
	}
}

func NewTestTableOptions(t testing.TB, driver, eng string) engine.Options {
	t.Helper()
	driver = firstOf(driver, os.Getenv("KNOX_DRIVER"), "bolt")
	eng = firstOf(eng, os.Getenv("KNOX_ENGINE"), "pack")
	return engine.Options{
		Driver:      driver,
		Engine:      eng,
		PageSize:    1 << 16, // 64kB
		PageFill:    0.9,
		PackSize:    1 << 16, // 64k
		JournalSize: 1 << 16, // 64k
		// NoSync:      true,
		ReadOnly: false,
		Log:      log.Log.Clone(""),
		IsTemp:   false,
	}
}

func NewTestIndexOptions(t testing.TB, driver, eng string) engine.Options {
	t.Helper()
	driver = firstOf(driver, os.Getenv("KNOX_DRIVER"), "bolt")
	eng = firstOf(eng, os.Getenv("KNOX_ENGINE"), "pack")
	return engine.Options{
		Driver:      driver,
		Engine:      eng,
		JournalSize: 1 << 16, // 64k
		PageSize:    1 << 16, // 64kB
		PageFill:    0.9,
		PackSize:    1 << 12, // 4k
		ReadOnly:    false,
		// NoSync:      true,
		Log:    log.Log.Clone(""),
		IsTemp: false,
	}
}

func NewTestEngine(t testing.TB, opts engine.Options) *engine.Engine {
	t.Helper()
	eng, err := engine.Create(context.Background(), TEST_DB_NAME, opts.DatabaseOptions()...)
	require.NoError(t, err, "Failed to create database")
	return eng
}

func OpenTestEngine(t testing.TB, opts engine.Options) *engine.Engine {
	t.Helper()
	eng, err := engine.Open(context.Background(), TEST_DB_NAME, opts.DatabaseOptions()...)
	require.NoError(t, err, "Failed to open database at %s", opts.Path)
	return eng
}

// NewDatabase sets up a fresh database and creates tables from struct types.
func NewDatabase(t testing.TB, typs ...any) (*engine.Engine, func()) {
	t.Helper()
	dbo := NewTestDatabaseOptions(t, "")
	if testing.Verbose() {
		t.Logf("NEW DB catalog driver=%s at %s", dbo.Driver, dbo.Path)
	}
	db := NewTestEngine(t, dbo)

	ctx := context.Background()
	// t.Logf("NEW enum=my_enum")
	e, err := db.CreateEnum(ctx, "my_enum")
	require.NoError(t, err, "Failed to create enum")

	err = db.ExtendEnum(ctx, "my_enum", myEnums...)
	require.NoError(t, err, "Failed to extend enum")

	enums := enum.NewEnumRegistry()
	enums.Register(0, e)

	// Create tables and indexes for given types
	for _, typ := range typs {
		s, err := reflect.SchemaOf(typ, schema.Enums(enums))
		require.NoError(t, err, "Failed to generate schema for type %T", typ)
		t.Log("Using schema", s)
		opts := NewTestTableOptions(t, "", "")
		if testing.Verbose() {
			t.Logf("NEW table=%s driver=%s engine=%s", s.Name, opts.Driver, opts.Engine)
		}
		_, err = db.CreateTable(ctx, s, opts.TableOptions()...)
		require.NoError(t, err, "Failed to create table for type %T", typ)

		indexes, err := reflect.IndexesOf(typ)
		require.NoError(t, err, "Failed to generate index schemas for type %T", typ)

		// create indexes for type
		for _, is := range indexes {
			iopts := NewTestIndexOptions(t, "", "")
			_, err = db.CreateIndex(ctx, is, iopts.IndexOptions()...)
			require.NoError(t, err, "create pk index")
		}
	}

	return db, func() {
		if testing.Verbose() {
			t.Log("Cleanup up after test.")
		}
		for _, typ := range typs {
			s, _ := reflect.SchemaOf(typ)
			idxs, _ := reflect.IndexesOf(typ)
			for _, is := range idxs {
				require.NoError(t, db.DropIndex(ctx, is.Name))
			}
			require.NoError(t, db.DropTable(ctx, s.Name))
		}
		require.NoError(t, db.DropEnum(ctx, "my_enum"))
		require.NoError(t, db.Close(ctx))
		require.NoError(t, engine.Drop(TEST_DB_NAME, dbo.DatabaseOptions()...))
	}
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
