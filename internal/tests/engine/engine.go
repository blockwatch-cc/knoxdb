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

func NewTestDatabaseOptions(t testing.TB, opts ...engine.Option) engine.Options {
	t.Helper()
	testOpts := engine.Options{
		Path:       t.TempDir(),
		MaxWorkers: 2,
		MaxTasks:   4,
		Driver:     firstOf(os.Getenv("KNOX_DRIVER"), "bolt"),
		PageSize:   4096,
		PageFill:   1.0,
		CacheSize:  1 << 20,
		// NoSync:     true, // required for table wal tests
		Log: log.Log.Clone(""),
	}
	return testOpts.Apply(opts...)
}

func NewTestTableOptions(t testing.TB, opts ...engine.Option) engine.Options {
	t.Helper()
	testOpts := engine.Options{
		Driver:      firstOf(os.Getenv("KNOX_DRIVER"), "bolt"),
		Engine:      firstOf(os.Getenv("KNOX_ENGINE"), "pack"),
		PageSize:    1 << 16, // 64kB
		PageFill:    0.9,
		PackSize:    1 << 16, // 64k
		JournalSize: 1 << 16, // 64k
		// NoSync:      true,
		Log: log.Log.Clone(""),
	}
	return testOpts.Apply(opts...)
}

func NewTestIndexOptions(t testing.TB, opts ...engine.Option) engine.Options {
	t.Helper()
	testOpts := engine.Options{
		Driver:      firstOf(os.Getenv("KNOX_DRIVER"), "bolt"),
		Engine:      firstOf(os.Getenv("KNOX_ENGINE"), "pack"),
		JournalSize: 1 << 16, // 64k
		PageSize:    1 << 16, // 64kB
		PageFill:    0.9,
		PackSize:    1 << 12, // 4k
		// NoSync:      true,
		Log: log.Log.Clone(""),
	}
	return testOpts.Apply(opts...)
}

func NewTestEngine(t testing.TB, opts ...engine.Option) *engine.Engine {
	t.Helper()
	dbo := NewTestDatabaseOptions(t, opts...)
	if testing.Verbose() {
		t.Logf("NEW DB catalog driver=%s at %s", dbo.Driver, dbo.Path)
	}
	eng, err := engine.Create(context.Background(), TEST_DB_NAME, dbo.DatabaseOptions()...)
	require.NoError(t, err, "Failed to create database at %s", dbo.Path)
	return eng
}

func OpenTestEngine(t testing.TB, opts ...engine.Option) *engine.Engine {
	t.Helper()
	opt := NewTestDatabaseOptions(t, opts...)
	eng, err := engine.Open(context.Background(), TEST_DB_NAME, opt.DatabaseOptions()...)
	require.NoError(t, err, "Failed to open database at %s", opt.Path)
	return eng
}

// NewDatabase sets up a fresh database and creates tables from struct types.
func NewDatabase(t testing.TB, typ any, opts ...engine.Option) (*engine.Engine, func()) {
	t.Helper()
	db := NewTestEngine(t, opts...)

	ctx := context.Background()
	// t.Logf("NEW enum=my_enum")
	e, err := db.CreateEnum(ctx, "my_enum")
	require.NoError(t, err, "Failed to create enum")

	err = db.ExtendEnum(ctx, "my_enum", myEnums...)
	require.NoError(t, err, "Failed to extend enum")

	enums := enum.NewRegistry()
	enums.Register(0, e)

	// Create tables and indexes for given types
	s, err := reflect.SchemaOf(typ, schema.Enums(enums))
	require.NoError(t, err, "Failed to generate schema for type %T", typ)
	to := NewTestTableOptions(t, opts...)
	if testing.Verbose() {
		t.Logf("NEW table=%s driver=%s engine=%s", s.Name, to.Driver, to.Engine)
		t.Log("Using schema", s)
	}
	_, err = db.CreateTable(ctx, s, to.TableOptions()...)
	require.NoError(t, err, "Failed to create table for type %T", typ)

	indexes, err := reflect.IndexesOf(typ)
	require.NoError(t, err, "Failed to generate index schemas for type %T", typ)

	// create indexes for type
	for _, is := range indexes {
		iop := NewTestIndexOptions(t, opts...)
		_, err = db.CreateIndex(ctx, is, iop.IndexOptions()...)
		require.NoError(t, err, "create pk index")
	}

	return db, func() {
		if testing.Verbose() {
			t.Log("Cleanup up after test.")
		}
		for _, is := range indexes {
			require.NoError(t, db.DropIndex(ctx, is.Name))
		}
		require.NoError(t, db.DropTable(ctx, s.Name))
		require.NoError(t, db.DropEnum(ctx, "my_enum"))
		require.NoError(t, db.Close(ctx))
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
