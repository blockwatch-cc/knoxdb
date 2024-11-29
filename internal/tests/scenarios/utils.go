// Copyright (c) 2024 Blockwatch Data Inc.
// Author: oliver@blockwatch.cc
//
// utils.go contains shared utilities for KnoxDB workload tests,
// including database setup, schema definitions, thread-safe operations,
// and helper functions for generating test data. These utilities are used across all workloads.

package scenarios

import (
	"context"
	"encoding/hex"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"blockwatch.cc/knoxdb/pkg/knox"
	"blockwatch.cc/knoxdb/pkg/schema"
	"blockwatch.cc/knoxdb/pkg/util"
	"github.com/echa/log"
	"github.com/stretchr/testify/require"
)

var (
	myEnums   = []string{"one", "two", "three", "four"}
	enumMutex sync.Mutex // Mutex for synchronizing EnumRegistry access
)

// Types defines the schema for Workload1 and Workload2.
type Types struct {
	Id        uint64    `knox:"id,pk"`
	Timestamp time.Time `knox:"time"`
	String    string    `knox:"string"`
	Int64     int64     `knox:"int64"`
	MyEnum    string    `knox:"my_enum,enum"`
}

// NewRandomData generates random data for UnifiedRow and Types.
func NewRandomData() string {
	bytes := util.RandBytes(8) // Generates 8 random bytes
	return hex.EncodeToString(bytes)
}

// NewRandomTypes generates random instances of Types for workloads.
func NewRandomTypes(i int) *Types {
	return &Types{
		Id:        0, // Primary key will be assigned post-insertion
		Timestamp: time.Now().UTC(),
		String:    hex.EncodeToString(util.RandBytes(4)),
		Int64:     int64(i),
		MyEnum:    myEnums[i%len(myEnums)],
	}
}

// SetupDatabase sets up a fresh database for Workload1 and Workload2.
func SetupDatabase(t *testing.T) (knox.Database, knox.Table, func()) {
	ctx := context.Background()
	dbPath := t.TempDir()

	require.NoError(t, cleanDBDir(dbPath), "Failed to clean up database directory")
	require.NoError(t, ensureDBDir(dbPath), "Failed to ensure database directory")

	db, err := knox.CreateDatabase(ctx, "types", knox.DefaultDatabaseOptions.
		WithPath(dbPath).
		WithNamespace("cx.bwd.knox.scenarios").
		WithCacheSize(128*(1<<20)).
		WithLogger(log.Log))
	require.NoError(t, err, "Failed to create database")

	log.Infof("Creating enum 'my_enum'")
	enumMutex.Lock()
	_, err = db.CreateEnum(ctx, "my_enum")
	enumMutex.Unlock()
	require.NoError(t, err, "Failed to create enum")

	log.Infof("Extending enum 'my_enum' with values: %+v", myEnums)
	enumMutex.Lock()
	err = db.ExtendEnum(ctx, "my_enum", myEnums...)
	enumMutex.Unlock()
	require.NoError(t, err, "Failed to extend enum")

	// Create schema for Types
	s, err := schema.SchemaOf(&Types{})
	require.NoError(t, err, "Failed to generate schema for Types")

	table, err := db.CreateTable(ctx, s, knox.TableOptions{
		Engine:      "pack",
		Driver:      "bolt",
		PackSize:    1 << 16,
		JournalSize: 1 << 17,
		PageFill:    1.0,
	})
	require.NoError(t, err, "Failed to create table for Types")

	return db, table, func() { db.Close(ctx) }
}

// Unified database setup for Workload4.
func SetupUnifiedDatabase(t *testing.T) (knox.Database, knox.Table, func()) {
	ctx := context.Background()
	dbPath := t.TempDir()

	require.NoError(t, cleanDBDir(dbPath), "Failed to clean up database directory")
	require.NoError(t, ensureDBDir(dbPath), "Failed to ensure database directory")

	db, err := knox.CreateDatabase(ctx, "workload4", knox.DefaultDatabaseOptions.
		WithPath(dbPath).
		WithNamespace("cx.bwd.knox.workload4").
		WithCacheSize(128*(1<<20)).
		WithLogger(log.Log))
	require.NoError(t, err, "Failed to create database")

	schema, err := schema.SchemaOf(&UnifiedRow{})
	require.NoError(t, err, "Failed to generate schema")

	table, err := db.CreateTable(ctx, schema, knox.TableOptions{
		Engine:      "pack",
		Driver:      "bolt",
		PackSize:    1 << 16,
		JournalSize: 1 << 17,
		PageFill:    1.0,
	})
	require.NoError(t, err, "Failed to create unified table")

	return db, table, func() { db.Close(ctx) }
}

// Directory utility functions for cleaning and ensuring directories
func cleanDBDir(path string) error {
	absPath, err := filepath.Abs(path)
	if err != nil {
		return err
	}
	return os.RemoveAll(absPath)
}

func ensureDBDir(path string) error {
	absPath, err := filepath.Abs(path)
	if err != nil {
		return err
	}
	if _, err := os.Stat(absPath); os.IsNotExist(err) {
		return os.MkdirAll(absPath, 0755)
	}
	return nil
}
