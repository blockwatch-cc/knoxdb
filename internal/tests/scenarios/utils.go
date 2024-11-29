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

// Types defines the schema for our workload tests
type Types struct {
	Id        uint64    `knox:"id,pk"`
	Timestamp time.Time `knox:"time"`
	String    string    `knox:"string"`
	Int64     int64     `knox:"int64"`
	MyEnum    string    `knox:"my_enum,enum"`
}

// OperationLog defines the schema for workload 4, including thread_id and timestamp for tracking.
// MetaRow defines the schema for meta rows, describing transactions.
// WorkRow defines the schema for work rows, linked to meta rows via MetaRowID.
type OperationLog struct {
	Id        uint64    `knox:"id,pk"`
	ThreadID  int       `knox:"thread_id"`
	Timestamp time.Time `knox:"timestamp"`
	Operation string    `knox:"operation"` // e.g. "insert", "update", "delete"
	Data      string    `knox:"data"`      // Arbitrary data for the operation
}

// MetaRow defines the schema for meta rows, describing transactions.
type MetaRow struct {
	Id        uint64    `knox:"id,pk"`
	ThreadID  int       `knox:"thread_id"`
	Timestamp time.Time `knox:"timestamp"`
	Operation string    `knox:"operation"` // "insert", "update"
}

// WorkRow defines the schema for work rows, which are linked to meta rows via MetaRowID.
// It tracks individual work items and their update status.
type WorkRow struct {
	Id        uint64 `knox:"id,pk"`
	MetaRowID uint64 `knox:"meta_row_id"` // Link to meta row
	Value     string `knox:"value"`
	Updated   bool   `knox:"updated"`
}

// NewRandomData generates random data for the OperationLog's Data field.
func NewRandomData() string {
	bytes := util.RandBytes(8) // Generates 8 random bytes
	return hex.EncodeToString(bytes)
}

// NewRandomTypes generates a random instance of Types, with predictable values
// based on the index `i` for use in tests.
func NewRandomTypes(i int) *Types {
	return &Types{
		Id:        0, // Primary key will be assigned post-insertion
		Timestamp: time.Now().UTC(),
		String:    hex.EncodeToString(util.RandBytes(4)),
		Int64:     int64(i),
		MyEnum:    myEnums[i%len(myEnums)],
	}
}

// cleanDBDir ensures the database directory is removed before tests
func cleanDBDir(path string) error {
	absPath, err := filepath.Abs(path)
	if err != nil {
		return err
	}
	return os.RemoveAll(absPath)
}

// ensureDBDir ensures the database directory exists
func ensureDBDir(path string) error {
	absPath, err := filepath.Abs(path)
	if err != nil {
		return err
	}
	if _, err := os.Stat(absPath); os.IsNotExist(err) {
		log.Infof("Creating database directory: %s", absPath)
		return os.MkdirAll(absPath, 0755)
	}
	return nil
}

// SetupDatabase sets up a fresh database for workload tests, including
// creating the necessary tables, schemas, and enums with thread-safety for enum creation.
func SetupDatabase(t *testing.T) (knox.Database, knox.Table, func()) {
	ctx := context.Background()
	dbPath := t.TempDir()

	// Clean up any leftover state
	require.NoError(t, cleanDBDir(dbPath), "Failed to clean up database directory")
	require.NoError(t, ensureDBDir(dbPath), "Failed to ensure database directory")

	// Create new database
	db, err := knox.CreateDatabase(ctx, "types", knox.DefaultDatabaseOptions.
		WithPath(dbPath).
		WithNamespace("cx.bwd.knox.scenarios").
		WithCacheSize(128*(1<<20)).
		WithLogger(log.Log))
	require.NoError(t, err, "Failed to create database")

	// Create enum
	log.Infof("Creating enum 'my_enum'")
	enumMutex.Lock() // Ensure thread safety for enum creation
	enum, err := db.CreateEnum(ctx, "my_enum")
	if err != nil {
		log.Warnf("Enum 'my_enum' may already exist: %v", err)
	}
	if enum != nil {
		log.Infof("Enum created: %v", enum)
	}
	enumMutex.Unlock()

	// Extend the enum with values
	log.Infof("Extending enum 'my_enum' with values: %+v", myEnums)
	enumMutex.Lock()
	err = db.ExtendEnum(ctx, "my_enum", myEnums...)
	enumMutex.Unlock()
	require.NoErrorf(t, err, "Failed to extend enum 'my_enum': %v", err)

	// Validate that the enum exists
	enums := db.ListEnums()
	log.Infof("Registered Enums: %+v", enums)
	require.Contains(t, enums, "my_enum", "Enum 'my_enum' is not registered")

	// Create schema for Types
	s, err := schema.SchemaOf(&Types{})
	require.NoError(t, err, "Failed to generate schema for Types")
	log.Infof("Generated schema with fields:")
	for _, field := range s.Fields() {
		log.Infof("  - Field: Name=%s, Type=%s", field.Name(), field.Type())
	}

	// Create new table
	table, err := db.CreateTable(ctx, s, knox.TableOptions{
		Engine:      "pack",
		Driver:      "bolt",
		PackSize:    1 << 16,
		JournalSize: 1 << 17,
		PageFill:    1.0,
	})
	require.NoError(t, err, "Failed to create table")

	return db, table, func() { db.Close(ctx) }
}

// SetupDatabaseWithSchema sets up a fresh database and table with a provided schema model.
func SetupDatabaseWithSchema(t *testing.T, model interface{}) (knox.Database, knox.Table, func()) {
	ctx := context.Background()
	dbPath := t.TempDir()

	require.NoError(t, cleanDBDir(dbPath), "Failed to clean up database directory")
	require.NoError(t, ensureDBDir(dbPath), "Failed to ensure database directory")

	db, err := knox.CreateDatabase(ctx, "w4", knox.DefaultDatabaseOptions.
		WithPath(dbPath).
		WithNamespace("cx.bwd.knox.w4").
		WithCacheSize(128*(1<<20)).
		WithLogger(log.Log))
	require.NoError(t, err, "Failed to create database")

	schema, err := schema.SchemaOf(model)
	require.NoError(t, err, "Failed to generate schema")

	table, err := db.CreateTable(ctx, schema, knox.TableOptions{
		Engine:      "pack",
		Driver:      "bolt",
		PackSize:    1 << 16,
		JournalSize: 1 << 17,
		PageFill:    1.0,
	})
	require.NoError(t, err, "Failed to create table")

	return db, table, func() { db.Close(ctx) }
}
