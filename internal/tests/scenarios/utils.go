// Copyright (c) 2024 Blockwatch Data Inc.
// Author: oliver@blockwatch.cc
//
// utils.go contains shared utilities for KnoxDB workload tests,
// including database setup, schema definitions, and helper functions
// for generating test data. These utilities are used across all workloads.

package scenarios

import (
	"context"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"
	"time"

	"blockwatch.cc/knoxdb/pkg/knox"
	"blockwatch.cc/knoxdb/pkg/schema"
	"blockwatch.cc/knoxdb/pkg/util"
	"github.com/echa/log"
	"github.com/stretchr/testify/require"
)

// Define a type alias for schema.Enum if not already defined
type Enum string

var myEnums = []Enum{"one", "two", "three", "four"}

// Types defines the schema for our workload tests.
type Types struct {
	Id        uint64 `knox:"id,pk"`
	Timestamp time.Time
	String    string
	Int64     int64
	MyEnum    Enum `knox:"my_enum,enum"`
}

// NewRandomTypes generates a random instance of Types.
func NewRandomTypes(i int) *Types {
	return &Types{
		Id:        0, // Empty, will be set by insert
		Timestamp: time.Now().UTC(),
		String:    hex.EncodeToString(util.RandBytes(4)),
		Int64:     int64(i),
		MyEnum:    myEnums[i%len(myEnums)],
	}
}

// ensureDBDir ensures the database directory exists.
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

// SetupDatabase initializes a KnoxDB instance for testing.
func SetupDatabase(t *testing.T) (knox.Database, knox.Table, func()) {
	ctx := context.Background()

	dbPath := "./db"
	require.NoError(t, ensureDBDir(dbPath))

	db, err := knox.OpenDatabase(ctx, "types", knox.DatabaseOptions{
		Path:      dbPath,
		Namespace: "cx.bwd.knox.types-demo",
		Logger:    log.Log,
	})
	if err == nil {
		table, err := db.UseTable("types")
		require.NoError(t, err)
		return db, table, func() { db.Close(ctx) }
	}

	// Create schema and table if database doesn't exist.
	s, err := schema.SchemaOf(&Types{})
	require.NoError(t, err)

	db, err = knox.CreateDatabase(ctx, "types", knox.DefaultDatabaseOptions.
		WithPath(dbPath).
		WithNamespace("cx.bwd.knox.types-demo").
		WithCacheSize(128*(1<<20)).
		WithLogger(log.Log))
	require.NoError(t, err)

	// Extend enum
	err = db.ExtendEnum(ctx, "my_enum", []string{"one", "two", "three", "four"}...)
	require.NoError(t, err)

	table, err := db.CreateTable(ctx, s, knox.TableOptions{
		Engine:      "pack",
		Driver:      "bolt",
		PackSize:    1 << 16,
		JournalSize: 1 << 17,
		PageFill:    1.0,
	})
	require.NoError(t, err)

	return db, table, func() { db.Close(ctx) }
}
