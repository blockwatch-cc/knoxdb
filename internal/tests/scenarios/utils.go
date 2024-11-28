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

var myEnums = []string{"one", "two", "three", "four"}

// Types defines the schema for our workload tests
type Types struct {
	Id        uint64    `knox:"id,pk"`
	Timestamp time.Time `knox:"time"`
	String    string    `knox:"string"`
	Int64     int64     `knox:"int64"`
	MyEnum    string    `knox:"my_enum,enum"`
}

// NewRandomTypes generates a random instance of Types
func NewRandomTypes(i int) *Types {
	return &Types{
		Id:        0,
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

func SetupDatabase(t *testing.T) (knox.Database, knox.Table, func()) {
	ctx := context.Background()
	dbPath := "./db"

	// Clean up any leftover state
	require.NoError(t, cleanDBDir(dbPath), "Failed to clean up database directory")
	require.NoError(t, ensureDBDir(dbPath), "Failed to ensure database directory")

	// Create new database
	db, err := knox.CreateDatabase(ctx, "types", knox.DefaultDatabaseOptions.
		WithPath(dbPath).
		WithNamespace("cx.bwd.knox.types-demo").
		WithCacheSize(128*(1<<20)).
		WithLogger(log.Log))
	require.NoError(t, err, "Failed to create database")

	// Create enum
	log.Infof("Creating enum 'my_enum'")
	enum, err := db.CreateEnum(ctx, "my_enum")
	if err != nil {
		log.Warnf("Enum 'my_enum' may already exist: %v", err)
	}
	if enum != nil {
		log.Infof("Enum created: %v", enum)
	}

	// Extend the enum with values
	log.Infof("Extending enum 'my_enum' with values: %+v", myEnums)
	err = db.ExtendEnum(ctx, "my_enum", myEnums...)
	if err != nil {
		log.Errorf("Failed to extend enum 'my_enum': %v", err)
		require.NoError(t, err, "Failed to extend enum 'my_enum'")
	}
	log.Infof("Successfully extended enum 'my_enum' with values: %+v", myEnums)

	// Validate that the enum exists
	enums := db.ListEnums()
	log.Infof("Existing enums after extending: %+v", enums)
	require.Contains(t, enums, "my_enum", "Enum 'my_enum' is not registered")

	// Create schema for Types
	s, err := schema.SchemaOf(&Types{})
	require.NoError(t, err, "Failed to generate schema for Types")
	log.Infof("Generated schema: %+v", s)

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
