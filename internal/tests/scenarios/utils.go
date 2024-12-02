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
	"testing"
	"time"

	"blockwatch.cc/knoxdb/pkg/knox"
	"blockwatch.cc/knoxdb/pkg/schema"
	"blockwatch.cc/knoxdb/pkg/util"
	"github.com/echa/log"
	"github.com/stretchr/testify/require"
)

var (
	myEnums = []string{"one", "two", "three", "four"}
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
func SetupDatabase(t *testing.T, typ any) (knox.Database, knox.Table, func()) {
	ctx := context.Background()
	dbPath := t.TempDir()

	db, err := knox.CreateDatabase(ctx, "db", knox.DefaultDatabaseOptions.
		WithPath(dbPath).
		WithNamespace("cx.bwd.knox.scenarios").
		WithCacheSize(128*(1<<20)).
		WithLogger(log.Log))
	require.NoError(t, err, "Failed to create database")

	log.Infof("Creating enum 'my_enum'")
	_, err = db.CreateEnum(ctx, "my_enum")
	require.NoError(t, err, "Failed to create enum")

	log.Infof("Extending enum 'my_enum' with values: %+v", myEnums)
	err = db.ExtendEnum(ctx, "my_enum", myEnums...)
	require.NoError(t, err, "Failed to extend enum")

	// Create schema for given type
	s, err := schema.SchemaOf(typ)
	require.NoError(t, err, "Failed to generate schema for type %T", typ)

	table, err := db.CreateTable(ctx, s, knox.TableOptions{
		Engine:      "pack",
		Driver:      "bolt",
		PackSize:    1 << 11,
		JournalSize: 1 << 10,
		PageFill:    1.0,
	})
	require.NoError(t, err, "Failed to create table for Types")

	return db, table, func() { db.Close(ctx) }
}
