// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package engine_test

import (
	"context"
	"testing"

	"blockwatch.cc/knoxdb/internal/engine"
	"blockwatch.cc/knoxdb/internal/tests"
	"blockwatch.cc/knoxdb/pkg/schema"
	"github.com/echa/log"
	"github.com/stretchr/testify/require"
)

func TestMain(m *testing.M) {
	// must register enum type with global schema registry
	tests.RegisterEnum()
	m.Run()
}

func TestTableCreate(t *testing.T) {
	ctx := context.Background()
	e := engine.NewTestEngine(engine.NewTestDatabaseOptions(t, "bolt"))
	defer e.Close(ctx)
	ctx, _, commit, abort, err := e.WithTransaction(ctx)
	defer abort()
	require.NoError(t, err)
	opts := engine.TableOptions{
		Engine:          engine.TableKindPack,
		Driver:          "bolt",
		PackSize:        8,
		JournalSize:     4,
		JournalSegments: 1,
		PageSize:        4096,
		PageFill:        1.0,
		ReadOnly:        false,
		NoSync:          false,
		NoGrowSync:      false,
		DB:              nil,
		Logger:          log.Log,
	}
	_, err = e.CreateEnum(ctx, "my_enum")
	require.NoError(t, err)
	tab, err := e.CreateTable(ctx, schema.MustSchemaOf(tests.AllTypes{}), opts)
	require.NoError(t, err)
	require.NoError(t, commit())
	defer tab.Close(ctx)
}

func TestTableDrop(t *testing.T) {

}

func TestTableReopen(t *testing.T) {

}

func TestTableInsert(t *testing.T) {

}

func TestTableUpdate(t *testing.T) {

}

func TestTableDelete(t *testing.T) {

}

func TestTableQuery(t *testing.T) {

}

func TestTableStream(t *testing.T) {

}

func TestTableCount(t *testing.T) {

}

func TestTableIndexQuery(t *testing.T) {

}
