// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package engine_tests

import (
	"context"
	"path/filepath"
	"testing"

	"blockwatch.cc/knoxdb/internal/engine"
	"blockwatch.cc/knoxdb/internal/pack"
	"blockwatch.cc/knoxdb/internal/store"
	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/schema"
	"github.com/stretchr/testify/require"
)

var CompositeIndexTestCases = []IndexTestCase{
	{
		Name: "Create",
		Run:  CreateCompositeIndexTest,
	},
	{
		Name: "Open",
		Run:  OpenCompositeIndexTest,
	},
	{
		Name: "Drop",
		Run:  DropCompositeIndexTest,
	},
	{
		Name: "Truncate",
		Run:  TruncateCompositeIndexTest,
	},
	{
		Name: "Rebuild",
		Run:  RebuildCompositeIndexTest,
	},
	{
		Name: "Add",
		Run:  AddCompositeIndexTest,
	},
	{
		Name: "Del",
		Run:  DeleteCompositeIndexTest,
	},
	{
		Name: "CanMatch",
		Run:  CanMatchCompositeIndexTest,
	},
	{
		Name: "IsComposite",
		Run:  IsCompositeIndexTest,
	},
	{
		Name: "QueryComposite",
		Run:  QueryCompositeIndexTest,
	},
	{
		Name: "Sync",
		Run:  SyncCompositeIndexTest,
	},
	{
		Name: "Close",
		Run:  CloseCompositeIndexTest,
	},
}

func TestCompositeIndexEngine[T any, F IF[T]](t *testing.T, driver, eng string, table engine.TableEngine) {
	t.Helper()
	for _, c := range CompositeIndexTestCases {
		t.Run(c.Name, func(t *testing.T) {
			t.Helper()
			ctx := context.Background()
			e := NewTestEngine(t, NewTestDatabaseOptions(t, driver))
			defer e.Close(ctx)

			// create table and insert data
			CreateEnum(t, e)
			topts := NewTestTableOptions(t, driver, eng)
			CreateTable(t, e, table, topts, allTypesSchema)
			defer table.Close(ctx)
			InsertData(t, e, table)

			iopts := NewTestIndexOptions(t, driver, eng, types.IndexTypeComposite)
			indexSchema, err := allTypesSchema.SelectFields("i32", "i64", "id")
			require.NoError(t, err)

			var indexEngine F = new(T)
			c.Run(t, e, table, indexEngine, indexSchema, allTypesSchema, iopts, topts)
		})
	}
}

func CreateCompositeIndexTest(t *testing.T, e *engine.Engine, tab engine.TableEngine, ti engine.IndexEngine, is, ts *schema.Schema, io engine.IndexOptions, to engine.TableOptions) {
	CreateIndex(t, ti, tab, e, io, is)
}

func OpenCompositeIndexTest(t *testing.T, e *engine.Engine, tab engine.TableEngine, ti engine.IndexEngine, is, ts *schema.Schema, io engine.IndexOptions, to engine.TableOptions) {
	CreateIndex(t, ti, tab, e, io, is)
	require.NoError(t, ti.Close(context.Background()))
	ctx, _, commit, abort, err := e.WithTransaction(context.Background())
	defer abort()
	require.NoError(t, err)
	require.NoError(t, ti.Open(ctx, tab, is, io))
	require.NoError(t, commit())
	require.NoError(t, ti.Close(ctx))
}

func CloseCompositeIndexTest(t *testing.T, e *engine.Engine, tab engine.TableEngine, ti engine.IndexEngine, is, ts *schema.Schema, io engine.IndexOptions, to engine.TableOptions) {
	CreateIndex(t, ti, tab, e, io, is)
	ctx, _, commit, abort, err := e.WithTransaction(context.Background())
	defer abort()
	require.NoError(t, err)
	require.NoError(t, ti.Close(ctx))
	require.NoError(t, commit())
}

func DropCompositeIndexTest(t *testing.T, e *engine.Engine, tab engine.TableEngine, ti engine.IndexEngine, is, ts *schema.Schema, io engine.IndexOptions, to engine.TableOptions) {
	CreateIndex(t, ti, tab, e, io, is)
	ctx, _, commit, abort, err := e.WithTransaction(context.Background())
	defer abort()
	require.NoError(t, err)

	dbpath := filepath.Join(e.RootPath(), is.Name()+".db")
	ok, err := store.Exists(io.Driver, dbpath)
	require.NoError(t, err, "access error")
	require.True(t, ok, "db not exists")
	require.NoError(t, ti.Drop(ctx))
	require.NoError(t, commit())
	ok, err = store.Exists(io.Driver, dbpath)
	require.NoError(t, err, "access error")
	require.False(t, ok, "db not deleted")
}

func TruncateCompositeIndexTest(t *testing.T, e *engine.Engine, tab engine.TableEngine, ti engine.IndexEngine, is, ts *schema.Schema, io engine.IndexOptions, to engine.TableOptions) {
	CreateIndex(t, ti, tab, e, io, is)
	ctx, _, _, abort, err := e.WithTransaction(context.Background())
	defer abort()
	require.NoError(t, err)
	require.NoError(t, ti.Truncate(ctx))
}

func RebuildCompositeIndexTest(t *testing.T, e *engine.Engine, tab engine.TableEngine, ti engine.IndexEngine, is, ts *schema.Schema, io engine.IndexOptions, to engine.TableOptions) {
	CreateIndex(t, ti, tab, e, io, is)
	ctx, _, commit, abort, err := e.WithTransaction(context.Background())
	defer abort()
	require.NoError(t, err)
	require.NoError(t, ti.Rebuild(ctx))
	require.NoError(t, commit())
}

func SyncCompositeIndexTest(t *testing.T, e *engine.Engine, tab engine.TableEngine, ti engine.IndexEngine, is, ts *schema.Schema, io engine.IndexOptions, to engine.TableOptions) {
	CreateIndex(t, ti, tab, e, io, is)

	ctx, _, commit, _, _ := e.WithTransaction(context.Background())
	require.NoError(t, ti.Sync(ctx))
	require.NoError(t, commit())

	FillIndex(t, e, ti)

	ctx, _, commit, _, _ = e.WithTransaction(context.Background())
	require.NoError(t, ti.Sync(ctx))
	require.NoError(t, commit())
}

func CanMatchCompositeIndexTest(t *testing.T, e *engine.Engine, tab engine.TableEngine, ti engine.IndexEngine, is, ts *schema.Schema, io engine.IndexOptions, to engine.TableOptions) {
	CreateIndex(t, ti, tab, e, io, is)

	switch to.Engine {
	case engine.TableKindLSM:
		// eq
		require.True(t, ti.CanMatch(makeTree(makeFilter(ts, "i32", EQ, 1, nil))), EQ)
		// le
		require.True(t, ti.CanMatch(makeTree(makeFilter(ts, "i32", LE, 1, nil))), LE)
		// lt
		require.True(t, ti.CanMatch(makeTree(makeFilter(ts, "i32", LT, 1, nil))), LT)
		// ge
		require.True(t, ti.CanMatch(makeTree(makeFilter(ts, "i32", GE, 1, nil))), GE)
		// gt
		require.True(t, ti.CanMatch(makeTree(makeFilter(ts, "i32", GT, 1, nil))), GT)
		// rg
		require.True(t, ti.CanMatch(makeTree(makeFilter(ts, "i32", RG, 1, 2))), RG)
		// complex trees
		require.True(t, ti.CanMatch(makeTree(
			makeFilter(ts, "i64", EQ, 1, nil),
			makeFilter(ts, "i32", RG, 1, 2),
		)), "multi")
		// no other mode
		require.False(t, ti.CanMatch(makeTree(makeFilter(ts, "i32", IN, []int{1}, nil))), IN)
		require.False(t, ti.CanMatch(makeTree(makeFilter(ts, "i32", NI, []int{1}, nil))), NI)
		// no simple filters
		require.False(t, ti.CanMatch(makeFilter(ts, "i32", EQ, 1, nil)), "no simple")
		// no ineligible fields
		require.False(t, ti.CanMatch(makeTree(makeFilter(ts, "u64", EQ, 1, nil))), "non index field")

	case engine.TableKindPack:
		// complex trees
		require.True(t, ti.CanMatch(makeTree(
			makeFilter(ts, "i64", EQ, 1, nil),
			makeFilter(ts, "i32", EQ, 1, nil),
		)), "multi")
		// no sub-selection of fields (because its a hash index)
		require.False(t, ti.CanMatch(makeTree(makeFilter(ts, "i32", EQ, 1, nil))), EQ)
		// no other mode
		require.False(t, ti.CanMatch(makeTree(makeFilter(ts, "i32", LE, 1, nil))), LE)
		require.False(t, ti.CanMatch(makeTree(makeFilter(ts, "i32", LT, 1, nil))), LT)
		require.False(t, ti.CanMatch(makeTree(makeFilter(ts, "i32", GE, 1, nil))), GE)
		require.False(t, ti.CanMatch(makeTree(makeFilter(ts, "i32", GT, 1, nil))), GT)
		require.False(t, ti.CanMatch(makeTree(makeFilter(ts, "i32", IN, []int{1}, nil))), IN)
		require.False(t, ti.CanMatch(makeTree(makeFilter(ts, "i32", NI, []int{1}, nil))), NI)
		require.False(t, ti.CanMatch(makeTree(makeFilter(ts, "i32", RG, 1, 2))), RG)
		// no simple filters
		require.False(t, ti.CanMatch(makeFilter(ts, "i32", EQ, 1, nil)), "no simple")
		// no suffix filters
		require.False(t, ti.CanMatch(makeTree(makeFilter(ts, "i64", EQ, 1, nil))), "no suffix")
		// no ineligible fields
		require.False(t, ti.CanMatch(makeTree(makeFilter(ts, "u64", EQ, 1, nil))), "non index field")

	default:
		require.Fail(t, "no case for testing table engine %s", to.Engine)
	}
}

func AddCompositeIndexTest(t *testing.T, e *engine.Engine, tab engine.TableEngine, ti engine.IndexEngine, is, ts *schema.Schema, io engine.IndexOptions, to engine.TableOptions) {
	CreateIndex(t, ti, tab, e, io, is)
	FillIndex(t, e, ti)

	// need tx to query index
	ctx, _, _, abort, err := e.WithTransaction(context.Background())
	defer abort()
	require.NoError(t, err)

	// query data to confirm it is stored
	res, _, err := ti.QueryComposite(ctx, makeTree(
		makeFilter(ts, "i64", EQ, 1, nil),
		makeFilter(ts, "i32", EQ, 1, nil),
	))
	require.NoError(t, err)
	require.NotNil(t, res)
	require.Equal(t, 1, res.Count())
}

func DeleteCompositeIndexTest(t *testing.T, e *engine.Engine, tab engine.TableEngine, ti engine.IndexEngine, is, ts *schema.Schema, io engine.IndexOptions, to engine.TableOptions) {
	CreateIndex(t, ti, tab, e, io, is)
	prev := FillIndex(t, e, ti)

	// need tx to query index
	ctx, _, _, abort, err := e.WithTransaction(context.Background())
	defer abort()
	require.NoError(t, err)

	q := makeTree(
		makeFilter(ts, "i64", EQ, 5, nil),
		makeFilter(ts, "i32", EQ, 5, nil),
	)

	res, _, err := ti.QueryComposite(ctx, q)
	require.NoError(t, err)
	require.NotNil(t, res)
	require.Equal(t, 1, res.Count())
	abort()

	// delete last item stored
	ctx, _, commit, _, _ := e.WithTransaction(context.Background())
	require.NoError(t, ti.DelPack(ctx, prev, pack.WriteModeAll))
	require.NoError(t, ti.Sync(ctx))
	require.NoError(t, commit())

	// query again
	ctx, _, _, abort, err = e.WithTransaction(context.Background())
	defer abort()
	require.NoError(t, err)

	// check 2: confirm item is removed
	res, _, err = ti.QueryComposite(ctx, q)
	require.NoError(t, err)
	require.NotNil(t, res)
	require.Equal(t, 0, res.Count())
}

func QueryCompositeIndexTest(t *testing.T, e *engine.Engine, tab engine.TableEngine, ti engine.IndexEngine, is, ts *schema.Schema, io engine.IndexOptions, to engine.TableOptions) {
	CreateIndex(t, ti, tab, e, io, is)
	FillIndex(t, e, ti)

	// need tx to query index
	ctx, _, _, abort, err := e.WithTransaction(context.Background())
	defer abort()
	require.NoError(t, err)

	switch to.Engine {
	case engine.TableKindLSM:
		// le
		q := makeTree(makeFilter(ts, "i32", LE, 6, nil))
		res, _, err := ti.QueryComposite(ctx, q)
		require.NoError(t, err)
		require.NotNil(t, res)
		require.Equal(t, 6, res.Count())

	case engine.TableKindPack:
		// complex trees
		q := makeTree(
			makeFilter(ts, "i64", EQ, 1, nil),
			makeFilter(ts, "i32", EQ, 1, nil),
		)
		res, _, err := ti.QueryComposite(ctx, q)
		require.NoError(t, err)
		require.NotNil(t, res)
		require.Equal(t, 1, res.Count())

	default:
		require.Fail(t, "no case for testing table engine %s", to.Engine)
	}
}

func IsCompositeIndexTest(t *testing.T, e *engine.Engine, tab engine.TableEngine, ti engine.IndexEngine, si *schema.Schema, st *schema.Schema, io engine.IndexOptions, to engine.TableOptions) {
	t.Helper()

	if io.Type != types.IndexTypeComposite {
		// create index
		CreateIndex(t, ti, tab, e, io, si)
		// check is false
		require.False(t, ti.IsComposite())
	} else {
		// create composite index index
		cs, err := st.SelectFields("i64", "i32", "id")
		require.NoError(t, err)

		CreateIndex(t, ti, tab, e, io, cs)

		// check is true
		require.True(t, ti.IsComposite())
	}
}
