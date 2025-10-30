// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package engine_tests

import (
	"context"
	"path/filepath"
	"testing"

	"blockwatch.cc/knoxdb/internal/engine"
	"blockwatch.cc/knoxdb/internal/pack"
	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/schema"
	"blockwatch.cc/knoxdb/pkg/store"
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
			ts := table.Schema()

			iopts := NewTestIndexOptions(t, driver, eng)
			ss, err := ts.Select("i32", "i64")
			require.NoError(t, err)
			indexSchema := &schema.IndexSchema{
				Name:   "test_index",
				Type:   types.IndexTypeComposite,
				Base:   ts,
				Fields: ss.Fields,
			}

			var indexEngine F = new(T)
			c.Run(t, e, table, ts, topts, indexEngine, indexSchema, iopts)
		})
	}
}

func CreateCompositeIndexTest(t *testing.T, e *engine.Engine, te engine.TableEngine, ts *schema.Schema, to engine.TableOptions, ie engine.IndexEngine, is *schema.IndexSchema, io engine.IndexOptions) {
	CreateIndex(t, e, te, ie, is, io)
}

func OpenCompositeIndexTest(t *testing.T, e *engine.Engine, te engine.TableEngine, ts *schema.Schema, to engine.TableOptions, ie engine.IndexEngine, is *schema.IndexSchema, io engine.IndexOptions) {
	ctx := context.Background()
	CreateIndex(t, e, te, ie, is, io)
	require.NoError(t, ie.Close(ctx))
	ctx = engine.WithEngine(ctx, e)
	require.NoError(t, ie.Open(ctx, te, is, io))
	require.NoError(t, ie.Close(ctx))
}

func CloseCompositeIndexTest(t *testing.T, e *engine.Engine, te engine.TableEngine, ts *schema.Schema, to engine.TableOptions, ie engine.IndexEngine, is *schema.IndexSchema, io engine.IndexOptions) {
	CreateIndex(t, e, te, ie, is, io)
	ctx := engine.WithEngine(context.Background(), e)
	require.NoError(t, ie.Close(ctx))
}

func DropCompositeIndexTest(t *testing.T, e *engine.Engine, te engine.TableEngine, ts *schema.Schema, to engine.TableOptions, ie engine.IndexEngine, is *schema.IndexSchema, io engine.IndexOptions) {
	CreateIndex(t, e, te, ie, is, io)
	ctx := engine.WithEngine(context.Background(), e)

	dbpath := filepath.Join(e.RootPath(), is.Name)
	ok, err := store.Exists(io.Driver, dbpath)
	require.NoError(t, err, "access error")
	require.True(t, ok, "db not exists")
	require.NoError(t, ie.Drop(ctx))
	ok, err = store.Exists(io.Driver, dbpath)
	require.NoError(t, err, "access error")
	require.False(t, ok, "db not deleted")
}

func TruncateCompositeIndexTest(t *testing.T, e *engine.Engine, te engine.TableEngine, ts *schema.Schema, to engine.TableOptions, ie engine.IndexEngine, is *schema.IndexSchema, io engine.IndexOptions) {
	CreateIndex(t, e, te, ie, is, io)
	ctx := engine.WithEngine(context.Background(), e)
	require.NoError(t, ie.Truncate(ctx))
}

func RebuildCompositeIndexTest(t *testing.T, e *engine.Engine, te engine.TableEngine, ts *schema.Schema, to engine.TableOptions, ie engine.IndexEngine, is *schema.IndexSchema, io engine.IndexOptions) {
	CreateIndex(t, e, te, ie, is, io)
	ctx := engine.WithEngine(context.Background(), e)
	require.NoError(t, ie.Rebuild(ctx))
}

func SyncCompositeIndexTest(t *testing.T, e *engine.Engine, te engine.TableEngine, ts *schema.Schema, to engine.TableOptions, ie engine.IndexEngine, is *schema.IndexSchema, io engine.IndexOptions) {
	CreateIndex(t, e, te, ie, is, io)
	ctx := engine.WithEngine(context.Background(), e)
	require.NoError(t, ie.Sync(ctx))
	FillIndex(t, e, ie)
	require.NoError(t, ie.Sync(ctx))
}

func CanMatchCompositeIndexTest(t *testing.T, e *engine.Engine, te engine.TableEngine, ts *schema.Schema, to engine.TableOptions, ie engine.IndexEngine, is *schema.IndexSchema, io engine.IndexOptions) {
	CreateIndex(t, e, te, ie, is, io)

	switch to.Engine {
	case engine.TableKindLSM:
		// eq
		require.True(t, ie.CanMatch(makeTree(makeFilter(ts, "i32", EQ, 1, nil))), EQ)
		// le
		require.True(t, ie.CanMatch(makeTree(makeFilter(ts, "i32", LE, 1, nil))), LE)
		// lt
		require.True(t, ie.CanMatch(makeTree(makeFilter(ts, "i32", LT, 1, nil))), LT)
		// ge
		require.True(t, ie.CanMatch(makeTree(makeFilter(ts, "i32", GE, 1, nil))), GE)
		// gt
		require.True(t, ie.CanMatch(makeTree(makeFilter(ts, "i32", GT, 1, nil))), GT)
		// rg
		require.True(t, ie.CanMatch(makeTree(makeFilter(ts, "i32", RG, 1, 2))), RG)
		// complex trees
		require.True(t, ie.CanMatch(makeTree(
			makeFilter(ts, "i64", EQ, 1, nil),
			makeFilter(ts, "i32", RG, 1, 2),
		)), "multi")
		// no other mode
		require.False(t, ie.CanMatch(makeTree(makeFilter(ts, "i32", IN, []int{1}, nil))), IN)
		require.False(t, ie.CanMatch(makeTree(makeFilter(ts, "i32", NI, []int{1}, nil))), NI)
		// no simple filters
		require.False(t, ie.CanMatch(makeFilter(ts, "i32", EQ, 1, nil)), "no simple")
		// no ineligible fields
		require.False(t, ie.CanMatch(makeTree(makeFilter(ts, "u64", EQ, 1, nil))), "non index field")

	case engine.TableKindPack:
		// complex trees
		require.True(t, ie.CanMatch(makeTree(
			makeFilter(ts, "i64", EQ, 1, nil),
			makeFilter(ts, "i32", EQ, 1, nil),
		)), "multi")
		// no sub-selection of fields (because its a hash index)
		require.False(t, ie.CanMatch(makeTree(makeFilter(ts, "i32", EQ, 1, nil))), EQ)
		// no other mode
		require.False(t, ie.CanMatch(makeTree(makeFilter(ts, "i32", LE, 1, nil))), LE)
		require.False(t, ie.CanMatch(makeTree(makeFilter(ts, "i32", LT, 1, nil))), LT)
		require.False(t, ie.CanMatch(makeTree(makeFilter(ts, "i32", GE, 1, nil))), GE)
		require.False(t, ie.CanMatch(makeTree(makeFilter(ts, "i32", GT, 1, nil))), GT)
		require.False(t, ie.CanMatch(makeTree(makeFilter(ts, "i32", IN, []int{1}, nil))), IN)
		require.False(t, ie.CanMatch(makeTree(makeFilter(ts, "i32", NI, []int{1}, nil))), NI)
		require.False(t, ie.CanMatch(makeTree(makeFilter(ts, "i32", RG, 1, 2))), RG)
		// no simple filters
		require.False(t, ie.CanMatch(makeFilter(ts, "i32", EQ, 1, nil)), "no simple")
		// no suffix filters
		require.False(t, ie.CanMatch(makeTree(makeFilter(ts, "i64", EQ, 1, nil))), "no suffix")
		// no ineligible fields
		require.False(t, ie.CanMatch(makeTree(makeFilter(ts, "u64", EQ, 1, nil))), "non index field")

	default:
		require.Fail(t, "no case for testing table engine %s", to.Engine)
	}
}

func AddCompositeIndexTest(t *testing.T, e *engine.Engine, te engine.TableEngine, ts *schema.Schema, to engine.TableOptions, ie engine.IndexEngine, is *schema.IndexSchema, io engine.IndexOptions) {
	CreateIndex(t, e, te, ie, is, io)
	FillIndex(t, e, ie)
	ctx := engine.WithEngine(context.Background(), e)

	// query data to confirm it is stored
	res, _, err := ie.QueryComposite(ctx, makeTree(
		makeFilter(ts, "i64", EQ, 1, nil),
		makeFilter(ts, "i32", EQ, 1, nil),
	))
	require.NoError(t, err)
	require.NotNil(t, res)
	require.Equal(t, 1, res.Count())
}

func DeleteCompositeIndexTest(t *testing.T, e *engine.Engine, te engine.TableEngine, ts *schema.Schema, to engine.TableOptions, ie engine.IndexEngine, is *schema.IndexSchema, io engine.IndexOptions) {
	CreateIndex(t, e, te, ie, is, io)
	prev := FillIndex(t, e, ie)
	ctx := engine.WithEngine(context.Background(), e)

	q := makeTree(
		makeFilter(ts, "i64", EQ, 5, nil),
		makeFilter(ts, "i32", EQ, 5, nil),
	)

	res, _, err := ie.QueryComposite(ctx, q)
	require.NoError(t, err)
	require.NotNil(t, res)
	require.Equal(t, 1, res.Count())

	// delete last item stored
	require.NoError(t, ie.DelPack(ctx, prev, pack.WriteModeAll, 0))
	require.NoError(t, ie.Finalize(ctx, 1))
	require.NoError(t, ie.GC(ctx, 1))

	// query again
	// check 2: confirm item is removed
	res, _, err = ie.QueryComposite(ctx, q)
	require.NoError(t, err)
	require.NotNil(t, res)
	require.Equal(t, 0, res.Count())
}

func QueryCompositeIndexTest(t *testing.T, e *engine.Engine, te engine.TableEngine, ts *schema.Schema, to engine.TableOptions, ie engine.IndexEngine, is *schema.IndexSchema, io engine.IndexOptions) {
	CreateIndex(t, e, te, ie, is, io)
	FillIndex(t, e, ie)
	ctx := engine.WithEngine(context.Background(), e)

	switch to.Engine {
	case engine.TableKindLSM:
		// le
		q := makeTree(makeFilter(ts, "i32", LE, 6, nil))
		res, _, err := ie.QueryComposite(ctx, q)
		require.NoError(t, err)
		require.NotNil(t, res)
		require.Equal(t, 6, res.Count())

	case engine.TableKindPack:
		// complex trees
		q := makeTree(
			makeFilter(ts, "i64", EQ, 1, nil),
			makeFilter(ts, "i32", EQ, 1, nil),
		)
		res, _, err := ie.QueryComposite(ctx, q)
		require.NoError(t, err)
		require.NotNil(t, res)
		require.Equal(t, 1, res.Count())

	default:
		require.Fail(t, "no case for testing table engine %s", to.Engine)
	}
}

func IsCompositeIndexTest(t *testing.T, e *engine.Engine, te engine.TableEngine, ts *schema.Schema, to engine.TableOptions, ie engine.IndexEngine, is *schema.IndexSchema, io engine.IndexOptions) {
	t.Helper()

	if is.Type != types.IndexTypeComposite {
		// create index
		CreateIndex(t, e, te, ie, is, io)
		// check is false
		require.False(t, ie.IsComposite())
	} else {
		// create composite index index
		ss, err := ts.Select("i64", "i32")
		require.NoError(t, err)
		is.Fields = ss.Fields

		CreateIndex(t, e, te, ie, is, io)

		// check is true
		require.True(t, ie.IsComposite())
	}
}
