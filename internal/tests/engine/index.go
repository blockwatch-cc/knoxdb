// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package engine_tests

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"

	"blockwatch.cc/knoxdb/internal/engine"
	"blockwatch.cc/knoxdb/internal/operator/filter"
	"blockwatch.cc/knoxdb/internal/pack"
	"blockwatch.cc/knoxdb/internal/store"
	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/schema"
	"github.com/stretchr/testify/require"
)

type IndexTestCase struct {
	Name string
	Run  func(*testing.T, *engine.Engine, engine.TableEngine, engine.IndexEngine, *schema.Schema, *schema.Schema, engine.IndexOptions, engine.TableOptions)
}

type IF[T any] interface {
	*T
	engine.IndexEngine
}

var IndexTestCases = []IndexTestCase{
	{
		Name: "Create",
		Run:  CreateIndexTest,
	},
	{
		Name: "Open",
		Run:  OpenIndexTest,
	},
	{
		Name: "Drop",
		Run:  DropIndexTest,
	},
	{
		Name: "Truncate",
		Run:  TruncateIndexTest,
	},
	{
		Name: "Rebuild",
		Run:  RebuildIndexTest,
	},
	{
		Name: "Add",
		Run:  AddIndexTest,
	},
	{
		Name: "Del",
		Run:  DeleteIndexTest,
	},
	{
		Name: "CanMatch",
		Run:  CanMatchIndexTest,
	},
	{
		Name: "Query",
		Run:  QueryIndexTest,
	},
	{
		Name: "Sync",
		Run:  SyncIndexTest,
	},
	{
		Name: "Close",
		Run:  CloseIndexTest,
	},
}

func TestIndexEngine[T any, F IF[T]](t *testing.T, driver, eng string, table engine.TableEngine, ityps []types.IndexType) {
	t.Helper()
	for _, c := range IndexTestCases {
		for _, indexType := range ityps {
			t.Run(fmt.Sprintf("%s/%s/%s", c.Name, driver, indexType), func(t *testing.T) {
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
				ts := table.Schema() // table has added metadata

				iopts := NewTestIndexOptions(t, driver, eng, indexType)
				indexSchema, err := ts.SelectFields("u64", "$rid")
				require.NoError(t, err)

				var indexEngine F = new(T)
				c.Run(t, e, table, indexEngine, indexSchema, ts, iopts, topts)
			})
		}
	}
}

func CreateIndex(t *testing.T, idxEngine engine.IndexEngine, tab engine.TableEngine, e *engine.Engine, idxOpts engine.IndexOptions, s *schema.Schema) {
	t.Helper()
	ctx := engine.WithEngine(context.Background(), e)
	require.NoError(t, idxEngine.Create(ctx, tab, s, idxOpts))
	tab.ConnectIndex(idxEngine)
}

func FillIndex(t *testing.T, e *engine.Engine, ti engine.IndexEngine) *pack.Package {
	t.Helper()
	ctx := engine.WithEngine(context.Background(), e)
	enc := schema.NewEncoder(ti.Table().Schema())
	pkg := pack.New().WithSchema(ti.Table().Schema()).WithMaxRows(1 << 11).Alloc()
	meta := &schema.Meta{}
	for i := range 6 {
		allType := NewAllTypes(i)
		allType.Id = uint64(i + 1)
		meta.Rid = uint64(i + 1)
		buf, err := enc.Encode(allType, nil)
		require.NoError(t, err)
		pkg.AppendWire(buf, meta)
	}
	require.NoError(t, ti.AddPack(ctx, pkg, pack.WriteModeAll))
	require.NoError(t, ti.Finalize(ctx, 1))

	// return a package with just the last row (for delete tests)
	pkg.Delete(0, 5)
	return pkg
}

func QueryIndex(t *testing.T, ctx context.Context, idx engine.IndexEngine, f *filter.Node, cnt int) {
	t.Helper()
	res, _, err := idx.Query(ctx, f)
	require.NoError(t, err)
	require.NotNil(t, res, "nil result bitmap")
	require.Equal(t, cnt, res.Count())
}

func QueryIndexFail(t *testing.T, ctx context.Context, idx engine.IndexEngine, f *filter.Node, cnt int) {
	t.Helper()
	res, _, err := idx.Query(ctx, f)
	require.Error(t, err)
	require.Nil(t, res, "nil result bitmap")
}

func CreateIndexTest(t *testing.T, e *engine.Engine, tab engine.TableEngine, ti engine.IndexEngine, is, ts *schema.Schema, io engine.IndexOptions, to engine.TableOptions) {
	CreateIndex(t, ti, tab, e, io, is)
}

func OpenIndexTest(t *testing.T, e *engine.Engine, tab engine.TableEngine, ti engine.IndexEngine, is, ts *schema.Schema, io engine.IndexOptions, to engine.TableOptions) {
	CreateIndex(t, ti, tab, e, io, is)
	require.NoError(t, ti.Close(context.Background()))
	ctx := engine.WithEngine(context.Background(), e)
	require.NoError(t, ti.Open(ctx, tab, is, io))
	require.NoError(t, ti.Close(ctx))
}

func CloseIndexTest(t *testing.T, e *engine.Engine, tab engine.TableEngine, ti engine.IndexEngine, is, ts *schema.Schema, io engine.IndexOptions, to engine.TableOptions) {
	CreateIndex(t, ti, tab, e, io, is)
	ctx := engine.WithEngine(context.Background(), e)
	require.NoError(t, ti.Close(ctx))
}

func DropIndexTest(t *testing.T, e *engine.Engine, tab engine.TableEngine, ti engine.IndexEngine, is, ts *schema.Schema, io engine.IndexOptions, to engine.TableOptions) {
	CreateIndex(t, ti, tab, e, io, is)
	ctx := engine.WithEngine(context.Background(), e)
	dbpath := filepath.Join(e.RootPath(), is.Name()+".db")
	ok, err := store.Exists(io.Driver, dbpath)
	require.NoError(t, err, "access error")
	require.True(t, ok, "db not exists")
	require.NoError(t, ti.Drop(ctx))
	ok, err = store.Exists(io.Driver, dbpath)
	require.NoError(t, err, "access error")
	require.False(t, ok, "db not deleted")
}

func TruncateIndexTest(t *testing.T, e *engine.Engine, tab engine.TableEngine, ti engine.IndexEngine, is, ts *schema.Schema, io engine.IndexOptions, to engine.TableOptions) {
	CreateIndex(t, ti, tab, e, io, is)
	FillIndex(t, e, ti)
	ctx := engine.WithEngine(context.Background(), e)
	require.NoError(t, ti.Truncate(ctx))
}

func RebuildIndexTest(t *testing.T, e *engine.Engine, tab engine.TableEngine, ti engine.IndexEngine, is, ts *schema.Schema, io engine.IndexOptions, to engine.TableOptions) {
	CreateIndex(t, ti, tab, e, io, is)
	ctx := engine.WithEngine(context.Background(), e)
	require.NoError(t, ti.Rebuild(ctx))
}

func SyncIndexTest(t *testing.T, e *engine.Engine, tab engine.TableEngine, ti engine.IndexEngine, is, ts *schema.Schema, io engine.IndexOptions, to engine.TableOptions) {
	CreateIndex(t, ti, tab, e, io, is)
	ctx := engine.WithEngine(context.Background(), e)
	require.NoError(t, ti.Sync(ctx))
	FillIndex(t, e, ti)
	require.NoError(t, ti.Sync(ctx))
}

func CanMatchIndexTest(t *testing.T, e *engine.Engine, tab engine.TableEngine, ti engine.IndexEngine, is, ts *schema.Schema, io engine.IndexOptions, to engine.TableOptions) {
	CreateIndex(t, ti, tab, e, io, is)

	// check by type
	switch io.Type {
	case types.IndexTypeHash:
		// eq
		require.True(t, ti.CanMatch(makeFilter(ts, "u64", EQ, 1, nil)), EQ)
		// in
		require.True(t, ti.CanMatch(makeFilter(ts, "u64", IN, []int{1, 2}, nil)), IN)
		// no other mode
		require.False(t, ti.CanMatch(makeFilter(ts, "u64", LE, 1, nil)), LE)
		require.False(t, ti.CanMatch(makeFilter(ts, "u64", LT, 1, nil)), LT)
		require.False(t, ti.CanMatch(makeFilter(ts, "u64", GE, 1, nil)), GE)
		require.False(t, ti.CanMatch(makeFilter(ts, "u64", GT, 1, nil)), GT)
		require.False(t, ti.CanMatch(makeFilter(ts, "u64", RG, 1, 2)), RG)
		// no trees
		require.False(t, ti.CanMatch(makeTree(
			makeFilter(ts, "u64", EQ, 1, nil),
			makeFilter(ts, "u32", EQ, 2, nil),
		)), "no multi")
		// no ineligible fields
		require.False(t, ti.CanMatch(makeFilter(ts, "i32", EQ, 1, nil)), "non index field")
		require.False(t, ti.CanMatch(makeFilter(ts, "u64", NI, []int{1, 2}, nil)), NI)

	case types.IndexTypeInt, types.IndexTypePk:
		// eq
		require.True(t, ti.CanMatch(makeFilter(ts, "u64", EQ, 1, nil)), EQ)
		// le
		require.True(t, ti.CanMatch(makeFilter(ts, "u64", LE, 1, nil)), LE)
		// lt
		require.True(t, ti.CanMatch(makeFilter(ts, "u64", LT, 1, nil)), LT)
		// ge
		require.True(t, ti.CanMatch(makeFilter(ts, "u64", GE, 1, nil)), GE)
		// gt
		require.True(t, ti.CanMatch(makeFilter(ts, "u64", GT, 1, nil)), GT)
		// rg
		require.True(t, ti.CanMatch(makeFilter(ts, "u64", RG, 1, 2)), RG)
		// no other mode
		require.False(t, ti.CanMatch(makeFilter(ts, "u64", IN, []int{1, 2}, nil)), IN)
		require.False(t, ti.CanMatch(makeFilter(ts, "u64", NI, []int{1, 2}, nil)), NI)
		// no trees
		require.False(t, ti.CanMatch(makeTree(
			makeFilter(ts, "u64", EQ, 1, nil),
			makeFilter(ts, "u32", EQ, 2, nil),
		)), "no multi")
		// no ineligible fields
		require.False(t, ti.CanMatch(makeFilter(ts, "i32", EQ, 1, nil)), "non index field")
	default:
		require.Fail(t, "no case for testing index type %s", io.Type)
	}
}

func AddIndexTest(t *testing.T, e *engine.Engine, tab engine.TableEngine, ti engine.IndexEngine, is, ts *schema.Schema, io engine.IndexOptions, to engine.TableOptions) {
	CreateIndex(t, ti, tab, e, io, is)
	FillIndex(t, e, ti)
	ctx := engine.WithEngine(context.Background(), e)

	// query data to confirm it is stored
	switch io.Type {
	case types.IndexTypeHash:
		QueryIndex(t, ctx, ti, makeFilter(ts, "u64", EQ, 5, nil), 1)
		QueryIndex(t, ctx, ti, makeFilter(ts, "u64", EQ, 15, nil), 0)

	case types.IndexTypeInt:
		QueryIndex(t, ctx, ti, makeFilter(ts, "u64", LT, 6, nil), 6)
		QueryIndex(t, ctx, ti, makeFilter(ts, "u64", GT, 15, nil), 0)

	default:
		require.Fail(t, "no case for testing index type %s", io.Type)
	}
}

func DeleteIndexTest(t *testing.T, e *engine.Engine, tab engine.TableEngine, ti engine.IndexEngine, is, ts *schema.Schema, io engine.IndexOptions, to engine.TableOptions) {
	CreateIndex(t, ti, tab, e, io, is)
	prev := FillIndex(t, e, ti)
	ctx := engine.WithEngine(context.Background(), e)

	switch io.Type {
	case types.IndexTypeHash:
		QueryIndex(t, ctx, ti, makeFilter(ts, "u64", EQ, 5, nil), 1)

	case types.IndexTypeInt:
		QueryIndex(t, ctx, ti, makeFilter(ts, "u64", LT, 6, nil), 6)

	default:
		require.Fail(t, "no case for testing index type %s", io.Type)
	}

	// delete last item and store
	require.NoError(t, ti.DelPack(ctx, prev, pack.WriteModeAll, 0))
	require.NoError(t, ti.Finalize(ctx, 1))
	require.NoError(t, ti.GC(ctx, 1))

	// query again, confirm item is removed
	switch io.Type {
	case types.IndexTypeHash:
		QueryIndex(t, ctx, ti, makeFilter(ts, "u64", EQ, 5, nil), 0)

	case types.IndexTypeInt:
		QueryIndex(t, ctx, ti, makeFilter(ts, "u64", LT, 6, nil), 5)

	default:
		require.Fail(t, "no case for testing index type %s", io.Type)
	}
}

func QueryIndexTest(t *testing.T, e *engine.Engine, tab engine.TableEngine, ti engine.IndexEngine, is, ts *schema.Schema, io engine.IndexOptions, to engine.TableOptions) {
	CreateIndex(t, ti, tab, e, io, is)
	FillIndex(t, e, ti)
	ctx := engine.WithEngine(context.Background(), e)

	// query by type
	switch io.Type {
	case types.IndexTypeHash:
		// eq
		QueryIndex(t, ctx, ti, makeFilter(ts, "u64", EQ, 1, nil), 1)
		// in
		QueryIndex(t, ctx, ti, makeFilter(ts, "u64", IN, []int{1, 2}, nil), 2)
	case types.IndexTypeInt:
		// eq
		QueryIndex(t, ctx, ti, makeFilter(ts, "u64", EQ, 1, nil), 1)
		// le
		QueryIndex(t, ctx, ti, makeFilter(ts, "u64", LE, 1, nil), 2)
		// lt
		QueryIndex(t, ctx, ti, makeFilter(ts, "u64", LT, 1, nil), 1)
		// ge
		QueryIndex(t, ctx, ti, makeFilter(ts, "u64", GE, 1, nil), 5)
		// gt
		QueryIndex(t, ctx, ti, makeFilter(ts, "u64", GT, 1, nil), 4)
		// rg
		QueryIndex(t, ctx, ti, makeFilter(ts, "u64", RG, 1, 2), 2)
	default:
		require.Fail(t, "no case for testing index type %s", io.Type)
	}
}
