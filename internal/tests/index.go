package tests

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"

	"blockwatch.cc/knoxdb/internal/engine"
	"blockwatch.cc/knoxdb/internal/query"
	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/schema"
	"github.com/echa/log"
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

func TestIndexEngine[T any, F IF[T]](t *testing.T, driver, eng string, tableEngine engine.TableEngine, ityps []types.IndexType) {
	t.Helper()
	for _, c := range IndexTestCases {
		for _, indexType := range ityps {
			t.Run(fmt.Sprintf("%s/%s", c.Name, indexType), func(t *testing.T) {
				t.Helper()

				ctx := context.Background()

				dopts := NewTestDatabaseOptions(t, driver)

				e := NewTestEngine(t, dopts)
				defer e.Close(ctx)

				var indexEngine F = new(T)
				topts := NewTestTableOptions(t, driver, eng)

				// create table
				CreateEnum(t, e)
				CreateTable(t, e, tableEngine, topts, allTypesSchema)

				// insert data table
				ctx, _, commit, abort, err := e.WithTransaction(context.Background())
				defer abort()
				require.NoError(t, err)
				require.NoError(t, tableEngine.Open(ctx, allTypesSchema, topts))
				InsertData(t, ctx, tableEngine, allTypesSchema)

				// commit
				require.NoError(t, commit())

				iopts := NewTestIndexOptions(t, driver, eng, indexType)
				indexSchema, err := allTypesSchema.SelectFields("u64", "id")
				require.NoError(t, err)

				if testing.Verbose() {
					iopts.Logger = log.Log.SetLevel(log.LevelDebug)
				}

				c.Run(t, e, tableEngine, indexEngine, indexSchema, allTypesSchema, iopts, topts)

				require.NoError(t, tableEngine.Close(ctx))
			})
		}
	}
}

func CreateIndex(t *testing.T, idxEngine engine.IndexEngine, tab engine.TableEngine, e *engine.Engine, idxOpts engine.IndexOptions, s *engine.Schema) {
	t.Helper()
	ctx, _, commit, abort, err := e.WithTransaction(context.Background())
	defer abort()
	require.NoError(t, err)

	err = idxEngine.Create(ctx, tab, s, idxOpts)
	require.NoError(t, err)
	require.NoError(t, commit())
	tab.UseIndex(idxEngine)
}

func FillIndex(t *testing.T, ti engine.IndexEngine, ts *schema.Schema) []byte {
	t.Helper()
	ctx := context.Background()
	enc := schema.NewEncoder(ts)
	var last []byte
	for i := range 6 {
		allType := NewAllTypes(i)
		allType.Id = uint64(i + 1)
		buf, err := enc.Encode(allType, nil)
		require.NoError(t, err)
		require.NoError(t, ti.Add(ctx, nil, buf))
		last = buf
	}
	require.NoError(t, ti.Sync(ctx))
	return last
}

func QueryIndex(t *testing.T, ctx context.Context, idx engine.IndexEngine, f *query.FilterTreeNode, cnt int) {
	t.Helper()
	res, _, err := idx.Query(ctx, f)
	require.NoError(t, err)
	require.NotNil(t, res, "nil result bitmap")
	require.Equal(t, cnt, res.Count())
}

func QueryIndexFail(t *testing.T, ctx context.Context, idx engine.IndexEngine, f *query.FilterTreeNode, cnt int) {
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
	ctx, _, commit, abort, err := e.WithTransaction(context.Background())
	defer abort()
	require.NoError(t, err)
	require.NoError(t, ti.Open(ctx, tab, is, io))
	require.NoError(t, commit())
	require.NoError(t, ti.Close(ctx))
}

func CloseIndexTest(t *testing.T, e *engine.Engine, tab engine.TableEngine, ti engine.IndexEngine, is, ts *schema.Schema, io engine.IndexOptions, to engine.TableOptions) {
	CreateIndex(t, ti, tab, e, io, is)
	ctx, _, commit, abort, err := e.WithTransaction(context.Background())
	defer abort()
	require.NoError(t, err)
	require.NoError(t, ti.Close(ctx))
	require.NoError(t, commit())
}

func DropIndexTest(t *testing.T, e *engine.Engine, tab engine.TableEngine, ti engine.IndexEngine, is, ts *schema.Schema, io engine.IndexOptions, to engine.TableOptions) {
	CreateIndex(t, ti, tab, e, io, is)

	ctx, _, _, abort, err := e.WithTransaction(context.Background())
	defer abort()
	require.NoError(t, err)

	isBadger := io.Driver == "badger"
	if !isBadger {
		require.FileExists(t, filepath.Join(e.Options().Path, "testdb", ti.Schema().Name()+".db"))
		require.NoError(t, ti.Drop(ctx))
		require.NoFileExists(t, filepath.Join(e.Options().Path, "testdb", ti.Schema().Name()+".db"))
	} else {
		require.DirExists(t, filepath.Join(e.Options().Path, "testdb", ti.Schema().Name()+".db"))
		require.NoError(t, ti.Drop(ctx))
		require.NoDirExists(t, filepath.Join(e.Options().Path, "testdb", ti.Schema().Name()+".db"))
	}
}

func TruncateIndexTest(t *testing.T, e *engine.Engine, tab engine.TableEngine, ti engine.IndexEngine, is, ts *schema.Schema, io engine.IndexOptions, to engine.TableOptions) {
	CreateIndex(t, ti, tab, e, io, is)
	FillIndex(t, ti, ts)
	require.NoError(t, ti.Truncate(context.Background()))
}

func RebuildIndexTest(t *testing.T, e *engine.Engine, tab engine.TableEngine, ti engine.IndexEngine, is, ts *schema.Schema, io engine.IndexOptions, to engine.TableOptions) {
	CreateIndex(t, ti, tab, e, io, is)
	ctx, _, commit, abort, err := e.WithTransaction(context.Background())
	defer abort()
	require.NoError(t, err)
	require.NoError(t, ti.Rebuild(ctx))
	require.NoError(t, commit())
}

func SyncIndexTest(t *testing.T, e *engine.Engine, tab engine.TableEngine, ti engine.IndexEngine, is, ts *schema.Schema, io engine.IndexOptions, to engine.TableOptions) {
	ctx := context.Background()
	CreateIndex(t, ti, tab, e, io, is)
	require.NoError(t, ti.Sync(ctx))
	FillIndex(t, ti, ts)
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

	case types.IndexTypeInt:
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
	FillIndex(t, ti, ts)

	// need tx to query index
	ctx, _, _, abort, err := e.WithTransaction(context.Background())
	defer abort()
	require.NoError(t, err)

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
	prev := FillIndex(t, ti, ts)

	// need tx to query index
	ctx, _, _, abort, err := e.WithTransaction(context.Background())
	defer abort()
	require.NoError(t, err)

	switch io.Type {
	case types.IndexTypeHash:
		QueryIndex(t, ctx, ti, makeFilter(ts, "u64", EQ, 5, nil), 1)

	case types.IndexTypeInt:
		QueryIndex(t, ctx, ti, makeFilter(ts, "u64", LT, 6, nil), 6)

	default:
		require.Fail(t, "no case for testing index type %s", io.Type)
	}
	abort()

	// delete last item and store
	require.NoError(t, ti.Del(ctx, prev))
	require.NoError(t, ti.Sync(ctx))

	// query again
	ctx, _, _, abort, err = e.WithTransaction(context.Background())
	defer abort()
	require.NoError(t, err)

	// confirm item is removed
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
	FillIndex(t, ti, ts)

	ctx, _, _, abort, err := e.WithTransaction(context.Background())
	defer abort()
	require.NoError(t, err)

	// query by type
	switch io.Type {
	case types.IndexTypeHash:
		// // eq
		// QueryIndex(t, ctx, ti, makeFilter(ts, "u64", EQ, 1, nil), 1)
		// // in
		// QueryIndex(t, ctx, ti, makeFilter(ts, "u64", IN, []int{1, 2}, nil), 2)
	case types.IndexTypeInt:
		// // eq
		// QueryIndex(t, ctx, ti, makeFilter(ts, "u64", EQ, 1, nil), 1)
		// // le
		// QueryIndex(t, ctx, ti, makeFilter(ts, "u64", LE, 1, nil), 2)
		// // lt
		// QueryIndex(t, ctx, ti, makeFilter(ts, "u64", LT, 1, nil), 1)
		// // ge
		// QueryIndex(t, ctx, ti, makeFilter(ts, "u64", GE, 1, nil), 5)
		// // gt
		// QueryIndex(t, ctx, ti, makeFilter(ts, "u64", GT, 1, nil), 4)
		// rg
		QueryIndex(t, ctx, ti, makeFilter(ts, "u64", RG, 1, 2), 2)
	default:
		require.Fail(t, "no case for testing index type %s", io.Type)
	}
}
