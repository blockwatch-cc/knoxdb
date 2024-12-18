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

type I[T any] interface {
	*T
	engine.IndexEngine
}

var IndexTestCases = []IndexTestCase{
	{
		Name: "Create",
		Run:  CreateIndexEnginefunc,
	},
	{
		Name: "Open",
		Run:  OpenIndexEnginefunc,
	},
	{
		Name: "Drop",
		Run:  DropIndexEnginefunc,
	},
	{
		Name: "Truncate",
		Run:  TruncateIndexEnginefunc,
	},
	{
		Name: "Rebuild",
		Run:  RebuildIndexEnginefunc,
	},
	{
		Name: "Add",
		Run:  AddIndexEnginefunc,
	},
	{
		Name: "Del",
		Run:  DeleteIndexEnginefunc,
	},
	{
		Name: "CanMatch",
		Run:  CanMatchIndexEnginefunc,
	},
	{
		Name: "Query",
		Run:  QueryIndexEnginefunc,
	},
	{
		Name: "Sync",
		Run:  SyncIndexEnginefunc,
	},
	{
		Name: "Close",
		Run:  CloseIndexEnginefunc,
	},
}

func TestIndexEngine[T any, B I[T]](t *testing.T, driver, eng string, tableEngine engine.TableEngine, supportedTypes []types.IndexType) {
	t.Helper()
	for _, c := range IndexTestCases {
		for _, indexType := range supportedTypes {
			t.Run(fmt.Sprintf("%s/%s", c.Name, indexType), func(t *testing.T) {
				t.Helper()

				ctx := context.Background()
				s := schema.MustSchemaOf(AllTypes{})
				dopts := NewTestDatabaseOptions(t, driver)

				e := NewTestEngine(t, dopts)
				defer e.Close(ctx)

				var indexEngine B = new(T)
				topts := NewTestTableOptions(t, driver, eng)

				// create table
				CreateEnum(t, e)
				CreateTable(t, e, tableEngine, topts, s)

				// insert data table
				ctx, _, commit, abort, err := e.WithTransaction(context.Background())
				defer abort()
				require.NoError(t, err)
				err = tableEngine.Open(ctx, s, topts)
				require.NoError(t, err)
				InsertData(t, ctx, tableEngine, s)

				// commit
				require.NoError(t, commit())

				iopts := NewTestIndexOptions(t, driver, eng, indexType)
				sc, err := s.SelectFields("i32", "id")
				require.NoError(t, err)

				if testing.Verbose() {
					iopts.Logger = log.Log.SetLevel(log.LevelDebug)
				}

				c.Run(t, e, tableEngine, indexEngine, sc, s, iopts, topts)

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
	tab.UseIndex(idxEngine)
	require.NoError(t, err)
	require.NoError(t, commit())
}

func CreateIndexEnginefunc(t *testing.T, e *engine.Engine, tab engine.TableEngine, ti engine.IndexEngine, s *schema.Schema, ts *schema.Schema, io engine.IndexOptions, to engine.TableOptions) {
	t.Helper()
	// create index
	CreateIndex(t, ti, tab, e, io, s)
}

func OpenIndexEnginefunc(t *testing.T, e *engine.Engine, tab engine.TableEngine, ti engine.IndexEngine, s *schema.Schema, ts *schema.Schema, io engine.IndexOptions, to engine.TableOptions) {
	t.Helper()
	// create index
	CreateIndex(t, ti, tab, e, io, s)
	require.NoError(t, ti.Close(context.Background()))

	ctx, _, commit, abort, err := e.WithTransaction(context.Background())
	defer abort()
	require.NoError(t, err)
	require.NoError(t, ti.Open(ctx, tab, s, io))
	require.NoError(t, commit())
	require.NoError(t, ti.Close(ctx))
}

func CloseIndexEnginefunc(t *testing.T, e *engine.Engine, tab engine.TableEngine, ti engine.IndexEngine, s *schema.Schema, ts *schema.Schema, io engine.IndexOptions, to engine.TableOptions) {
	t.Helper()
	// create index
	CreateIndex(t, ti, tab, e, io, s)

	ctx, _, commit, abort, err := e.WithTransaction(context.Background())
	defer abort()
	require.NoError(t, err)

	require.NoError(t, ti.Close(ctx))
	require.NoError(t, commit())
}

func DropIndexEnginefunc(t *testing.T, e *engine.Engine, tab engine.TableEngine, ti engine.IndexEngine, s *schema.Schema, ts *schema.Schema, io engine.IndexOptions, to engine.TableOptions) {
	t.Helper()
	// create index
	CreateIndex(t, ti, tab, e, io, s)

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

func TruncateIndexEnginefunc(t *testing.T, e *engine.Engine, tab engine.TableEngine, ti engine.IndexEngine, s *schema.Schema, ts *schema.Schema, io engine.IndexOptions, to engine.TableOptions) {
	t.Helper()
	// create index
	CreateIndex(t, ti, tab, e, io, s)

	ctx, _, _, abort, err := e.WithTransaction(context.Background())
	defer abort()
	require.NoError(t, err)

	require.NoError(t, ti.Truncate(ctx))
}

func RebuildIndexEnginefunc(t *testing.T, e *engine.Engine, tab engine.TableEngine, ti engine.IndexEngine, s *schema.Schema, ts *schema.Schema, io engine.IndexOptions, to engine.TableOptions) {
	t.Helper()
	// create index
	CreateIndex(t, ti, tab, e, io, s)

	ctx, _, commit, abort, err := e.WithTransaction(context.Background())
	defer abort()
	require.NoError(t, err)

	require.NoError(t, ti.Rebuild(ctx))
	require.NoError(t, commit())
}

func SyncIndexEnginefunc(t *testing.T, e *engine.Engine, tab engine.TableEngine, ti engine.IndexEngine, s *schema.Schema, ts *schema.Schema, io engine.IndexOptions, to engine.TableOptions) {
	t.Helper()
	// create index
	CreateIndex(t, ti, tab, e, io, s)

	ctx, _, commit, abort, err := e.WithTransaction(context.Background())
	defer abort()
	require.NoError(t, err)

	require.NoError(t, ti.Sync(ctx))
	require.NoError(t, commit())
}

func AddIndexEnginefunc(t *testing.T, e *engine.Engine, tab engine.TableEngine, ti engine.IndexEngine, si *schema.Schema, st *schema.Schema, io engine.IndexOptions, to engine.TableOptions) {
	t.Helper()
	ctx, _, commit, abort, err := e.WithTransaction(context.Background())
	defer abort()
	require.NoError(t, err)

	// create index
	CreateIndex(t, ti, tab, e, io, si)

	// add data index
	enc := schema.NewEncoder(st)
	for i := range 6 {
		allType := NewAllTypes(i)
		allType.Id = uint64(i + 1)
		buf, err := enc.Encode(allType, nil)
		require.NoError(t, err)
		require.NoError(t, ti.Add(ctx, nil, buf))
	}

	// commit
	require.NoError(t, commit())

	ctx, _, commit, abort, err = e.WithTransaction(context.Background())
	defer abort()
	require.NoError(t, err)
	// store
	require.NoError(t, ti.Sync(ctx))
	// commit
	require.NoError(t, commit())

	ctx, _, commit, abort, err = e.WithTransaction(context.Background())
	defer abort()
	require.NoError(t, err)

	// query data to confirm it is stored
	conditionId, err := query.ParseCondition("i32.lt", "6", si, e.Enums())
	require.NoError(t, err)
	conditionIdFlt, err := conditionId.Compile(si, nil)
	require.NoError(t, err)

	tRes, ok, err := ti.Query(ctx, conditionIdFlt.Children[0])
	require.NoError(t, err)
	require.Equal(t, io.Type == types.IndexTypeHash && io.Driver != "badger", ok)
	require.NotNil(t, tRes)
	require.Equal(t, 6, tRes.Count())

	// commit
	require.NoError(t, commit())
}

func DeleteIndexEnginefunc(t *testing.T, e *engine.Engine, tab engine.TableEngine, ti engine.IndexEngine, si *schema.Schema, st *schema.Schema, io engine.IndexOptions, to engine.TableOptions) {
	t.Helper()
	ctx, _, commit, abort, err := e.WithTransaction(context.Background())
	defer abort()
	require.NoError(t, err)

	// create index
	CreateIndex(t, ti, tab, e, io, si)

	// add data index
	enc := schema.NewEncoder(st)

	var prev []byte
	for i := range 6 {
		allType := NewAllTypes(i)
		allType.Id = uint64(i + 1)
		buf, err := enc.Encode(allType, nil)
		require.NoError(t, err)
		prev = buf
		require.NoError(t, ti.Add(ctx, nil, buf))
	}

	// commit
	require.NoError(t, commit())

	ctx, _, commit, abort, err = e.WithTransaction(context.Background())
	defer abort()
	require.NoError(t, err)
	// store
	require.NoError(t, ti.Sync(ctx))
	// commit
	require.NoError(t, commit())

	ctx, _, commit, abort, err = e.WithTransaction(context.Background())
	defer abort()
	require.NoError(t, err)

	// check 1:  query data to confirm it is stored
	conditionId, err := query.ParseCondition("i32.eq", "5", si, e.Enums())
	require.NoError(t, err)
	conditionIdFlt, err := conditionId.Compile(si, nil)
	require.NoError(t, err)
	tRes, ok, err := ti.Query(ctx, conditionIdFlt.Children[0])
	require.NoError(t, err)
	require.Equal(t, io.Type == types.IndexTypeHash && io.Driver != "badger", ok)
	require.NotNil(t, tRes)
	require.Equal(t, 1, tRes.Count())

	// delete last item stored
	require.NoError(t, ti.Del(ctx, prev))

	// store
	require.NoError(t, ti.Sync(ctx))
	// commit
	require.NoError(t, commit())

	ctx, _, commit, abort, err = e.WithTransaction(context.Background())
	defer abort()
	require.NoError(t, err)

	// check 2: confirm item is removed
	tRes, ok, err = ti.Query(ctx, conditionIdFlt.Children[0])
	require.NoError(t, err)
	require.False(t, ok, "no collision")
	require.NotNil(t, tRes)
	require.Equal(t, 0, tRes.Count())

	require.NoError(t, commit())
}

func CanMatchIndexEnginefunc(t *testing.T, e *engine.Engine, tab engine.TableEngine, ti engine.IndexEngine, si *schema.Schema, st *schema.Schema, io engine.IndexOptions, to engine.TableOptions) {
	t.Helper()
	ctx, _, commit, abort, err := e.WithTransaction(context.Background())
	defer abort()
	require.NoError(t, err)

	// create index
	CreateIndex(t, ti, tab, e, io, si)

	// add data index
	enc := schema.NewEncoder(st)

	for i := range 6 {
		allType := NewAllTypes(i)
		allType.Id = uint64(i + 1)
		buf, err := enc.Encode(allType, nil)
		require.NoError(t, err)
		require.NoError(t, ti.Add(ctx, nil, buf))
	}

	// commit
	require.NoError(t, commit())

	ctx, _, commit, abort, err = e.WithTransaction(context.Background())
	defer abort()
	require.NoError(t, err)
	// store
	require.NoError(t, ti.Sync(ctx))
	// commit
	require.NoError(t, commit())

	// condition 1 (in range of added data)
	conditionId, err := query.ParseCondition("i32.eq", "4", si, e.Enums())
	require.NoError(t, err)
	conditionId01Flt, err := conditionId.Compile(si, nil)
	require.NoError(t, err)
	require.True(t, ti.CanMatch(conditionId01Flt))

	// condition 2 ( higher than added data)
	conditionId, err = query.ParseCondition("i32.eq", "50", si, e.Enums())
	require.NoError(t, err)
	conditionId02Flt, err := conditionId.Compile(si, nil)
	require.NoError(t, err)
	require.True(t, ti.CanMatch(conditionId02Flt))

	// condition 3 (using id)
	conditionId, err = query.ParseCondition("id.eq", "5", si, e.Enums())
	require.NoError(t, err)
	conditionId03Flt, err := conditionId.Compile(si, nil)
	require.NoError(t, err)
	require.True(t, ti.CanMatch(conditionId03Flt))
}

func QueryIndexEnginefunc(t *testing.T, e *engine.Engine, tab engine.TableEngine, ti engine.IndexEngine, si *schema.Schema, st *schema.Schema, io engine.IndexOptions, to engine.TableOptions) {
	t.Helper()

	if io.Type == types.IndexTypeComposite {
		t.SkipNow()
	}

	ctx, _, commit, abort, err := e.WithTransaction(context.Background())
	defer abort()
	require.NoError(t, err)

	// create index
	CreateIndex(t, ti, tab, e, io, si)

	// add data index
	enc := schema.NewEncoder(st)
	for i := range 6 {
		allType := NewAllTypes(i)
		allType.Id = uint64(i + 1)
		buf, err := enc.Encode(allType, nil)
		require.NoError(t, err)
		require.NoError(t, ti.Add(ctx, nil, buf))
	}

	// commit
	require.NoError(t, commit())

	ctx, _, commit, abort, err = e.WithTransaction(context.Background())
	defer abort()
	require.NoError(t, err)

	// store
	require.NoError(t, ti.Sync(ctx))

	// query data (AllTypes) with id <= 4
	conditionId, err := query.ParseCondition("i32.lt", "5", si, e.Enums())
	require.NoError(t, err)
	conditionIdFlt, err := conditionId.Compile(si, nil)
	require.NoError(t, err)

	tRes, ok, err := ti.Query(ctx, conditionIdFlt.Children[0])
	require.NoError(t, err)
	require.Equal(t, false, ok)
	require.NotNil(t, tRes)
	require.Equal(t, 5, tRes.Count())

	// commit
	require.NoError(t, commit())
}