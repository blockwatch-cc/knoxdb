package tests

import (
	"context"
	"testing"

	"blockwatch.cc/knoxdb/internal/engine"
	"blockwatch.cc/knoxdb/internal/query"
	"blockwatch.cc/knoxdb/pkg/schema"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type TestCase struct {
	Name string
	Run  func(*engine.Engine, *testing.T, engine.TableEngine, engine.TableOptions)
}

type TF[T any] interface {
	*T
	engine.TableEngine
}

var TestCases = []TestCase{
	{
		Name: "Create",
		Run:  CreateTableTest,
	},
	{
		Name: "Create Multiple Tables Sequentially",
		Run:  CreateMultipleTableSequentialTest,
	},
	{
		Name: "Open",
		Run:  OpenTableTest,
	},
	{
		Name: "Drop",
		Run:  DropTableTest,
	},
	{
		Name: "Sync",
		Run:  SyncTableTest,
	},
	{
		Name: "Compact",
		Run:  CompactTableTest,
	},
	{
		Name: "Truncate",
		Run:  TruncateTableTest,
	},
	{
		Name: "InsertRows",
		Run:  InsertRowsTableTest,
	},
	{
		Name: "InsertRows:ReadOnlyDb",
		Run:  InsertRowsReadOnlyTableTest,
	},
	{
		Name: "UpdateRows",
		Run:  UpdateRowsTableTest,
	},
	{
		Name: "Query",
		Run:  QueryTableTest,
	},
	{
		Name: "Count",
		Run:  CountTableTest,
	},
	{
		Name: "Delete",
		Run:  DeleteTableTest,
	},
	{
		Name: "Stream",
		Run:  StreamTableTest,
	},
}

func TestTableEngine[T any, F TF[T]](t *testing.T, driver, eng string) {
	for _, c := range TestCases {
		t.Run(c.Name, func(t *testing.T) {
			ctx := context.Background()
			dopts := NewTestDatabaseOptions(t, driver)
			e := NewTestEngine(t, dopts)
			defer e.Close(ctx)

			var tab F = new(T)
			topts := NewTestTableOptions(t, driver, eng)
			c.Run(e, t, tab, topts)
		})
	}
}

func CreateTableTest(e *engine.Engine, t *testing.T, tab engine.TableEngine, opts engine.TableOptions) {
	t.Helper()
	CreateEnum(t, e)
	CreateTable(t, e, tab, opts, allTypesSchema)
}

func CreateMultipleTableSequentialTest(e *engine.Engine, t *testing.T, tab engine.TableEngine, opts engine.TableOptions) {
	t.Helper()
	CreateEnum(t, e)
	CreateTable(t, e, tab, opts, allTypesSchema)
	CreateTable(t, e, tab, opts, securitySchema)
}

func CreateTable(t *testing.T, e *engine.Engine, tab engine.TableEngine, opts engine.TableOptions, s *schema.Schema) {
	t.Helper()
	ctx, _, commit, abort, err := e.WithTransaction(context.Background())
	defer abort()
	require.NoError(t, err)

	err = tab.Create(ctx, s, opts)
	require.NoError(t, err)
	require.NoError(t, commit())
	require.NoError(t, tab.Close(ctx))
}

func CreateEnum(t *testing.T, e *engine.Engine) {
	t.Helper()
	ctx, _, commit, abort, err := e.WithTransaction(context.Background())
	defer abort()
	require.NoError(t, err)

	_, err = e.CreateEnum(context.Background(), "my_enum")
	require.NoError(t, err)
	err = e.ExtendEnum(ctx, "my_enum", "one", "two", "three", "four")
	require.NoError(t, err, "extend enum")
	require.NoError(t, commit())
}

func InsertData(t *testing.T, ctx context.Context, tab engine.TableEngine, s *engine.Schema) {
	t.Helper()
	allTypes := make([]*AllTypes, 10)
	for i := range allTypes {
		allTypes[i] = NewAllTypes(i)
	}

	var cnt uint64
	enc := schema.NewEncoder(s)
	for _, all := range allTypes {
		buf, err := enc.Encode(all, nil)
		require.NoError(t, err)
		cnt, err = tab.InsertRows(ctx, buf)
		require.NoError(t, err)
	}
	assert.Equal(t, uint64(len(allTypes)), cnt)
}

func OpenTableTest(e *engine.Engine, t *testing.T, tab engine.TableEngine, opts engine.TableOptions) {
	CreateTableTest(e, t, tab, opts)
	ctx, _, commit, abort, err := e.WithTransaction(context.Background())
	defer abort()
	require.NoError(t, err)
	require.NoError(t, tab.Open(ctx, allTypesSchema, opts))
	require.NoError(t, commit())
}

func DropTableTest(e *engine.Engine, t *testing.T, tab engine.TableEngine, opts engine.TableOptions) {
	CreateTableTest(e, t, tab, opts)
	ctx, _, commit, abort, err := e.WithTransaction(context.Background())
	defer abort()
	require.NoError(t, err)
	require.NoError(t, tab.Open(ctx, allTypesSchema, opts))
	require.NoError(t, commit())
	require.NoError(t, tab.Drop(ctx))
}

func SyncTableTest(e *engine.Engine, t *testing.T, tab engine.TableEngine, opts engine.TableOptions) {
	CreateTableTest(e, t, tab, opts)
	ctx, _, commit, abort, err := e.WithTransaction(context.Background())
	defer abort()
	require.NoError(t, err)
	require.NoError(t, tab.Open(ctx, allTypesSchema, opts))
	require.NoError(t, commit())
	require.NoError(t, tab.Sync(ctx))
	require.NoError(t, commit())
}

func CompactTableTest(e *engine.Engine, t *testing.T, tab engine.TableEngine, opts engine.TableOptions) {
	CreateTableTest(e, t, tab, opts)
	ctx, _, commit, abort, err := e.WithTransaction(context.Background())
	defer abort()
	require.NoError(t, err)
	require.NoError(t, tab.Open(ctx, allTypesSchema, opts))
	require.NoError(t, commit())
	require.NoError(t, tab.Compact(ctx))
}

func TruncateTableTest(e *engine.Engine, t *testing.T, tab engine.TableEngine, opts engine.TableOptions) {
	CreateTableTest(e, t, tab, opts)
	ctx, _, commit, abort, err := e.WithTransaction(context.Background())
	require.NoError(t, err)
	defer abort()
	require.NoError(t, tab.Open(ctx, allTypesSchema, opts))
	require.NoError(t, commit())

	ctx, _, commit, abort, err = e.WithTransaction(context.Background())
	require.NoError(t, err)
	defer abort()
	require.NoError(t, tab.Truncate(ctx))
	require.NoError(t, commit())
}

func InsertRowsTableTest(e *engine.Engine, t *testing.T, tab engine.TableEngine, opts engine.TableOptions) {
	CreateTableTest(e, t, tab, opts)
	ctx, _, commit, abort, err := e.WithTransaction(context.Background())
	defer abort()
	require.NoError(t, err)
	require.NoError(t, tab.Open(ctx, allTypesSchema, opts))
	require.NoError(t, commit())
	ctx, _, commit, abort, err = e.WithTransaction(context.Background())
	require.NoError(t, err)
	defer abort()
	InsertData(t, ctx, tab, allTypesSchema)
	require.NoError(t, commit())
	require.NoError(t, tab.Close(ctx))
}

func InsertRowsReadOnlyTableTest(e *engine.Engine, t *testing.T, tab engine.TableEngine, opts engine.TableOptions) {
	opts.ReadOnly = true
	CreateTableTest(e, t, tab, opts)

	ctx, _, commit, abort, err := e.WithTransaction(context.Background())
	defer abort()
	require.NoError(t, err)
	require.NoError(t, tab.Open(ctx, allTypesSchema, opts))

	enc := schema.NewEncoder(tab.Schema()).WithEnums(tab.Enums())
	buf, err := enc.Encode(NewAllTypes(10), nil)
	require.NoError(t, err)

	cnt, err := tab.InsertRows(ctx, buf)
	require.Error(t, err)
	assert.Equal(t, uint64(0), cnt)

	require.NoError(t, commit())
	require.NoError(t, tab.Close(ctx))
}

func UpdateRowsTableTest(e *engine.Engine, t *testing.T, tab engine.TableEngine, opts engine.TableOptions) {
	CreateTable(t, e, tab, opts, allTypesSchema)

	ctx, _, commit, abort, err := e.WithTransaction(context.Background())
	defer abort()
	require.NoError(t, err)
	require.NoError(t, tab.Open(ctx, allTypesSchema, opts))
	InsertData(t, ctx, tab, allTypesSchema)

	enc := schema.NewEncoder(allTypesSchema)
	allTypes := make([]*AllTypes, 10)
	for i := range allTypes {
		allTypes[i] = NewAllTypes(i)
		allTypes[i].Id = uint64(i + 1)
	}
	buf, err := enc.Encode(allTypes, nil)
	require.NoError(t, err)

	cnt, err := tab.UpdateRows(ctx, buf)
	require.NoError(t, err)
	assert.Equal(t, uint64(len(allTypes)), cnt)
	require.NoError(t, commit())
}

func QueryTableTest(e *engine.Engine, t *testing.T, tab engine.TableEngine, opts engine.TableOptions) {
	CreateTable(t, e, tab, opts, allTypesSchema)

	ctx, _, commit, abort, err := e.WithTransaction(context.Background())
	defer abort()
	require.NoError(t, err)
	require.NoError(t, tab.Open(ctx, allTypesSchema, opts))
	InsertData(t, ctx, tab, allTypesSchema)

	plan := query.NewQueryPlan().
		WithFilters(makeFilter(allTypesSchema, "id", EQ, 5, nil)).
		WithSchema(allTypesSchema).
		WithLimit(10).
		WithTable(tab)
	defer plan.Close()
	require.NoError(t, plan.Validate())
	require.NoError(t, plan.Compile(ctx))

	res, err := tab.Query(ctx, plan)
	require.NoError(t, err)
	defer res.Close()
	assert.Equal(t, int(1), res.Len())
	require.NoError(t, commit())
}

func CountTableTest(e *engine.Engine, t *testing.T, tab engine.TableEngine, opts engine.TableOptions) {
	CreateTable(t, e, tab, opts, allTypesSchema)

	ctx, _, commit, abort, err := e.WithTransaction(context.Background())
	defer abort()
	require.NoError(t, err)
	require.NoError(t, tab.Open(ctx, allTypesSchema, opts))
	InsertData(t, ctx, tab, allTypesSchema)

	plan := query.NewQueryPlan().
		WithFilters(makeFilter(allTypesSchema, "id", LT, 5, nil)).
		WithSchema(allTypesSchema).
		WithLimit(10).
		WithTable(tab)
	defer plan.Close()
	require.NoError(t, plan.Validate())
	require.NoError(t, plan.Compile(ctx))

	res, err := tab.Count(ctx, plan)
	require.NoError(t, err)
	assert.Equal(t, uint64(4), res)
	require.NoError(t, commit())
}

func DeleteTableTest(e *engine.Engine, t *testing.T, tab engine.TableEngine, opts engine.TableOptions) {
	CreateTable(t, e, tab, opts, allTypesSchema)
	ctx, _, commit, abort, err := e.WithTransaction(context.Background())
	defer abort()
	require.NoError(t, err)
	require.NoError(t, tab.Open(ctx, allTypesSchema, opts))
	InsertData(t, ctx, tab, allTypesSchema)

	plan := query.NewQueryPlan().
		WithFilters(makeFilter(allTypesSchema, "id", GE, 5, nil)).
		WithSchema(allTypesSchema).
		WithLimit(10).
		WithTable(tab)
	defer plan.Close()
	require.NoError(t, plan.Validate())
	require.NoError(t, plan.Compile(ctx))

	res, err := tab.Delete(ctx, plan)
	require.NoError(t, err)
	assert.Equal(t, uint64(6), res)
	require.NoError(t, commit())
}

func StreamTableTest(e *engine.Engine, t *testing.T, tab engine.TableEngine, opts engine.TableOptions) {
	CreateTableTest(e, t, tab, opts)

	ctx, _, commit, abort, err := e.WithTransaction(context.Background())
	defer abort()
	require.NoError(t, err)
	require.NoError(t, tab.Open(ctx, allTypesSchema, opts))
	InsertData(t, ctx, tab, allTypesSchema)

	plan := query.NewQueryPlan().
		WithFilters(makeFilter(allTypesSchema, "id", LT, 5, nil)).
		WithSchema(allTypesSchema).
		WithLimit(10).
		WithTable(tab)
	defer plan.Close()
	require.NoError(t, plan.Validate())
	require.NoError(t, plan.Compile(ctx))

	i := uint64(1)
	assertRowQuery := func(qr engine.QueryRow) error {
		var a AllTypes
		err = qr.Decode(&a)
		require.NoError(t, err)
		require.Equal(t, i, a.Id)
		i++
		return nil
	}

	require.NoError(t, tab.Stream(ctx, plan, assertRowQuery))
	require.NoError(t, commit())
}
