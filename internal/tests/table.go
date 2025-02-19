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
	Run  func(*testing.T, *engine.Engine, engine.TableEngine, engine.TableOptions)
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
	// {
	// 	Name: "Compact",
	// 	Run:  CompactTableTest,
	// },
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
			c.Run(t, e, tab, topts)
		})
	}
}

func SetupTableTest(t *testing.T, e *engine.Engine, tab engine.TableEngine, opts engine.TableOptions) {
	t.Helper()
	CreateEnum(t, e)
	CreateTable(t, e, tab, opts, allTypesSchema)
}

func CreateTable(t *testing.T, e *engine.Engine, tab engine.TableEngine, opts engine.TableOptions, s *schema.Schema) {
	t.Helper()
	ctx, _, commit, abort, err := e.WithTransaction(context.Background())
	require.NoError(t, err)
	defer abort()

	s = s.Clone().WithEnums(e.CloneEnums(s.EnumFieldNames()...)).Finalize()
	require.NoError(t, tab.Create(ctx, s, opts))
	require.NoError(t, commit())

	// reopen read-only if requested
	if opts.ReadOnly {
		require.NoError(t, tab.Close(context.Background()))
		ctx, _, commit, abort, err = e.WithTransaction(context.Background())
		require.NoError(t, err)
		defer abort()
		require.NoError(t, tab.Open(ctx, s, opts))
		require.NoError(t, commit())
	}
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

func InsertData(t *testing.T, e *engine.Engine, tab engine.TableEngine) {
	t.Helper()
	data := make([]*AllTypes, 10)
	for i := range data {
		data[i] = NewAllTypes(i)
	}

	var cnt int
	enc := schema.NewEncoder(tab.Schema())
	for _, rec := range data {
		buf, err := enc.Encode(rec, nil)
		require.NoError(t, err)
		ctx, _, commit, abort, err := e.WithTransaction(context.Background())
		require.NoError(t, err)
		_, err = tab.InsertRows(ctx, buf)
		assert.NoError(t, err)
		assert.NoError(t, commit())
		abort()
		cnt++
	}
	require.Equal(t, len(data), cnt)
}

func CreateTableTest(t *testing.T, e *engine.Engine, tab engine.TableEngine, opts engine.TableOptions) {
	CreateEnum(t, e)
	CreateTable(t, e, tab, opts, allTypesSchema)
}

func CreateMultipleTableSequentialTest(t *testing.T, e *engine.Engine, tab engine.TableEngine, opts engine.TableOptions) {
	t.Helper()
	CreateEnum(t, e)
	CreateTable(t, e, tab, opts, allTypesSchema)
	CreateTable(t, e, tab, opts, securitySchema)
}

func OpenTableTest(t *testing.T, e *engine.Engine, tab engine.TableEngine, opts engine.TableOptions) {
	SetupTableTest(t, e, tab, opts)
	require.NoError(t, tab.Close(context.Background()))
	ctx, _, commit, abort, err := e.WithTransaction(context.Background())
	defer abort()
	require.NoError(t, err)
	s := allTypesSchema.Clone().WithEnums(e.CloneEnums(allTypesSchema.EnumFieldNames()...)).Finalize()
	require.NoError(t, tab.Open(ctx, s, opts))
	require.NoError(t, commit())
}

func DropTableTest(t *testing.T, e *engine.Engine, tab engine.TableEngine, opts engine.TableOptions) {
	SetupTableTest(t, e, tab, opts)
	ctx, _, commit, abort, err := e.WithTransaction(context.Background())
	defer abort()
	require.NoError(t, err)
	require.NoError(t, tab.Drop(ctx))
	require.NoError(t, commit())
}

func SyncTableTest(t *testing.T, e *engine.Engine, tab engine.TableEngine, opts engine.TableOptions) {
	SetupTableTest(t, e, tab, opts)
	ctx, _, commit, abort, err := e.WithTransaction(context.Background())
	defer abort()
	require.NoError(t, err)
	require.NoError(t, tab.Sync(ctx))
	require.NoError(t, commit())
}

// TODO: enable when implemented
// func CompactTableTest(t *testing.T, e *engine.Engine, tab engine.TableEngine, opts engine.TableOptions) {
// 	SetupTableTest(t, e, tab, opts)
// 	ctx, _, commit, abort, err := e.WithTransaction(context.Background())
// 	defer abort()
// 	require.NoError(t, err)
// 	require.NoError(t, tab.Compact(ctx))
// 	require.NoError(t, commit())
// }

func TruncateTableTest(t *testing.T, e *engine.Engine, tab engine.TableEngine, opts engine.TableOptions) {
	SetupTableTest(t, e, tab, opts)
	ctx, _, commit, abort, err := e.WithTransaction(context.Background())
	require.NoError(t, err)
	defer abort()
	require.NoError(t, tab.Truncate(ctx))
	require.NoError(t, commit())
}

func InsertRowsTableTest(t *testing.T, e *engine.Engine, tab engine.TableEngine, opts engine.TableOptions) {
	SetupTableTest(t, e, tab, opts)
	InsertData(t, e, tab)
}

func InsertRowsReadOnlyTableTest(t *testing.T, e *engine.Engine, tab engine.TableEngine, opts engine.TableOptions) {
	opts.ReadOnly = true
	SetupTableTest(t, e, tab, opts)

	ctx, _, commit, abort, err := e.WithTransaction(context.Background())
	defer abort()
	require.NoError(t, err)

	enc := schema.NewEncoder(tab.Schema())
	buf, err := enc.Encode(NewAllTypes(10), nil)
	require.NoError(t, err)

	cnt, err := tab.InsertRows(ctx, buf)
	require.Error(t, err)
	assert.Equal(t, uint64(0), cnt)

	require.NoError(t, commit())
}

func UpdateRowsTableTest(t *testing.T, e *engine.Engine, tab engine.TableEngine, opts engine.TableOptions) {
	SetupTableTest(t, e, tab, opts)
	InsertData(t, e, tab)

	enc := schema.NewEncoder(tab.Schema())
	data := make([]*AllTypes, 10)
	for i := range data {
		data[i] = NewAllTypes(i)
		data[i].Id = uint64(i + 1)
	}
	buf, err := enc.Encode(data, nil)
	require.NoError(t, err)

	ctx, _, commit, abort, err := e.WithTransaction(context.Background())
	defer abort()
	require.NoError(t, err)

	cnt, err := tab.UpdateRows(ctx, buf)
	require.NoError(t, err)
	assert.Equal(t, uint64(len(data)), cnt)
	require.NoError(t, commit())
}

func QueryTableTest(t *testing.T, e *engine.Engine, tab engine.TableEngine, opts engine.TableOptions) {
	SetupTableTest(t, e, tab, opts)
	InsertData(t, e, tab)

	ctx, _, commit, abort, err := e.WithTransaction(context.Background())
	defer abort()
	require.NoError(t, err)

	plan := query.NewQueryPlan().
		WithFilters(makeFilter(tab.Schema(), "id", EQ, 5, nil)).
		WithSchema(tab.Schema()).
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

func CountTableTest(t *testing.T, e *engine.Engine, tab engine.TableEngine, opts engine.TableOptions) {
	SetupTableTest(t, e, tab, opts)
	InsertData(t, e, tab)

	ctx, _, commit, abort, err := e.WithTransaction(context.Background())
	defer abort()
	require.NoError(t, err)

	plan := query.NewQueryPlan().
		WithFilters(makeFilter(tab.Schema(), "id", LT, 5, nil)).
		WithSchema(tab.Schema()).
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

func DeleteTableTest(t *testing.T, e *engine.Engine, tab engine.TableEngine, opts engine.TableOptions) {
	SetupTableTest(t, e, tab, opts)
	InsertData(t, e, tab)

	ctx, _, commit, abort, err := e.WithTransaction(context.Background())
	defer abort()
	require.NoError(t, err)

	plan := query.NewQueryPlan().
		WithFilters(makeFilter(tab.Schema(), "id", GE, 5, nil)).
		WithSchema(tab.Schema()).
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

func StreamTableTest(t *testing.T, e *engine.Engine, tab engine.TableEngine, opts engine.TableOptions) {
	SetupTableTest(t, e, tab, opts)
	InsertData(t, e, tab)

	ctx, _, commit, abort, err := e.WithTransaction(context.Background())
	defer abort()
	require.NoError(t, err)

	plan := query.NewQueryPlan().
		WithFilters(makeFilter(tab.Schema(), "id", LT, 5, nil)).
		WithSchema(tab.Schema()).
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
