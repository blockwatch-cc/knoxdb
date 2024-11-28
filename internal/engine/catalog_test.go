// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package engine

import (
	"context"
	"testing"
	"time"

	"blockwatch.cc/knoxdb/internal/store"
	_ "blockwatch.cc/knoxdb/internal/store/mem"
	"blockwatch.cc/knoxdb/pkg/schema"
	"github.com/stretchr/testify/require"
)

func TestCatalogCreate(t *testing.T) {
	e := NewTestEngine(NewTestDatabaseOptions(t, "mem"))
	ctx, _, commit, _, err := e.WithTransaction(context.Background())
	require.NoError(t, err)
	require.NoError(t, e.cat.Create(ctx, e.opts))
	require.NoError(t, commit())

	// worst case we get a tx deadlock, so we check using Eventually()
	require.Eventually(t, func() bool {
		err := e.cat.db.View(func(tx store.Tx) error {
			for _, key := range [][]byte{
				databaseKey,
				schemasKey,
				optionsKey,
				tablesKey,
				indexesKey,
				viewsKey,
				enumsKey,
				storesKey,
				snapshotsKey,
				streamsKey,
			} {
				t.Log("Check bucket", string(key))
				require.NotNil(t, tx.Bucket(key))
			}
			return nil
		})
		require.NoError(t, err)
		return true
	}, time.Second/4, time.Second/8)

	require.NoError(t, e.cat.Close(context.Background()))
}

func TestCatalogOpen(t *testing.T) {
	// create first engine
	e := NewTestEngine(NewTestDatabaseOptions(t, "mem"))
	ctx, _, commit, _, err := e.WithTransaction(context.Background())
	require.NoError(t, err)

	// create catalog (requires write tx)
	require.NoError(t, e.cat.Create(ctx, e.opts))
	require.NoError(t, commit())
	require.NoError(t, e.cat.Close(context.Background()))

	// create new engine
	e = OpenTestEngine(e.opts)
	ctx, _, _, abort, err := e.WithTransaction(context.Background())
	require.NoError(t, err)
	require.NoError(t, e.cat.Open(ctx, e.opts))

	// worst case we get a tx deadlock, so we check using Eventually()
	require.Eventually(t, func() bool {
		err := e.cat.db.View(func(tx store.Tx) error {
			for _, key := range [][]byte{
				databaseKey,
				schemasKey,
				optionsKey,
				tablesKey,
				indexesKey,
				viewsKey,
				enumsKey,
				storesKey,
				snapshotsKey,
				streamsKey,
			} {
				t.Log("Check bucket", string(key))
				require.NotNil(t, tx.Bucket(key))
			}
			return nil
		})
		require.NoError(t, err)
		return true
	}, time.Second/4, time.Second/8)

	require.NoError(t, abort())
	require.NoError(t, e.cat.Close(context.Background()))
}

func WithCatalog(t *testing.T) (context.Context, *Engine, *Catalog, func() error) {
	ctx := context.Background()
	e := NewTestEngine(NewTestDatabaseOptions(t, "mem"))
	tctx, _, commit, _, err := e.WithTransaction(ctx)
	require.NoError(t, err)
	require.NoError(t, e.cat.Create(tctx, e.opts))
	require.NoError(t, commit())
	return ctx, e, e.cat, func() error { return e.cat.Close(ctx) }
}

type TestTable struct {
	Id uint64 `knox:"id,pk"`
	F1 int    `knox:"f1"`
}

func TestCatalogAddTable(t *testing.T) {
	ctx, eng, cat, close := WithCatalog(t)
	defer close()
	tctx, _, commit, abort, err := eng.WithTransaction(ctx)
	require.NoError(t, err)
	defer abort()
	s, err := schema.SchemaOf(&TestTable{})
	require.NoError(t, err)
	t.Log("Table", s)
	opts := TableOptions{
		Engine:   "pack",
		Driver:   "mem",
		PageSize: 1024,
	}
	require.NoError(t, cat.AddTable(tctx, 1, s, opts))
	require.NoError(t, commit())

	// list tables
	tctx, _, _, abort, err = eng.WithTransaction(ctx)
	require.NoError(t, err)
	defer abort()
	keys, err := cat.ListTables(tctx)
	require.NoError(t, err)
	require.Len(t, keys, 1)
	require.Equal(t, keys[0], uint64(1))

	// get table
	s2, opts2, err := cat.GetTable(tctx, 1)
	require.NoError(t, err)
	require.NotNil(t, s2)
	require.Equal(t, s2.Name(), s.Name())
	require.Equal(t, s2.Hash(), s.Hash())
	require.Equal(t, opts2, opts)
	require.NoError(t, abort())

	// drop table
	tctx, _, commit, abort, err = eng.WithTransaction(ctx)
	require.NoError(t, err)
	defer abort()
	require.NoError(t, cat.DropTable(tctx, 1))
	require.NoError(t, commit())

	tctx, _, _, abort, err = eng.WithTransaction(ctx)
	require.NoError(t, err)
	defer abort()
	keys, err = cat.ListTables(tctx)
	require.NoError(t, err)
	require.Len(t, keys, 0)
	require.NoError(t, abort())

	// drop unknown table
	tctx, _, _, abort, err = eng.WithTransaction(ctx)
	require.NoError(t, err)
	defer abort()
	require.Error(t, cat.DropTable(tctx, 1))
}

func TestCatalogAddIndex(t *testing.T) {
	ctx, eng, cat, close := WithCatalog(t)
	defer close()
	tctx, _, commit, abort, err := eng.WithTransaction(ctx)
	require.NoError(t, err)
	defer abort()
	s, err := schema.SchemaOf(&TestTable{})
	require.NoError(t, err)
	s.WithName(s.Name() + "_index")
	t.Log("Index", s)
	opts := IndexOptions{
		Engine:   "pack",
		Driver:   "mem",
		PageSize: 1024,
	}
	require.NoError(t, cat.AddIndex(tctx, 2, 1, s, opts))
	require.NoError(t, commit())

	// list indexes
	tctx, _, _, abort, err = eng.WithTransaction(ctx)
	require.NoError(t, err)
	defer abort()
	keys, err := cat.ListIndexes(tctx, 1)
	require.NoError(t, err)
	require.Len(t, keys, 1)
	require.Equal(t, keys[0], uint64(2))

	// get index
	s2, opts2, err := cat.GetIndex(tctx, 2)
	require.NoError(t, err)
	require.NotNil(t, s2)
	require.Equal(t, s2.Name(), s.Name())
	require.Equal(t, s2.Hash(), s.Hash())
	require.Equal(t, opts2, opts)
	require.NoError(t, abort())

	// drop index
	tctx, _, commit, abort, err = eng.WithTransaction(ctx)
	require.NoError(t, err)
	defer abort()
	require.NoError(t, cat.DropIndex(tctx, 2))
	require.NoError(t, commit())

	tctx, _, _, abort, err = eng.WithTransaction(ctx)
	require.NoError(t, err)
	defer abort()
	keys, err = cat.ListIndexes(tctx, 1)
	require.NoError(t, err)
	require.Len(t, keys, 0)
	require.NoError(t, abort())

	// drop unknown index
	tctx, _, _, abort, err = eng.WithTransaction(ctx)
	require.NoError(t, err)
	defer abort()
	require.Error(t, cat.DropIndex(tctx, 1))
}

func TestCatalogAddStore(t *testing.T) {
	ctx, eng, cat, close := WithCatalog(t)
	defer close()
	tctx, _, commit, abort, err := eng.WithTransaction(ctx)
	require.NoError(t, err)
	defer abort()
	s, err := schema.SchemaOf(&TestTable{})
	require.NoError(t, err)
	s.WithName(s.Name() + "_store")
	t.Log("Store", s)
	opts := StoreOptions{
		Driver:   "mem",
		PageSize: 1024,
	}
	require.NoError(t, cat.AddStore(tctx, 1, s, opts))
	require.NoError(t, commit())

	// list stores
	tctx, _, _, abort, err = eng.WithTransaction(ctx)
	require.NoError(t, err)
	defer abort()
	keys, err := cat.ListStores(tctx)
	require.NoError(t, err)
	require.Len(t, keys, 1)
	require.Equal(t, keys[0], uint64(1))

	// get store
	s2, opts2, err := cat.GetStore(tctx, 1)
	require.NoError(t, err)
	require.NotNil(t, s2)
	require.Equal(t, s2.Name(), s.Name())
	require.Equal(t, s2.Hash(), s.Hash())
	require.Equal(t, opts2, opts)
	require.NoError(t, abort())

	// drop store
	tctx, _, commit, abort, err = eng.WithTransaction(ctx)
	require.NoError(t, err)
	defer abort()
	require.NoError(t, cat.DropStore(tctx, 1))
	require.NoError(t, commit())

	tctx, _, _, abort, err = eng.WithTransaction(ctx)
	require.NoError(t, err)
	defer abort()
	keys, err = cat.ListStores(tctx)
	require.NoError(t, err)
	require.Len(t, keys, 0)
	require.NoError(t, abort())

	// drop unknown store
	tctx, _, _, abort, err = eng.WithTransaction(ctx)
	require.NoError(t, err)
	defer abort()
	require.Error(t, cat.DropStore(tctx, 1))
}

func TestCatalogAddEnum(t *testing.T) {
	ctx, eng, cat, close := WithCatalog(t)
	defer close()
	tctx, _, commit, abort, err := eng.WithTransaction(ctx)
	require.NoError(t, err)
	defer abort()
	enum := schema.NewEnumDictionary("enum")
	enum.Append("a", "b", "c")
	require.NoError(t, cat.AddEnum(tctx, enum))
	require.NoError(t, commit())

	// list enums
	tctx, _, _, abort, err = eng.WithTransaction(ctx)
	require.NoError(t, err)
	defer abort()
	keys, err := cat.ListEnums(tctx)
	require.NoError(t, err)
	require.Len(t, keys, 1)
	require.Equal(t, keys[0], enum.Tag())

	// get enum
	enum2, err := cat.GetEnum(tctx, enum.Tag())
	require.NoError(t, err)
	require.NotNil(t, enum2)
	require.Equal(t, enum2.Name(), enum.Name())
	require.Equal(t, enum2.Tag(), enum.Tag())
	require.Equal(t, enum2.Len(), enum.Len())
	require.NoError(t, abort())

	// drop enum
	tctx, _, commit, abort, err = eng.WithTransaction(ctx)
	require.NoError(t, err)
	defer abort()
	require.NoError(t, cat.DropEnum(tctx, enum.Tag()))
	require.NoError(t, commit())

	tctx, _, _, abort, err = eng.WithTransaction(ctx)
	require.NoError(t, err)
	defer abort()
	keys, err = cat.ListEnums(tctx)
	require.NoError(t, err)
	require.Len(t, keys, 0)
	require.NoError(t, abort())

	// drop unknown enum
	tctx, _, _, abort, err = eng.WithTransaction(ctx)
	require.NoError(t, err)
	defer abort()
	require.Error(t, cat.DropEnum(tctx, 1))
}
