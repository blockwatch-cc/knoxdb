// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package stats

import (
	"context"
	"fmt"
	"testing"

	"blockwatch.cc/knoxdb/internal/pack"
	"blockwatch.cc/knoxdb/internal/query"
	"blockwatch.cc/knoxdb/internal/store"
	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/schema"
	"blockwatch.cc/knoxdb/pkg/util"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// -------------------------------------------------------------
// Test Helpers
//

type TestStruct struct {
	Id  uint64 `knox:"id,pk"`
	I64 int64  `knox:"i64,index=bloom:2"`
	I32 int32  `knox:"i32,index=bfuse"`
	I16 int16  `knox:"i16"`
	I8  int8   `knox:"i8,index=bits"`
	// Buf []byte `knox:"buf,fixed=8"`
	Buf []byte `knox:"buf"`
}

const (
	TEST_PKG_SIZE = 16
)

var (
	TestSchema = schema.MustSchemaOf(TestStruct{}).WithMeta()
)

func makeTestData(sz int, pk uint64) (res []TestStruct) {
	for i := 1; i <= sz; i++ {
		id := pk + uint64(i-1)
		res = append(res, TestStruct{
			Id:  id,
			I64: int64(id),
			I32: int32(id),
			I16: int16(id),
			I8:  int8(id),
			Buf: util.U64Bytes(id),
		})
	}
	return
}

func makeTestPackage(t testing.TB, key int, pk uint64) *pack.Package {
	pkg := pack.New().
		WithKey(uint32(key)).
		WithSchema(TestSchema).
		WithMaxRows(TEST_PKG_SIZE).
		WithStats().
		Alloc()
	enc := schema.NewGenericEncoder[TestStruct]()
	for _, v := range makeTestData(TEST_PKG_SIZE, pk) {
		buf, err := enc.Encode(v, nil)
		require.NoError(t, err)
		pkg.AppendWire(buf, &schema.Meta{Rid: v.Id, Xmin: 1})
	}
	// init statistics
	pstats := pkg.Stats()
	for i, b := range pkg.Blocks() {
		pstats.MinMax[i][0], pstats.MinMax[i][1] = b.MinMax()
	}

	return pkg
}

func makeFilter(name string, mode query.FilterMode, val, val2 any) *query.FilterTreeNode {
	field, ok := TestSchema.FieldByName(name)
	if !ok {
		panic(fmt.Errorf("missing field %s in schema %s", name, TestSchema))
	}
	m := query.NewFactory(field.Type()).New(mode)
	c := schema.NewCaster(field.Type(), field.Scale(), nil)
	var err error
	switch mode {
	case query.FilterModeRange:
		val, err = c.CastValue(val)
		if err != nil {
			panic(err)
		}
		val2, err = c.CastValue(val2)
		rg := query.RangeValue{val, val2}
		val = rg
	case query.FilterModeIn, query.FilterModeNotIn:
		val, err = c.CastSlice(val)
	default:
		val, err = c.CastValue(val)
	}
	if err != nil {
		panic(err)
	}
	m.WithValue(val)
	return &query.FilterTreeNode{
		Filter: &query.Filter{
			Name:    field.Name(),
			Type:    field.Type().BlockType(),
			Mode:    mode,
			Index:   field.Id() - 1,
			Value:   val,
			Matcher: m,
		},
	}
}

// silence golangci-lint unparam
var _ = makeFilter("i32", query.FilterModeEqual, 1, nil)

// TODO: test complex filter conditions
// func makeAndFilter(a, b *query.FilterTreeNode) *query.FilterTreeNode {
// 	return &query.FilterTreeNode{Children: []*query.FilterTreeNode{a, b}}
// }

// func makeOrFilter(a, b *query.FilterTreeNode) *query.FilterTreeNode {
// 	return &query.FilterTreeNode{OrKind: true, Children: []*query.FilterTreeNode{a, b}}
// }

// -------------------------------------------------------------
// Validation
func validateTree(t *testing.T, idx *Index) {
	ilen, slen := len(idx.inodes), len(idx.snodes)
	require.LessOrEqual(t, slen, ilen, "node type balance")
	for i, n := range idx.snodes {
		// all existing snodes are non-nil
		require.NotNil(t, n, "snode=%d", i)

		// all parent inodes are non-nil
		for n := parentIndex(i + ilen - 1); n >= 0; n = parentIndex(n) {
			require.NotNil(t, idx.inodes[n], "inode=%d", n)
		}
	}

	// TODO (only right sub-trees may not exist up to a certain level
	// we could probably compute at which level to stop)
	// all parent inodes for non-existing snodes are nil
	// make slen even because parent exists for left only child
	// t.Logf("ilen=%d slen=%d", ilen, slen)
	// slen += slen % 2
	// for i := slen; i < ilen; i++ {
	// 	sx := i + ilen - 1
	// 	t.Logf("snode=%d id=%d parent=%d", i, sx, parentIndex(sx))
	// 	for n := parentIndex(sx); n >= 0; n = parentIndex(n) {
	// 		require.Nil(t, (*idx.inodes)[n], "inode=%d", n)
	// 	}
	// }
}

// -------------------------------------------------------------
// Tree management (add, update, delete packs)
//
// - tree structure changes after add, update, delete
// - meta statistics after add, update, delete

func TestIndexCreate(t *testing.T) {
	ctx := context.Background()
	db, err := store.Create("mem", "stats")
	require.NoError(t, err)
	defer db.Close()
	idx := NewIndex(db, TestSchema, TEST_PKG_SIZE)
	defer idx.Close()
	// t.Log(idx.schema)
	assert.Equal(t, 0, idx.Len(), "num data packs")
	assert.Equal(t, 0, idx.Count(), "num data rows")
	assert.Equal(t, 0, idx.HeapSize(), "heap size")
	assert.Equal(t, 0, idx.TableSize(), "table size")
	assert.Equal(t, 0, idx.IndexSize(), "index size")
	assert.Equal(t, uint32(0), idx.NextKey(), "next key")
	assert.Equal(t, uint64(0), idx.GlobalMinRid(), "global min rid")
	assert.Equal(t, uint64(0), idx.GlobalMaxRid(), "global max rid")

	// find should not panic
	it, ok := idx.FindRid(ctx, uint64(1))
	assert.False(t, ok, "find on empty index")
	assert.False(t, it.IsValid(), "valid iterator")
	it.Close()

	// query should not panic
	it, ok = idx.Query(ctx, nil, types.OrderAsc)
	assert.False(t, ok, "query on empty index")
	assert.False(t, it.IsValid(), "valid iterator")
	it.Close()
}

func TestIndexAddSingle(t *testing.T) {
	ctx := context.Background()
	db, err := store.Create("mem", "stats")
	require.NoError(t, err)
	defer db.Close()
	idx := NewIndex(db, TestSchema, TEST_PKG_SIZE)
	defer idx.Close()
	pkg := makeTestPackage(t, 0, 1)
	require.NoError(t, idx.AddPack(ctx, pkg))

	// index internals
	require.Len(t, idx.inodes, 2, "idx inodes")
	require.Len(t, idx.snodes, 1, "idx snodes")

	validateTree(t, idx)

	// snode internals
	snode := idx.snodes[0]
	assert.Equal(t, uint32(0), snode.spack.Key(), "snode spack key")
	assert.Equal(t, 1, snode.spack.Len(), "snode spack len")
	assert.True(t, snode.dirty, "snode dirty")

	// snode api
	assert.Equal(t, uint32(0), snode.Key(), "snode key")
	assert.LessOrEqual(t, idx.build.Len(), len(snode.Bytes()), "snode bytes")
	assert.False(t, snode.IsEmpty(), "snode empty")
	assert.True(t, snode.IsWritable(), "snode writable")
	assert.Equal(t, 1, snode.NPacks(), "snode num data packs")
	assert.Equal(t, uint32(0), snode.MinKey(), "first data pack key")
	assert.Equal(t, uint32(0), snode.MaxKey(), "last data pack key")

	// index api
	assert.Equal(t, 1, idx.Len(), "num data packs")
	assert.Equal(t, TEST_PKG_SIZE, idx.Count(), "num data rows")
	assert.Less(t, 0, idx.HeapSize(), "heap size")
	// assert.Less(t, 0, idx.TableSize(), "table size") // TODO
	assert.Less(t, 0, idx.IndexSize(), "index size")
	assert.Equal(t, uint32(1), idx.NextKey(), "next key")
	assert.Equal(t, uint64(1), idx.GlobalMinRid(), "global min rid")
	assert.Equal(t, uint64(TEST_PKG_SIZE), idx.GlobalMaxRid(), "global max rid")
}

func TestIndexAddMany(t *testing.T) {
	ctx := context.Background()
	db, err := store.Create("mem", "stats")
	require.NoError(t, err)
	defer db.Close()
	idx := NewIndex(db, TestSchema, TEST_PKG_SIZE)
	defer idx.Close()

	// num snodes we expect to attach
	sz := 5
	for n := 0; n < sz; n++ {
		// t.Logf("Starting spack #%d", n)
		// number of data packs per spack
		for p := 0; p < STATS_PACK_SIZE; p++ {
			// package key (sequential)
			key := p + n*STATS_PACK_SIZE

			// first data primary key (sequential)
			pk := uint64(1 + n*STATS_PACK_SIZE*TEST_PKG_SIZE + p*TEST_PKG_SIZE)

			// add and check error
			require.NoError(t, idx.AddPack(ctx, makeTestPackage(t, key, pk)))
		}
	}

	validateTree(t, idx)

	// tree internals
	require.Len(t, idx.inodes, 1<<log2ceil(sz), "idx num inodes")
	require.Len(t, idx.snodes, sz, "idx num snodes")

	// api
	assert.Equal(t, sz*STATS_PACK_SIZE, idx.Len(), "num data packs")
	assert.Equal(t, sz*STATS_PACK_SIZE*TEST_PKG_SIZE, idx.Count(), "num data rows")
	assert.Equal(t, uint32(sz*STATS_PACK_SIZE), idx.NextKey(), "next key")
	assert.Equal(t, uint64(1), idx.GlobalMinRid(), "global min rid")
	assert.Equal(t, uint64(sz*STATS_PACK_SIZE*TEST_PKG_SIZE), idx.GlobalMaxRid(), "global max rid")
}

func TestIndexUpdate(t *testing.T) {
	ctx := context.Background()
	db, err := store.Create("mem", "stats")
	require.NoError(t, err)
	defer db.Close()
	idx := NewIndex(db, TestSchema, TEST_PKG_SIZE)
	defer idx.Close()
	pkg := makeTestPackage(t, 0, 1)
	require.NoError(t, idx.AddPack(ctx, pkg))
	snode := idx.snodes[0]
	snode.dirty = false

	// set all pkg blocks clean
	for _, b := range pkg.Blocks() {
		b.SetClean()
	}

	// override pk in first row (note: use rid field since we use pack metadata!!)
	pkg.Block(6).Uint64().Set(0, 1000)
	pkg.WithStats()
	assert.True(t, pkg.Block(6).IsDirty(), "block is dirty after write")
	require.NoError(t, idx.UpdatePack(ctx, pkg))

	// index internals
	require.Len(t, idx.inodes, 2, "idx inodes")
	require.Len(t, idx.snodes, 1, "idx snodes")

	validateTree(t, idx)

	// snode internals
	snode = idx.snodes[0]
	assert.Equal(t, uint32(0), snode.spack.Key(), "snode spack key")
	assert.Equal(t, 1, snode.spack.Len(), "snode spack len")
	assert.True(t, snode.dirty, "snode dirty")

	// snode api
	assert.Equal(t, uint32(0), snode.Key(), "snode key")
	assert.LessOrEqual(t, idx.build.Len(), len(snode.Bytes()), "snode bytes")
	assert.False(t, snode.IsEmpty(), "snode empty")
	assert.True(t, snode.IsWritable(), "snode writable")
	assert.Equal(t, 1, snode.NPacks(), "snode num data packs")
	assert.Equal(t, uint32(0), snode.MinKey(), "first data pack key")
	assert.Equal(t, uint32(0), snode.MaxKey(), "last data pack key")

	// index api
	assert.Equal(t, 1, idx.Len(), "num data packs")
	assert.Equal(t, TEST_PKG_SIZE, idx.Count(), "num data rows")
	assert.Less(t, 0, idx.HeapSize(), "heap size")
	// assert.Less(t, 0, idx.TableSize(), "table size") // TODO
	assert.Less(t, 0, idx.IndexSize(), "index size")
	assert.Equal(t, uint32(1), idx.NextKey(), "next key")
	assert.Equal(t, uint64(2), idx.GlobalMinRid(), "global min rid")
	assert.Equal(t, uint64(1000), idx.GlobalMaxRid(), "global max rid")
}

func TestIndexDeleteSingle(t *testing.T) {
	ctx := context.Background()
	db, err := store.Create("mem", "stats")
	require.NoError(t, err)
	defer db.Close()
	idx := NewIndex(db, TestSchema, TEST_PKG_SIZE)
	defer idx.Close()
	require.NoError(t, idx.AddPack(ctx, makeTestPackage(t, 0, 1)))
	require.NoError(t, idx.AddPack(ctx, makeTestPackage(t, 1, TEST_PKG_SIZE+1)))
	pkg := makeTestPackage(t, 2, 2*TEST_PKG_SIZE+1)
	require.NoError(t, idx.AddPack(ctx, pkg))

	// check tree
	require.Len(t, idx.inodes, 2, "idx inodes")
	require.Len(t, idx.snodes, 1, "idx snodes")
	validateTree(t, idx)
	assert.Equal(t, 3, idx.Len(), "num data packs")
	assert.Equal(t, 3*TEST_PKG_SIZE, idx.Count(), "num data rows")
	assert.Equal(t, uint64(1), idx.GlobalMinRid(), "global min rid")
	assert.Equal(t, uint64(3*TEST_PKG_SIZE), idx.GlobalMaxRid(), "global max rid")

	// delete
	require.NoError(t, idx.DeletePack(ctx, pkg))

	// check tree
	require.Len(t, idx.inodes, 2, "idx inodes")
	require.Len(t, idx.snodes, 1, "idx snodes")
	validateTree(t, idx)
	assert.Equal(t, 2, idx.Len(), "num data packs")
	assert.Equal(t, 2*TEST_PKG_SIZE, idx.Count(), "num data rows")
	assert.Equal(t, uint64(1), idx.GlobalMinRid(), "global min rid")
	assert.Equal(t, uint64(2*TEST_PKG_SIZE), idx.GlobalMaxRid(), "global max rid")
}

func TestIndexDeleteMany(t *testing.T) {
	// Tests deleting an entire spack to force a tree reorg.
	ctx := context.Background()
	db, err := store.Create("mem", "stats")
	require.NoError(t, err)
	defer db.Close()
	idx := NewIndex(db, TestSchema, TEST_PKG_SIZE)
	defer idx.Close()

	sz := 5
	for n := 0; n < sz; n++ {
		for p := 0; p < STATS_PACK_SIZE; p++ {
			key := p + n*STATS_PACK_SIZE
			pk := uint64(1 + n*STATS_PACK_SIZE*TEST_PKG_SIZE + p*TEST_PKG_SIZE)
			require.NoError(t, idx.AddPack(ctx, makeTestPackage(t, key, pk)))
		}
	}

	// delete the first spack worth of data packs
	for i := 0; i < STATS_PACK_SIZE; i++ {
		pkg := pack.New().WithKey(uint32(i))
		require.NoError(t, idx.DeletePack(ctx, pkg))
	}

	// store should remove first spack and rebuild the full tree
	tx, err := idx.db.Begin(true)
	defer tx.Rollback()
	require.NoError(t, err)
	require.NoError(t, idx.Store(ctx, tx))
	require.NoError(t, tx.Commit())

	validateTree(t, idx)

	// tree internals
	require.Len(t, idx.inodes, 1<<log2ceil(4), "idx num inodes")
	require.Len(t, idx.snodes, 4, "idx num snodes")

	snode := idx.snodes[0]
	assert.Equal(t, uint32(STATS_PACK_SIZE), snode.MinKey(), "first snode first data pack key")
	assert.Equal(t, uint32(2*STATS_PACK_SIZE-1), snode.MaxKey(), "first snode last data pack key")

	// api
	assert.Equal(t, (sz-1)*STATS_PACK_SIZE, idx.Len(), "num data packs")
	assert.Equal(t, (sz-1)*STATS_PACK_SIZE*TEST_PKG_SIZE, idx.Count(), "num data rows")
	assert.Equal(t, uint32(sz*STATS_PACK_SIZE), idx.NextKey(), "next key")
	assert.Equal(t, uint64(STATS_PACK_SIZE*TEST_PKG_SIZE+1), idx.GlobalMinRid(), "global min rid")
	assert.Equal(t, uint64(sz*STATS_PACK_SIZE*TEST_PKG_SIZE), idx.GlobalMaxRid(), "global max rid")
}

// -------------------------------------------------------------
// store/load/clone
// - tree structure reconstruction on load
// - block materialize on add/update post load
// - meta stats queries post load work on all nodes
func TestIndexStore(t *testing.T) {
	ctx := context.Background()
	db, err := store.Create("mem", "stats")
	require.NoError(t, err)
	defer db.Close()
	src := NewIndex(db, TestSchema, TEST_PKG_SIZE)

	sz := 5
	for n := 0; n < sz; n++ {
		for p := 0; p < STATS_PACK_SIZE; p++ {
			key := p + n*STATS_PACK_SIZE
			pk := uint64(1 + n*STATS_PACK_SIZE*TEST_PKG_SIZE + p*TEST_PKG_SIZE)
			require.NoError(t, src.AddPack(ctx, makeTestPackage(t, key, pk)))
		}
	}

	tx, err := src.db.Begin(true)
	require.NoError(t, err)
	require.NoError(t, src.Store(ctx, tx))
	require.NoError(t, tx.Commit())
	src.Close()

	// load 2nd index
	idx := NewIndex(db, TestSchema, TEST_PKG_SIZE)
	defer idx.Close()
	tx, err = idx.db.Begin(false)
	require.NoError(t, err)
	require.NoError(t, idx.Load(ctx, tx))
	require.NoError(t, tx.Rollback())

	validateTree(t, idx)

	// tree internals
	require.Len(t, idx.inodes, 1<<log2ceil(sz), "idx num inodes")
	require.Len(t, idx.snodes, sz, "idx num snodes")

	// api
	assert.Equal(t, sz*STATS_PACK_SIZE, idx.Len(), "num data packs")
	assert.Equal(t, sz*STATS_PACK_SIZE*TEST_PKG_SIZE, idx.Count(), "num data rows")
	assert.Equal(t, uint32(sz*STATS_PACK_SIZE), idx.NextKey(), "next key")
	assert.Equal(t, true, idx.IsTailFull(), "fill tail")
	assert.Equal(t, uint64(1), idx.GlobalMinRid(), "global min rid")
	assert.Equal(t, uint64(sz*STATS_PACK_SIZE*TEST_PKG_SIZE), idx.GlobalMaxRid(), "global max rid")
}

func TestIndexStoreAndAdd(t *testing.T) {
	ctx := context.Background()
	db, err := store.Create("mem", "stats")
	require.NoError(t, err)
	defer db.Close()
	idx := NewIndex(db, TestSchema, TEST_PKG_SIZE)

	// fill half
	for k := 0; k < STATS_PACK_SIZE/2; k++ {
		pk := uint64(1 + k*TEST_PKG_SIZE)
		require.NoError(t, idx.AddPack(ctx, makeTestPackage(t, k, pk)))
	}

	// store
	tx, err := idx.db.Begin(true)
	require.NoError(t, err)
	require.NoError(t, idx.Store(ctx, tx))
	require.NoError(t, tx.Commit())

	// fill more
	key := STATS_PACK_SIZE / 2
	pk := uint64(1 + key*TEST_PKG_SIZE)
	require.NoError(t, idx.AddPack(ctx, makeTestPackage(t, key, pk)))
}

// -------------------------------------------------------------
// query features
// - min/max queries and on demand block loading
// - findpk placement
// - visit all packs (nil filter, walk with iterator)

func TestIndexQueryEqual(t *testing.T) {
	ctx := context.Background()
	db, err := store.Create("mem", "stats")
	require.NoError(t, err)
	defer db.Close()
	idx := NewIndex(db, TestSchema, TEST_PKG_SIZE).WithFeatures(FeatRangeFilter)
	defer idx.Close()

	sz := 5
	for n := 0; n < sz; n++ {
		for p := 0; p < STATS_PACK_SIZE; p++ {
			key := p + n*STATS_PACK_SIZE
			pk := uint64(1 + n*STATS_PACK_SIZE*TEST_PKG_SIZE + p*TEST_PKG_SIZE)
			require.NoError(t, idx.AddPack(ctx, makeTestPackage(t, key, pk)))
		}
	}

	// equal filter: matches first record in second snode
	f := makeFilter("id", query.FilterModeEqual, uint64(STATS_PACK_SIZE*TEST_PKG_SIZE+1), nil)
	it, ok := idx.Query(ctx, f, types.OrderAsc)
	defer it.Close()
	require.True(t, ok, "found match")
	require.NotNil(t, it, "it is not nil")
	require.True(t, it.IsValid(), "is valid")
	assert.True(t, it.IsFull(), "is full")
	assert.Equal(t, uint32(STATS_PACK_SIZE), it.Key(), "data pack key")
	assert.Equal(t, TEST_PKG_SIZE, it.NValues(), "data pack len")
	minv, maxv := it.MinMax(0)
	assert.Equal(t, uint64(STATS_PACK_SIZE*TEST_PKG_SIZE+1), minv, "min pk")
	assert.Equal(t, uint64((STATS_PACK_SIZE+1)*TEST_PKG_SIZE), maxv, "max pk")
	assert.Equal(t, types.Range{0, 0}, it.Range(), "scan range")
	assert.False(t, it.Next(), "no more matches")
	assert.False(t, it.IsValid(), "is no longer valid")
}

func TestIndexQueryAll(t *testing.T) {
	ctx := context.Background()
	db, err := store.Create("mem", "stats")
	require.NoError(t, err)
	defer db.Close()
	idx := NewIndex(db, TestSchema, TEST_PKG_SIZE).WithFeatures(FeatRangeFilter)
	defer idx.Close()

	sz := 5
	for n := 0; n < sz; n++ {
		for p := 0; p < STATS_PACK_SIZE; p++ {
			key := p + n*STATS_PACK_SIZE
			pk := uint64(1 + n*STATS_PACK_SIZE*TEST_PKG_SIZE + p*TEST_PKG_SIZE)
			require.NoError(t, idx.AddPack(ctx, makeTestPackage(t, key, pk)))
		}
	}

	// match all
	it, ok := idx.Query(ctx, nil, types.OrderAsc)
	defer it.Close()
	require.True(t, ok, "found match")
	require.NotNil(t, it, "it is not nil")

	var pk uint64 = 1
	for i := 0; i < sz*STATS_PACK_SIZE; i++ {
		require.True(t, it.IsValid(), "is valid")
		require.True(t, it.IsFull(), "is full")
		require.Equal(t, uint32(i), it.Key(), "data pack key")
		require.Equal(t, TEST_PKG_SIZE, it.NValues(), "data pack len")
		minv, maxv := it.MinMax(0)
		require.Equal(t, pk, minv, "min pk")
		require.Equal(t, pk+uint64(TEST_PKG_SIZE-1), maxv, "max pk")
		require.Equal(t, types.Range{0, TEST_PKG_SIZE - 1}, it.Range(), "scan range")
		pk += TEST_PKG_SIZE
		if i < sz*STATS_PACK_SIZE-1 {
			require.True(t, it.Next(), "want more matches")
		} else {
			// end
			require.False(t, it.Next(), "no more matches")
			require.False(t, it.IsValid(), "is no longer valid")
		}
	}
}

func TestIndexQueryLess(t *testing.T) {
	// Tests deleting an entire spack to force a tree reorg.
	ctx := context.Background()
	db, err := store.Create("mem", "stats")
	require.NoError(t, err)
	defer db.Close()
	idx := NewIndex(db, TestSchema, TEST_PKG_SIZE).WithFeatures(FeatRangeFilter)
	defer idx.Close()

	sz := 1
	for n := 0; n < sz; n++ {
		for p := 0; p < STATS_PACK_SIZE; p++ {
			key := p + n*STATS_PACK_SIZE
			pk := uint64(1 + n*STATS_PACK_SIZE*TEST_PKG_SIZE + p*TEST_PKG_SIZE)
			require.NoError(t, idx.AddPack(ctx, makeTestPackage(t, key, pk)))
		}
	}

	// equal filter: matches first and second record in first snode
	f := makeFilter("id", query.FilterModeLe, uint64(TEST_PKG_SIZE+1), nil)
	it, ok := idx.Query(ctx, f, types.OrderAsc)
	defer it.Close()
	require.True(t, ok, "found match")
	require.NotNil(t, it, "it is not nil")
	require.True(t, it.IsValid(), "is valid")
	assert.True(t, it.IsFull(), "is full")
	assert.Equal(t, uint32(0), it.Key(), "data pack key")
	assert.Equal(t, TEST_PKG_SIZE, it.NValues(), "data pack len")
	minv, maxv := it.MinMax(0)
	// t.Logf("Pack pk range %d..%d", minv, maxv)
	assert.Equal(t, uint64(1), minv, "min pk")
	assert.Equal(t, uint64(TEST_PKG_SIZE), maxv, "max pk")
	assert.Equal(t, types.Range{0, 15}, it.Range(), "scan range")

	assert.True(t, it.Next(), "more matches")
	assert.True(t, it.IsValid(), "is still valid")
	assert.Equal(t, uint32(1), it.Key(), "data pack key")
	assert.Equal(t, TEST_PKG_SIZE, it.NValues(), "data pack len")
	minv, maxv = it.MinMax(0)
	// t.Logf("Pack pk range %d..%d", minv, maxv)
	assert.Equal(t, uint64(TEST_PKG_SIZE+1), minv, "min pk")
	assert.Equal(t, uint64(2*TEST_PKG_SIZE), maxv, "max pk")
	assert.Equal(t, types.Range{0, 0}, it.Range(), "scan range")

	assert.False(t, it.Next(), "no more matches")
	assert.False(t, it.IsValid(), "is no longer valid")
}

func TestIndexQueryRange(t *testing.T) {
	// Tests deleting an entire spack to force a tree reorg.
	ctx := context.Background()
	db, err := store.Create("mem", "stats")
	require.NoError(t, err)
	defer db.Close()
	idx := NewIndex(db, TestSchema, TEST_PKG_SIZE).WithFeatures(FeatRangeFilter)
	defer idx.Close()

	sz := 1
	for n := 0; n < sz; n++ {
		for p := 0; p < STATS_PACK_SIZE; p++ {
			key := p + n*STATS_PACK_SIZE
			pk := uint64(1 + n*STATS_PACK_SIZE*TEST_PKG_SIZE + p*TEST_PKG_SIZE)
			require.NoError(t, idx.AddPack(ctx, makeTestPackage(t, key, pk)))
		}
	}

	// equal filter: matches first and second record in first snode
	f := makeFilter("id", query.FilterModeRange, uint64(1), uint64(TEST_PKG_SIZE+1))
	it, ok := idx.Query(ctx, f, types.OrderAsc)
	defer it.Close()
	require.True(t, ok, "found match")
	require.NotNil(t, it, "it is not nil")
	require.True(t, it.IsValid(), "is valid")
	assert.True(t, it.IsFull(), "is full")
	assert.Equal(t, uint32(0), it.Key(), "data pack key")
	assert.Equal(t, TEST_PKG_SIZE, it.NValues(), "data pack len")
	minv, maxv := it.MinMax(0)
	// t.Logf("Pack pk range %d..%d", minv, maxv)
	assert.Equal(t, uint64(1), minv, "min pk")
	assert.Equal(t, uint64(TEST_PKG_SIZE), maxv, "max pk")
	assert.Equal(t, types.Range{0, 15}, it.Range(), "scan range")

	assert.True(t, it.Next(), "more matches")
	assert.True(t, it.IsValid(), "is still valid")
	assert.Equal(t, uint32(1), it.Key(), "data pack key")
	assert.Equal(t, TEST_PKG_SIZE, it.NValues(), "data pack len")
	minv, maxv = it.MinMax(0)
	// t.Logf("Pack pk range %d..%d", minv, maxv)
	assert.Equal(t, uint64(TEST_PKG_SIZE+1), minv, "min pk")
	assert.Equal(t, uint64(2*TEST_PKG_SIZE), maxv, "max pk")
	assert.Equal(t, types.Range{0, 0}, it.Range(), "scan range")

	assert.False(t, it.Next(), "no more matches")
	assert.False(t, it.IsValid(), "is no longer valid")
}

func TestIndexFindPk(t *testing.T) {
	ctx := context.Background()
	db, err := store.Create("mem", "stats")
	require.NoError(t, err)
	defer db.Close()
	idx := NewIndex(db, TestSchema, TEST_PKG_SIZE).WithFeatures(FeatRangeFilter)
	defer idx.Close()

	// fill half
	for k := 0; k < STATS_PACK_SIZE/2; k++ {
		pk := uint64(1 + k*TEST_PKG_SIZE)
		require.NoError(t, idx.AddPack(ctx, makeTestPackage(t, k, pk)))
	}

	// find pk in first data pack
	it, ok := idx.FindRid(ctx, 1)
	defer it.Close()
	require.True(t, ok, "found match")
	require.NotNil(t, it, "it is not nil")
	require.True(t, it.IsValid(), "is valid")
	assert.True(t, it.IsFull(), "is full")
	assert.Equal(t, uint32(0), it.Key(), "data pack key")
	assert.Equal(t, TEST_PKG_SIZE, it.NValues(), "data pack len")
	minv, maxv := it.MinMax(0)
	assert.Equal(t, uint64(1), minv, "min pk")
	assert.Equal(t, uint64(TEST_PKG_SIZE), maxv, "max pk")
	assert.Equal(t, types.Range{0, 0}, it.Range(), "scan range")
	assert.False(t, it.Next(), "no more matches")
	assert.False(t, it.IsValid(), "is no longer valid")
}

func TestIndexFindPkEnd(t *testing.T) {
	ctx := context.Background()
	db, err := store.Create("mem", "stats")
	require.NoError(t, err)
	defer db.Close()
	idx := NewIndex(db, TestSchema, TEST_PKG_SIZE)
	defer idx.Close()

	// fill half (last data pack is full, so no more room for this pk)
	for k := 0; k < STATS_PACK_SIZE/2; k++ {
		pk := uint64(1 + k*TEST_PKG_SIZE)
		require.NoError(t, idx.AddPack(ctx, makeTestPackage(t, k, pk)))
	}

	// find pk beyond the last data pack
	it, ok := idx.FindRid(ctx, uint64(STATS_PACK_SIZE/2*TEST_PKG_SIZE+1))
	defer it.Close()
	require.True(t, ok, "found match")
	require.NotNil(t, it, "it is not nil")
	require.True(t, it.IsValid(), "is valid")
	assert.True(t, it.IsFull(), "is full")
	assert.Equal(t, uint32(STATS_PACK_SIZE/2-1), it.Key(), "data pack key")
	assert.Equal(t, TEST_PKG_SIZE, it.NValues(), "data pack len")
	assert.False(t, it.Next(), "no more matches")
	assert.False(t, it.IsValid(), "is no longer valid")
}

func TestIndexFindPkEndWithSpace(t *testing.T) {
	ctx := context.Background()
	db, err := store.Create("mem", "stats")
	require.NoError(t, err)
	defer db.Close()
	idx := NewIndex(db, TestSchema, TEST_PKG_SIZE)
	defer idx.Close()

	// fill half
	for k := 0; k < STATS_PACK_SIZE/2; k++ {
		pk := uint64(1 + k*TEST_PKG_SIZE)
		require.NoError(t, idx.AddPack(ctx, makeTestPackage(t, k, pk)))
	}
	// add one more data pack that is partially full
	pk := uint64(1 + STATS_PACK_SIZE/2*TEST_PKG_SIZE)
	pkg := makeTestPackage(t, STATS_PACK_SIZE/2, pk)
	pkg.Delete(1, TEST_PKG_SIZE-1)
	require.NoError(t, idx.AddPack(ctx, pkg))

	// find pk beyond last data pack
	it, ok := idx.FindRid(ctx, pk+1)
	defer it.Close()
	require.True(t, ok, "found match")
	require.NotNil(t, it, "it is not nil")
	require.True(t, it.IsValid(), "is valid")
	assert.False(t, it.IsFull(), "is full")
	assert.Equal(t, uint32(STATS_PACK_SIZE/2), it.Key(), "data pack key")
	assert.Equal(t, 1, it.NValues(), "data pack len")
	assert.False(t, it.Next(), "no more matches")
	assert.False(t, it.IsValid(), "is no longer valid")
}

// --------------------------------------------
// Benchmarks

// counted in spacks, i.e. 2048 stats rows each
var benchSizes = []int{1, 8, 32}

func BenchmarkIndexQueryEqual(b *testing.B) {
	for _, sz := range benchSizes {
		// build index
		ctx := context.Background()
		db, err := store.Create("mem", "stats")
		require.NoError(b, err)
		idx := NewIndex(db, TestSchema, TEST_PKG_SIZE)

		// insert Nx2048 data packs
		for n := 0; n < sz; n++ {
			for p := 0; p < STATS_PACK_SIZE; p++ {
				key := p + n*STATS_PACK_SIZE
				pk := uint64(1 + n*STATS_PACK_SIZE*TEST_PKG_SIZE + p*TEST_PKG_SIZE)
				require.NoError(b, idx.AddPack(ctx, makeTestPackage(b, key, pk)))
			}
		}
		f := makeFilter("id", query.FilterModeEqual, uint64(STATS_PACK_SIZE*TEST_PKG_SIZE+1), nil)

		b.Run(fmt.Sprintf("tree-%dx2048", sz), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				it, ok := idx.Query(ctx, f, types.OrderAsc)
				for ; ok; ok = it.Next() {
				}
				it.Close()
			}
		})

		idx.Close()
		db.Close()
	}
}

func BenchmarkIndexQueryAll(b *testing.B) {
	for _, sz := range benchSizes {
		// build index
		ctx := context.Background()
		db, err := store.Create("mem", "stats")
		require.NoError(b, err)
		idx := NewIndex(db, TestSchema, TEST_PKG_SIZE)

		// insert Nx2048 data packs
		for n := 0; n < sz; n++ {
			for p := 0; p < STATS_PACK_SIZE; p++ {
				key := p + n*STATS_PACK_SIZE
				pk := uint64(1 + n*STATS_PACK_SIZE*TEST_PKG_SIZE + p*TEST_PKG_SIZE)
				require.NoError(b, idx.AddPack(ctx, makeTestPackage(b, key, pk)))
			}
		}

		b.Run(fmt.Sprintf("tree-%dx2048", sz), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				it, ok := idx.Query(ctx, nil, types.OrderAsc)
				for ; ok; ok = it.Next() {
				}
				it.Close()
			}
		})

		idx.Close()
		db.Close()
	}
}
