// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package knox

import (
	"context"
	"fmt"
	"unsafe"

	"blockwatch.cc/knoxdb/internal/engine"
	"blockwatch.cc/knoxdb/pkg/schema"
	"blockwatch.cc/knoxdb/pkg/schema/encode"
	"blockwatch.cc/knoxdb/pkg/schema/reflect"
)

var _ Table = (*TableImpl)(nil)

type TableImpl struct {
	db    Database
	table engine.TableEngine
	enc   *encode.Encoder
}

func (t TableImpl) Schema() *schema.Schema {
	return t.table.Schema().Base()
}

func (t TableImpl) Metrics() TableMetrics {
	return t.table.Metrics()
}

func (t TableImpl) Engine() engine.TableEngine {
	return t.table
}

func (t TableImpl) DB() Database {
	return t.db
}

func (t TableImpl) Insert(ctx context.Context, batch *schema.Batch) (uint64, int, error) {
	// check schema matches
	if t.table.Schema().Base().Hash != batch.Schema().Hash {
		return 0, 0, schema.ErrSchemaMismatch
	}

	// use or open tx
	ctx, commit, abort, err := t.db.Begin(ctx)
	if err != nil {
		return 0, 0, err
	}
	defer abort()

	// call backend
	pk, n, err := t.table.InsertBatch(ctx, batch)
	if err != nil {
		return 0, 0, err
	}

	// commit/abort
	if err := commit(); err != nil {
		return 0, 0, err
	}

	return pk, n, nil
}

func (t TableImpl) Update(ctx context.Context, batch *schema.Batch) (int, error) {
	// check schema matches
	if t.table.Schema().Base().Hash != batch.Schema().Hash {
		return 0, schema.ErrSchemaMismatch
	}

	// use or open tx
	ctx, commit, abort, err := t.db.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer abort()

	// call backend
	n, err := t.table.UpdateBatch(ctx, batch)
	if err != nil {
		return 0, err
	}

	if err := commit(); err != nil {
		return 0, err
	}

	return n, nil
}

func (t TableImpl) Delete(ctx context.Context, q QueryRequest) (int, error) {
	plan, err := q.MakePlan()
	if err != nil {
		return 0, err
	}

	// use or open tx
	ctx, commit, abort, err := t.db.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer abort()

	if err := plan.Compile(ctx); err != nil {
		return 0, err
	}

	n, err := t.table.Delete(ctx, plan)
	if err != nil {
		return 0, err
	}

	if err := commit(); err != nil {
		return 0, err
	}

	return n, nil
}

func (t TableImpl) Count(ctx context.Context, q QueryRequest) (int, error) {
	plan, err := q.MakePlan()
	if err != nil {
		return 0, err
	}

	// use or open tx
	ctx, commit, abort, err := t.db.Begin(ctx, TxFlagReadOnly)
	if err != nil {
		return 0, err
	}
	defer abort()

	if err := plan.Compile(ctx); err != nil {
		return 0, err
	}

	n, err := t.table.Count(ctx, plan)
	if err != nil {
		return 0, err
	}

	if err := commit(); err != nil {
		return 0, err
	}

	return n, nil
}

func (t TableImpl) Query(ctx context.Context, q QueryRequest) (QueryResult, error) {
	plan, err := q.MakePlan()
	if err != nil {
		return nil, err
	}

	// use or open tx
	ctx, commit, abort, err := t.db.Begin(ctx, TxFlagReadOnly)
	if err != nil {
		return nil, err
	}
	defer abort()

	if err := plan.Compile(ctx); err != nil {
		return nil, err
	}

	res, err := t.table.Query(ctx, plan)
	if err != nil {
		return nil, err
	}

	if err := commit(); err != nil {
		return nil, err
	}

	return res, nil
}

func (t TableImpl) Stream(ctx context.Context, q QueryRequest, fn func(QueryRow) error) error {
	plan, err := q.MakePlan()
	if err != nil {
		return err
	}
	defer plan.Close()

	// use or open tx
	ctx, commit, abort, err := t.db.Begin(ctx, TxFlagReadOnly)
	if err != nil {
		return err
	}
	defer abort()

	if err := plan.Compile(ctx); err != nil {
		return err
	}

	if err := t.table.Stream(ctx, plan, fn); err != nil {
		return err
	}

	return commit()
}

// TableT[T] implements Table interface for Go struct types
type TableT[T any] struct {
	schema *schema.Schema
	enc    *encode.EncoderT[T]
	table  engine.TableEngine
	db     Database
}

// TODO: TableFor[T any](t Table, opts ...schema.Option) for version control
func TableFor[T any](t Table) (*TableT[T], error) {
	return FindTableFor[T](t.DB(), t.Schema().Name)
}

// TODO:
// FindTableFor[T any](db Database, opts ...schema.Option) (*TableT[T], error) {
// so we can do version control and consistent names
// FindTableFor[T](db, schema.Name("override"), schema.Version(2))
func FindTableFor[T any](db Database, name string) (*TableT[T], error) {
	table, err := db.FindTable(name)
	if err != nil {
		return nil, err
	}
	s, err := reflect.SchemaFor[T](schema.Enums(table.Schema().Enums.Load()))
	if err != nil {
		return nil, err
	}
	// check schema matches
	if table.Schema().Hash != s.Hash {
		return nil, schema.ErrSchemaMismatch
	}
	return &TableT[T]{
		schema: table.Schema(),
		table:  table.(*TableImpl).table,
		db:     db,
	}, nil
}

func (t *TableT[T]) Name() string {
	return t.schema.Name
}

func (t *TableT[T]) Schema() *schema.Schema {
	return t.schema
}

func (t *TableT[T]) Engine() engine.TableEngine {
	return t.table
}

func (t *TableT[T]) Metrics() TableMetrics {
	return t.table.Metrics()
}

func (t *TableT[T]) Table() Table {
	return &TableImpl{
		table: t.table,
		db:    t.db,
	}
}

func (t *TableT[T]) DB() Database {
	return t.db
}

func (t *TableT[T]) Insert(ctx context.Context, vals ...*T) (uint64, int, error) {
	if len(vals) == 0 {
		return 0, 0, ErrNoValues
	}
	if t.enc == nil {
		t.enc = encode.NewEncoderFor[T](schema.Enums(t.Schema().Enums.Load()))
	}
	wr := t.enc.NewBatchWriter(len(vals))
	defer wr.Close()
	if err := t.enc.EncodeBatch(wr.Buffer(), vals); err != nil {
		return 0, 0, fmt.Errorf("insert: %T %w", new(T), err)
	}

	// use or open tx
	ctx, commit, abort, err := t.db.Begin(ctx)
	if err != nil {
		return 0, 0, err
	}
	defer abort()

	// call backend, returns first sequential pk assigned
	pk, n, err := t.table.InsertBatch(ctx, wr.Batch())
	if err != nil {
		return 0, 0, err
	}

	if err := commit(); err != nil {
		return 0, 0, err
	}

	// assign primary keys to all values
	pkOffset := t.enc.Offset(t.schema.PkIndex())
	for _, v := range vals {
		*(*uint64)(unsafe.Add(unsafe.Pointer(v), pkOffset)) = pk
	}

	return pk, n, nil
}

func (t *TableT[T]) Update(ctx context.Context, vals ...*T) (int, error) {
	if t.enc == nil {
		t.enc = encode.NewEncoderFor[T](schema.Enums(t.Schema().Enums.Load()))
	}
	wr := t.enc.NewBatchWriter(len(vals))
	defer wr.Close()
	if err := t.enc.EncodeBatch(wr.Buffer(), vals); err != nil {
		return 0, fmt.Errorf("update: %T %w", new(T), err)
	}

	// use or open tx
	ctx, commit, abort, err := t.db.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer abort()

	// call backend
	n, err := t.table.UpdateBatch(ctx, wr.Batch())
	if err != nil {
		return 0, err
	}

	if err := commit(); err != nil {
		return 0, err
	}

	return n, nil
}

func (t *TableT[T]) Delete(ctx context.Context, q QueryRequest) (int, error) {
	plan, err := q.MakePlan()
	if err != nil {
		return 0, err
	}

	// use or open tx
	ctx, commit, abort, err := t.db.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer abort()

	if err := plan.Compile(ctx); err != nil {
		return 0, err
	}

	n, err := t.table.Delete(ctx, plan)
	if err != nil {
		return 0, err
	}

	if err := commit(); err != nil {
		return 0, err
	}

	return n, nil
}

func (t *TableT[T]) Count(ctx context.Context, q QueryRequest) (int, error) {
	plan, err := q.MakePlan()
	if err != nil {
		return 0, err
	}

	// use or open tx
	ctx, commit, abort, err := t.db.Begin(ctx, TxFlagReadOnly)
	if err != nil {
		return 0, err
	}
	defer abort()

	if err := plan.Compile(ctx); err != nil {
		return 0, err
	}

	n, err := t.table.Count(ctx, plan)
	if err != nil {
		return 0, err
	}

	if err := commit(); err != nil {
		return 0, err
	}

	return n, nil
}

func (t *TableT[T]) Query(ctx context.Context, q QueryRequest) ([]T, error) {
	return (QueryT[T]{q.(Query).WithTable(t.Table())}).Run(ctx)
}

func (t *TableT[T]) Stream(ctx context.Context, q QueryRequest, fn func(*T) error) error {
	return (QueryT[T]{q.(Query).WithTable(t.Table())}).Stream(ctx, fn)
}
