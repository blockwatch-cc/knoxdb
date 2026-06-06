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

func (t TableImpl) Insert(ctx context.Context, val any) (uint64, int, error) {
	// analyze reflect
	s, err := reflect.SchemaOf(val, schema.Enums(t.table.Schema().Enums.Load()))
	if err != nil {
		return 0, 0, err
	}
	// check schema matches
	if t.table.Schema().Hash != s.Hash {
		return 0, 0, schema.ErrSchemaMismatch
	}

	// encode wire (single or slice) - schema is guaranteed the same
	// but we must use the one derived from Go type for struct read
	if t.enc == nil {
		// analyze struct layout
		l, err := reflect.LayoutOf(val, s)
		if err != nil {
			return 0, 0, err
		}
		t.enc = encode.NewEncoderWithLayout(s, l)
	}
	buf, err := t.enc.Encode(val, nil)
	if err != nil {
		return 0, 0, err
	}

	// use or open tx
	ctx, commit, abort, err := t.db.Begin(ctx)
	if err != nil {
		return 0, 0, err
	}
	defer abort()

	// call backend
	pk, n, err := t.table.InsertRows(ctx, buf)
	if err != nil {
		return 0, 0, err
	}

	// commit/abort
	if err := commit(); err != nil {
		return 0, 0, err
	}

	return pk, n, nil
}

func (t TableImpl) Update(ctx context.Context, val any) (int, error) {
	// analyze reflect
	s, err := reflect.SchemaOf(val, schema.Enums(t.table.Schema().Enums.Load()))
	if err != nil {
		return 0, err
	}
	// check schema matches
	if t.table.Schema().Hash != s.Hash {
		return 0, schema.ErrSchemaMismatch
	}

	// encode wire (single or slice) - schema is guaranteed the same
	// but we must use the one derived from Go type for struct read
	if t.enc == nil {
		// analyze struct layout
		l, err := reflect.LayoutOf(val, s)
		if err != nil {
			return 0, err
		}
		t.enc = encode.NewEncoderWithLayout(s, l)
	}
	buf, err := t.enc.Encode(val, nil)
	if err != nil {
		return 0, err
	}

	// use or open tx
	ctx, commit, abort, err := t.db.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer abort()

	// call backend
	n, err := t.table.UpdateRows(ctx, buf)
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

func TableFor[T any](t Table) (*TableT[T], error) {
	return FindTableFor[T](t.DB(), t.Schema().Name)
}

func FindTableFor[T any](db Database, name string) (*TableT[T], error) {
	table, err := db.FindTable(name)
	if err != nil {
		return nil, err
	}
	var t T
	s, err := reflect.SchemaOf(t, schema.Enums(table.Schema().Enums.Load()))
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

func (t *TableT[T]) Insert(ctx context.Context, val any) (uint64, int, error) {
	var (
		buf []byte
		err error
	)
	if t.enc == nil {
		t.enc = encode.NewEncoderFor[T](schema.Enums(t.Schema().Enums.Load()))
	}
	switch v := val.(type) {
	case *T:
		buf, err = t.enc.EncodePtr(v, nil)
	case []T:
		buf, err = t.enc.EncodeSlice(v, nil)
	case []*T:
		buf, err = t.enc.EncodePtrSlice(v, nil)
	default:
		return 0, 0, fmt.Errorf("insert: %T %w", val, err)
	}
	if err != nil {
		return 0, 0, err
	}

	// use or open tx
	ctx, commit, abort, err := t.db.Begin(ctx)
	if err != nil {
		return 0, 0, err
	}
	defer abort()

	// call backend, returns first sequential pk assigned
	pk, n, err := t.table.InsertRows(ctx, buf)
	if err != nil {
		return 0, 0, err
	}

	if err := commit(); err != nil {
		return 0, 0, err
	}

	// assign primary keys to all values
	pkOffset := t.enc.Offset(t.schema.PkIndex())
	switch v := val.(type) {
	case *T:
		*(*uint64)(unsafe.Add(unsafe.Pointer(v), pkOffset)) = pk
	case []T:
		for i := range v {
			*(*uint64)(unsafe.Add(unsafe.Pointer(&v[i]), pkOffset)) = pk
		}
	case []*T:
		for i := range v {
			*(*uint64)(unsafe.Add(unsafe.Pointer(v[i]), pkOffset)) = pk
		}
	}

	return pk, n, nil
}

func (t *TableT[T]) Update(ctx context.Context, val any) (int, error) {
	var (
		buf []byte
		err error
	)
	if t.enc == nil {
		t.enc = encode.NewEncoderFor[T](schema.Enums(t.Schema().Enums.Load()))
	}
	switch v := val.(type) {
	case *T:
		buf, err = t.enc.EncodePtr(v, nil)
	case []T:
		buf, err = t.enc.EncodeSlice(v, nil)
	case []*T:
		buf, err = t.enc.EncodePtrSlice(v, nil)
	default:
		return 0, fmt.Errorf("update: %T %w", val, err)
	}
	if err != nil {
		return 0, err
	}

	// use or open tx
	ctx, commit, abort, err := t.db.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer abort()

	// call backend
	n, err := t.table.UpdateRows(ctx, buf)
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
