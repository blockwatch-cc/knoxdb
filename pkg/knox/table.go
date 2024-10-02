// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package knox

import (
	"context"
	"fmt"
	"unsafe"

	"blockwatch.cc/knoxdb/internal/engine"
	"blockwatch.cc/knoxdb/pkg/schema"
	"github.com/echa/log"
)

var _ Table = (*TableImpl)(nil)

type TableImpl struct {
	table engine.TableEngine
	db    Database
	log   log.Logger
}

func (t TableImpl) Schema() *schema.Schema {
	return t.table.Schema()
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

func (t TableImpl) Insert(ctx context.Context, val any) (uint64, error) {
	// analyze reflect
	s, err := schema.SchemaOf(val)
	if err != nil {
		return 0, err
	}
	// check schema matches
	if !t.table.Schema().EqualHash(s.Hash()) {
		return 0, schema.ErrSchemaMismatch
	}

	// encode wire (single or slice) - schema is guaranteed the same
	enc := schema.NewEncoder(s)
	buf, err := enc.Encode(val, nil)
	if err != nil {
		return 0, err
	}

	// use or open tx
	ctx, commit, abort := t.db.Begin(ctx)
	defer abort()

	// call backend
	n, err := t.table.InsertRows(ctx, buf)
	if err != nil {
		return 0, err
	}

	// commit/abort
	if err := commit(); err != nil {
		return 0, err
	}

	return n, nil
}

func (t TableImpl) Update(ctx context.Context, val any) (uint64, error) {
	// analyze reflect
	s, err := schema.SchemaOf(val)
	if err != nil {
		return 0, err
	}
	// check schema matches
	if !t.table.Schema().EqualHash(s.Hash()) {
		return 0, schema.ErrSchemaMismatch
	}

	// encode wire (single or slice) - schema is guaranteed the same
	enc := schema.NewEncoder(s)
	buf, err := enc.Encode(val, nil)
	if err != nil {
		return 0, err
	}

	// use or open tx
	ctx, commit, abort := t.db.Begin(ctx)
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

func (t TableImpl) Delete(ctx context.Context, q QueryRequest) (uint64, error) {
	plan, err := q.MakePlan()
	if err != nil {
		return 0, err
	}

	if err := plan.Compile(ctx); err != nil {
		return 0, err
	}

	// use or open tx
	ctx, commit, abort := t.db.Begin(ctx)
	defer abort()

	n, err := t.table.Delete(ctx, plan)
	if err != nil {
		return 0, err
	}

	if err := commit(); err != nil {
		return 0, err
	}

	return n, nil
}

func (t TableImpl) Count(ctx context.Context, q QueryRequest) (uint64, error) {
	plan, err := q.MakePlan()
	if err != nil {
		return 0, err
	}

	if err := plan.Compile(ctx); err != nil {
		return 0, err
	}

	// use or open tx
	ctx, commit, abort := t.db.Begin(ctx)
	defer abort()

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

	if err := plan.Compile(ctx); err != nil {
		return nil, err
	}

	// use or open tx
	ctx, commit, abort := t.db.Begin(ctx)
	defer abort()

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

	if err := plan.Compile(ctx); err != nil {
		return err
	}

	// use or open tx
	ctx, commit, abort := t.db.Begin(ctx)
	defer abort()

	if err := t.table.Stream(ctx, plan, fn); err != nil {
		return err
	}

	return commit()
}

var _ Table = (*GenericTable[int])(nil)

// GenericTable[T] implements Table interface for Go struct types
type GenericTable[T any] struct {
	schema *schema.Schema
	table  engine.TableEngine
	db     Database
}

func UseGenericTable[T any](name string, db Database) (*GenericTable[T], error) {
	var t T
	s, err := schema.SchemaOf(t)
	if err != nil {
		return nil, err
	}
	table, err := db.UseTable(name)
	if err != nil {
		return nil, err
	}
	// check schema matches
	if !table.Schema().EqualHash(s.Hash()) {
		return nil, schema.ErrSchemaMismatch
	}
	return &GenericTable[T]{
		schema: table.Schema(),
		table:  table.(*TableImpl).table,
		db:     db,
	}, nil
}

func (t *GenericTable[T]) Name() string {
	return t.schema.Name()
}

func (t *GenericTable[T]) Schema() *schema.Schema {
	return t.schema
}

func (t *GenericTable[T]) Engine() engine.TableEngine {
	return t.table
}

func (t *GenericTable[T]) Metrics() TableMetrics {
	return t.table.Metrics()
}

func (t *GenericTable[T]) Table() Table {
	return &TableImpl{
		table: t.table,
		db:    t.db,
		log:   log.Disabled,
	}
}
func (t *GenericTable[T]) DB() Database {
	return t.db
}

func (t *GenericTable[T]) Insert(ctx context.Context, val any) (uint64, error) {
	enc := schema.NewGenericEncoder[T]()
	var (
		buf []byte
		err error
	)
	switch v := val.(type) {
	case *T:
		buf, err = enc.EncodePtr(v, nil)
	case []T:
		buf, err = enc.EncodeSlice(v, nil)
	case []*T:
		buf, err = enc.EncodePtrSlice(v, nil)
	default:
		return 0, fmt.Errorf("insert: %T %w", val, schema.ErrInvalidValueType)
	}
	if err != nil {
		return 0, err
	}

	// use or open tx
	ctx, commit, abort := t.db.Begin(ctx)
	defer abort()

	// call backend
	n, err := t.table.InsertRows(ctx, buf)
	if err != nil {
		return 0, err
	}

	if err := commit(); err != nil {
		return 0, err
	}

	// assign primary keys to all values, return above is first sequential pk assigned
	pkfield := t.schema.Pk()
	switch v := val.(type) {
	case *T:
		*(*uint64)(unsafe.Add(unsafe.Pointer(v), pkfield.Offset())) = n
	case []T:
		for i := range v {
			*(*uint64)(unsafe.Add(unsafe.Pointer(&v[i]), pkfield.Offset())) = n
			n++
		}
	case []*T:
		for i := range v {
			*(*uint64)(unsafe.Add(unsafe.Pointer(v[i]), pkfield.Offset())) = n
			n++
		}
	}

	return n, nil
}

func (t *GenericTable[T]) Update(ctx context.Context, val any) (uint64, error) {
	enc := schema.NewGenericEncoder[T]()
	var (
		buf []byte
		err error
	)
	switch v := val.(type) {
	case *T:
		buf, err = enc.EncodePtr(v, nil)
	case []T:
		buf, err = enc.EncodeSlice(v, nil)
	case []*T:
		buf, err = enc.EncodePtrSlice(v, nil)
	default:
		return 0, fmt.Errorf("update: %T %w", val, schema.ErrInvalidValueType)
	}
	if err != nil {
		return 0, err
	}

	// use or open tx
	ctx, commit, abort := t.db.Begin(ctx)
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

func (t *GenericTable[T]) Delete(ctx context.Context, q QueryRequest) (uint64, error) {
	plan, err := q.MakePlan()
	if err != nil {
		return 0, err
	}

	// use or open tx
	ctx, commit, abort := t.db.Begin(ctx)
	defer abort()

	n, err := t.table.Delete(ctx, plan)
	if err != nil {
		return 0, err
	}

	if err := commit(); err != nil {
		return 0, err
	}

	return n, nil
}

func (t *GenericTable[T]) Count(ctx context.Context, q QueryRequest) (uint64, error) {
	plan, err := q.MakePlan()
	if err != nil {
		return 0, err
	}

	// use or open tx
	ctx, commit, abort := t.db.Begin(ctx)
	defer abort()

	n, err := t.table.Count(ctx, plan)
	if err != nil {
		return 0, err
	}

	if err := commit(); err != nil {
		return 0, err
	}

	return n, nil
}

func (t *GenericTable[T]) Query(ctx context.Context, q QueryRequest) (QueryResult, error) {
	plan, err := q.MakePlan()
	if err != nil {
		return nil, err
	}

	// use or open tx
	ctx, commit, abort := t.db.Begin(ctx)
	defer abort()

	res, err := t.table.Query(ctx, plan)
	if err != nil {
		return nil, err
	}

	if err := commit(); err != nil {
		return nil, err
	}

	return res, nil
}

func (t *GenericTable[T]) Stream(ctx context.Context, q QueryRequest, fn func(QueryRow) error) error {
	plan, err := q.MakePlan()
	if err != nil {
		return err
	}

	// use or open tx
	ctx, commit, abort := t.db.Begin(ctx)
	defer abort()

	if err := t.table.Stream(ctx, plan, fn); err != nil {
		return err
	}

	return commit()
}
