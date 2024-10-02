// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package knox

import (
	"context"
	"errors"
	"fmt"

	"blockwatch.cc/knoxdb/internal/engine"
	"blockwatch.cc/knoxdb/pkg/schema"
)

var (
	ErrDatabaseExists = engine.ErrDatabaseExists
	ErrNoPointer      = errors.New("expected pointer value")

	ErrNotImplemented = errors.New("not implemented")
	ErrNoTable        = errors.New("missing table, use WithTable()")
)

// placeholder for an undefined table, helps delay raising an error
// until first API call happens
type errorTable struct {
	name string
	err  error
}

func newErrorTable(name string, err error) Table {
	return &errorTable{
		name: name,
		err:  fmt.Errorf("%s: %w", name, err),
	}
}

func (t *errorTable) Name() string                                                 { return t.name }
func (t *errorTable) Metrics() TableMetrics                                        { return TableMetrics{} }
func (t *errorTable) DB() Database                                                 { return nil }
func (t *errorTable) Schema() *schema.Schema                                       { return &schema.Schema{} }
func (t *errorTable) Engine() engine.TableEngine                                   { return nil }
func (t *errorTable) Insert(_ context.Context, _ any) (uint64, error)              { return 0, t.err }
func (t *errorTable) Update(_ context.Context, _ any) (uint64, error)              { return 0, t.err }
func (t *errorTable) Delete(_ context.Context, _ QueryRequest) (uint64, error)     { return 0, t.err }
func (t *errorTable) Query(_ context.Context, _ QueryRequest) (QueryResult, error) { return nil, t.err }
func (t *errorTable) Count(_ context.Context, _ QueryRequest) (uint64, error)      { return 0, t.err }
func (t *errorTable) Stream(_ context.Context, _ QueryRequest, _ func(QueryRow) error) error {
	return t.err
}
