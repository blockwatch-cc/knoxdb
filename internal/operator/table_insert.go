// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package operator

import (
	"context"

	"blockwatch.cc/knoxdb/internal/engine"
	"blockwatch.cc/knoxdb/internal/pack"
)

var _ PushOperator = (*PhysicalTableInserter)(nil)

type PhysicalTableInserter struct {
	table engine.TableEngine
	err   error
}

func NewPhysicalTableInserter(t engine.TableEngine) *PhysicalTableInserter {
	return &PhysicalTableInserter{table: t}
}

func (op *PhysicalTableInserter) Process(ctx context.Context, src *pack.Package) (*pack.Package, Result) {
	_, err := op.table.InsertInto(ctx, src)
	if err != nil {
		op.err = err
		return nil, ResultError
	}
	return nil, ResultOK
}

func (op *PhysicalTableInserter) Finalize(ctx context.Context) error {
	return nil
}

func (op *PhysicalTableInserter) Err() error {
	return op.err
}

func (op *PhysicalTableInserter) Close() {
	op.table = nil
	op.err = nil
}
