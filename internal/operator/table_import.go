// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package operator

import (
	"context"

	"blockwatch.cc/knoxdb/internal/engine"
	"blockwatch.cc/knoxdb/internal/pack"
)

var _ PushOperator = (*PhysicalTableImporter)(nil)

type PhysicalTableImporter struct {
	table engine.TableEngine
	err   error
}

func NewPhysicalTableImporter(t engine.TableEngine) *PhysicalTableImporter {
	return &PhysicalTableImporter{table: t}
}

func (op *PhysicalTableImporter) Process(ctx context.Context, src *pack.Package) (*pack.Package, Result) {
	_, _, err := op.table.ImportInto(ctx, src)
	if err != nil {
		op.err = err
		return nil, ResultError
	}
	return nil, ResultOK
}

func (op *PhysicalTableImporter) Finalize(_ context.Context) error {
	return nil
}

func (op *PhysicalTableImporter) Err() error {
	return op.err
}

func (op *PhysicalTableImporter) Close() {
	op.table = nil
	op.err = nil
}
