// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package operator

import (
	"context"

	"blockwatch.cc/knoxdb/internal/engine"
	"blockwatch.cc/knoxdb/internal/pack"
)

// TODO: run journal query and merge journal result

var _ PullOperator = (*PhysicalTableScan)(nil)

type PhysicalTableScan struct {
	r   engine.TableReader
	err error
}

func NewPhysicalTableScan(t engine.TableEngine, plan engine.QueryPlan) *PhysicalTableScan {
	return &PhysicalTableScan{
		r: t.NewReader().WithQuery(plan),
	}
}

func (op *PhysicalTableScan) Next(ctx context.Context) (*pack.Package, Result) {
	pkg, err := op.r.Next(ctx)
	if err != nil {
		op.err = err
		return nil, ResultError
	}
	if pkg == nil {
		return nil, ResultDone
	}
	return pkg, ResultOK
}

func (op *PhysicalTableScan) Err() error {
	return op.err
}

func (op *PhysicalTableScan) Close() {
	op.r.Close()
	op.err = nil
}
