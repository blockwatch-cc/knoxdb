// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package operator

import (
	"context"

	"blockwatch.cc/knoxdb/internal/pack"
)

var _ PushOperator = (*PhysicalUpdater)(nil)

type PhysicalUpdater struct {
	// cols []int
	// vm   []*expr.Vm
	err error
}

func (op *PhysicalUpdater) Process(ctx context.Context, src *pack.Package) (*pack.Package, Result) {
	op.err = ErrTodo
	return nil, ResultError
}

func (op *PhysicalUpdater) Finalize(ctx context.Context) error {
	return nil
}

func (op *PhysicalUpdater) Err() error {
	return op.err
}

func (op *PhysicalUpdater) Close() {
	op.err = nil
}
