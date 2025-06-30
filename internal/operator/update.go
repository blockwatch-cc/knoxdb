// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package operator

import (
	"context"

	"blockwatch.cc/knoxdb/internal/expr"
	"blockwatch.cc/knoxdb/internal/pack"
)

var _ PushOperator = (*UpdateOperator)(nil)

type UpdateOperator struct {
	cols []int
	vm   []*expr.Vm
	err  error
}

func (op *UpdateOperator) Process(ctx context.Context, src *pack.Package) (*pack.Package, Result) {
	op.err = ErrTodo
	return nil, ResultError
}

func (op *UpdateOperator) Finalize(ctx context.Context) (*pack.Package, Result) {
	op.err = ErrTodo
	return nil, ResultError
}

func (op *UpdateOperator) Err() error {
	return op.err
}

func (op *UpdateOperator) Close() {
	op.err = nil
}
