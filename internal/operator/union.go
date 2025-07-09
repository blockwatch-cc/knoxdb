// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package operator

import (
	"context"

	"blockwatch.cc/knoxdb/internal/pack"
)

var _ PushOperator = (*PhysicalUnion)(nil)

type PhysicalUnion struct {
	err error
}

func (op *PhysicalUnion) Process(ctx context.Context, src *pack.Package) (*pack.Package, Result) {
	op.err = ErrTodo
	return nil, ResultError
}

func (op *PhysicalUnion) Finalize(ctx context.Context) error {
	return nil
}

func (op *PhysicalUnion) Err() error {
	return op.err
}

func (op *PhysicalUnion) Close() {
	op.err = nil
}
