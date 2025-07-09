// Copyright (c) 2025 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package operator

import (
	"context"

	"blockwatch.cc/knoxdb/internal/bitset"
	"blockwatch.cc/knoxdb/internal/operator/filter"
	"blockwatch.cc/knoxdb/internal/pack"
)

var _ PushOperator = (*PhysicalFilter)(nil)

type PhysicalFilter struct {
	node *filter.Node
	bits *bitset.Bitset
	err  error
}

func NewPhysicalFilter(node *filter.Node, sz int) *PhysicalFilter {
	return &PhysicalFilter{
		node: node,
		bits: bitset.New(sz),
	}
}

func (op *PhysicalFilter) Process(ctx context.Context, src *pack.Package) (*pack.Package, Result) {
	filter.Match(op.node, src, nil, op.bits.Resize(src.Len()).Zero())
	if op.bits.All() {
		src.WithSelection(nil)
	} else {
		src.WithSelection(op.bits.Indexes(nil))
	}
	return src, ResultOK
}

func (op *PhysicalFilter) Finalize(ctx context.Context) error {
	return nil
}

func (op *PhysicalFilter) Err() error {
	return op.err
}

func (op *PhysicalFilter) Close() {
	op.node = nil
	op.bits.Close()
	op.bits = nil
	op.err = nil
}
