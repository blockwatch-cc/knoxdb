// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package query

import (
	"context"

	"blockwatch.cc/knoxdb/internal/engine"
	"blockwatch.cc/knoxdb/internal/xroar"
	"blockwatch.cc/knoxdb/pkg/schema"
)

var (
	_ engine.QueryableIndex = (*MockIndex)(nil)
	_ engine.QueryableTable = (*MockTable)(nil)
)

type MockIndex struct {
	schema *schema.Schema
	result *xroar.Bitmap
}

func NewMockIndex(s *schema.Schema, result *xroar.Bitmap) engine.QueryableIndex {
	return &MockIndex{
		schema: s,
		result: result,
	}
}

func (idx *MockIndex) IsPk() bool {
	return true
}

func (idx *MockIndex) Schema() *schema.Schema {
	return idx.schema
}

func (idx *MockIndex) IsComposite() bool {
	return false
}

func (idx *MockIndex) CanMatch(node engine.QueryCondition) bool {
	f, ok := idx.schema.FieldByIndex(0)
	if !ok {
		return false
	}
	return f.Name() == node.Fields()[0]
}

func (idx *MockIndex) Query(_ context.Context, _ engine.QueryCondition) (*xroar.Bitmap, bool, error) {
	return idx.result, false, nil
}

func (idx *MockIndex) QueryComposite(_ context.Context, _ engine.QueryCondition) (*xroar.Bitmap, bool, error) {
	return idx.result, false, nil
}

func (idx *MockIndex) Lookup(_ context.Context, pks []uint64, ridMap map[uint64]uint64) error {
	for _, v := range pks {
		ridMap[v] = v
	}
	return nil
}

type MockTable struct {
	schema  *schema.Schema
	indexes []engine.QueryableIndex
	result  engine.QueryResult
}

func NewMockTable(s *schema.Schema, idxs []engine.QueryableIndex, res engine.QueryResult) engine.QueryableTable {
	return &MockTable{
		schema:  s,
		indexes: idxs,
		result:  res,
	}
}

func (t *MockTable) Schema() *schema.Schema {
	return t.schema
}

func (t *MockTable) Indexes() []engine.QueryableIndex {
	return t.indexes
}

func (t *MockTable) Query(_ context.Context, _ engine.QueryPlan) (engine.QueryResult, error) {
	return t.result, nil
}

func (t *MockTable) Stream(_ context.Context, _ engine.QueryPlan, fn func(engine.QueryRow) error) error {
	var err error
	for _, r := range t.result.Iterator() {
		err = fn(r)
		if err != nil {
			break
		}
	}
	return nil
}
