// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package query

import (
	"context"
	"testing"

	"blockwatch.cc/knoxdb/internal/engine"
	"blockwatch.cc/knoxdb/pkg/bitmap"
	"blockwatch.cc/knoxdb/pkg/schema"
	"github.com/stretchr/testify/require"
)

var (
	_ engine.QueryableIndex = (*MockIndex)(nil)
	_ engine.QueryableTable = (*MockTable)(nil)
)

type MockIndex struct {
	schema *schema.Schema
	result bitmap.Bitmap
}

func NewMockIndex(s *schema.Schema, result bitmap.Bitmap) engine.QueryableIndex {
	return &MockIndex{
		schema: s,
		result: result,
	}
}

func (idx *MockIndex) Schema() *schema.Schema {
	return idx.schema
}

func (idx *MockIndex) IsComposite() bool {
	return false
}

func (idx *MockIndex) CanMatch(_ engine.QueryCondition) bool {
	return true
}

func (idx *MockIndex) Query(_ context.Context, _ engine.QueryCondition) (*bitmap.Bitmap, bool, error) {
	// return &idx.result, true, nil
	return &idx.result, false, nil
}

func (idx *MockIndex) QueryComposite(_ context.Context, _ engine.QueryCondition) (*bitmap.Bitmap, bool, error) {
	return &idx.result, false, nil
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
	return t.result.ForEach(fn)
}

func TestPlanValidate(t *testing.T) {
	// define and compile initial filter conditions; the result, a tree of
	// FilterTreeNode nodes will get optimized and changed during query
	// execution steps
	flt, err := And(Equal("id", 3), Equal("name", "hi")).Compile(testSchema)
	require.NoError(t, err)

	// construct a mock result and append some data
	res := NewResult(testSchema)
	require.NoError(t, res.Append(makeEncodedTestStruct(1), false))
	require.NoError(t, res.Append(makeEncodedTestStruct(2), false))

	// construct a query plan for testing
	plan := NewQueryPlan().
		WithTable(
			NewMockTable(
				testSchema,
				// list of indexes derived from table
				[]engine.QueryableIndex{
					NewMockIndex(
						// index schema is a child of table schema
						testIndexSchema,
						// index query results (i.e. matching primary key values)
						bitmap.NewFromArray([]uint64{1, 2}),
					),
				},
				res,
			),
		).
		// WithFlags(QueryFlagNoIndex).
		// WithTag("test").
		// WithLogger(log.Log).
		// WithOrder(OrderDesc).
		// WithLimit(1).
		WithFilters(flt).
		WithSchema(testSchema)
	defer plan.Close()

	require.NoError(t, plan.Validate())
}
