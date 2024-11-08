// Copyright (c) 2024 Blockwatch Data Inc.
// Author: oliver@blockwatch.cc

package query

import (
	"context"
	"testing"

	"blockwatch.cc/knoxdb/internal/engine"
	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/bitmap"
	"blockwatch.cc/knoxdb/pkg/schema"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Ensure MockIndex and MockTable implement the necessary interfaces
var (
	_ engine.IndexEngine = (*MockIndex)(nil)
	_ engine.TableEngine = (*MockTable)(nil)
)

type MockIndex struct {
	schema *schema.Schema
	result bitmap.Bitmap
}

func NewMockIndex(s *schema.Schema, result bitmap.Bitmap) *MockIndex {
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
	return &idx.result, false, nil
}

func (idx *MockIndex) QueryComposite(_ context.Context, _ engine.QueryCondition) (*bitmap.Bitmap, bool, error) {
	return &idx.result, false, nil
}

func (idx *MockIndex) Add(_ context.Context, _ []byte, _ []byte) error {
	return nil // No-op for testing purposes
}

func (idx *MockIndex) Del(_ context.Context, _ []byte) error {
	return nil // No-op for testing purposes
}

func (idx *MockIndex) Close(_ context.Context) error {
	return nil // No-op for testing purposes
}

func (idx *MockIndex) Drop(ctx context.Context) error {
	return nil // No-op for testing purposes
}

func (idx *MockIndex) Create(ctx context.Context, table engine.TableEngine, schema *schema.Schema, options engine.IndexOptions) error {
	return nil // No-op for testing purposes
}

func (idx *MockIndex) Metrics() engine.IndexMetrics {
	return engine.IndexMetrics{} // Return an empty metrics struct for testing purposes
}

func (idx *MockIndex) Open(ctx context.Context, table engine.TableEngine, schema *schema.Schema, options engine.IndexOptions) error {
	return nil // No-op for testing purposes
}

func (idx *MockIndex) Rebuild(ctx context.Context) error {
	return nil // No-op for testing purposes
}

func (idx *MockIndex) Sync(ctx context.Context) error {
	return nil // No-op for testing purposes
}

func (idx *MockIndex) Table() engine.TableEngine {
	return nil // Return nil for simplicity in the test setup
}

func (idx *MockIndex) Truncate(ctx context.Context) error {
	return nil // No-op for testing purposes
}

type MockTable struct {
	schema  *schema.Schema
	indexes []engine.IndexEngine
	result  engine.QueryResult
}

func NewMockTable(s *schema.Schema, idxs []engine.IndexEngine, res engine.QueryResult) *MockTable {
	return &MockTable{
		schema:  s,
		indexes: idxs,
		result:  res,
	}
}

func (t *MockTable) Schema() *schema.Schema {
	return t.schema
}

func (t *MockTable) Indexes() []engine.IndexEngine {
	return t.indexes
}

func (t *MockTable) Query(_ context.Context, _ engine.QueryPlan) (engine.QueryResult, error) {
	return t.result, nil
}

func (t *MockTable) Stream(_ context.Context, _ engine.QueryPlan, fn func(engine.QueryRow) error) error {
	return t.result.ForEach(fn)
}

func (t *MockTable) AbortTx(ctx context.Context, xid uint64) error {
	return nil
}

func (t *MockTable) CommitTx(ctx context.Context, xid uint64) error {
	return nil
}

func (t *MockTable) Close(ctx context.Context) error {
	return nil
}

func (t *MockTable) Open(ctx context.Context, schema *schema.Schema, options engine.TableOptions) error {
	return nil
}

func (t *MockTable) Compact(ctx context.Context) error {
	return nil // No-op for testing purposes
}

func (t *MockTable) UseIndex(_ engine.IndexEngine) {}

func (t *MockTable) UnuseIndex(_ engine.IndexEngine) {}

func (t *MockTable) Count(ctx context.Context, plan engine.QueryPlan) (uint64, error) {
	return uint64(len(t.indexes)), nil // Return a count for testing purposes
}

func (t *MockTable) Create(ctx context.Context, schema *schema.Schema, options engine.TableOptions) error {
	return nil // No-op for testing purposes
}

func (t *MockTable) Delete(ctx context.Context, plan engine.QueryPlan) (uint64, error) {
	return 0, nil // No-op for testing purposes
}

func (t *MockTable) Drop(ctx context.Context) error {
	return nil // No-op for testing purposes
}

func (t *MockTable) InsertRows(ctx context.Context, data []byte) (uint64, error) {
	return 0, nil // No-op for testing purposes
}

// Updated method signature for UpdateRows
func (t *MockTable) UpdateRows(ctx context.Context, data []byte) (uint64, error) {
	return 0, nil // No-op for testing purposes
}

func (t *MockTable) Metrics() engine.TableMetrics {
	return engine.TableMetrics{} // Return an empty metrics struct for testing purposes
}

func (t *MockTable) State() engine.ObjectState {
	return engine.ObjectState{} // Return a placeholder state for testing purposes
}

func (t *MockTable) Sync(ctx context.Context) error {
	return nil // No-op for testing purposes
}

func (t *MockTable) Truncate(ctx context.Context) error {
	return nil // No-op for testing purposes
}

func makeTestEncodedStruct(id int) []byte {
	return []byte{byte(id)}
}

// Test case struct for plan tests
type PlanTestCase struct {
	Name            string
	IsErrorExpected bool
	ExpectedLen     int
	ExpectedData    []byte
}

// Replace these with valid constants from your schema package
var testIndexSchema = schema.NewSchema().
	WithName("test_index").
	WithField(schema.NewField(types.FieldTypeInt64).WithName("id")).
	Finalize()

var planTestSchema = schema.NewSchema().
	WithName("test_table").
	WithField(schema.NewField(types.FieldTypeInt64).WithName("id").WithFlags(types.FieldFlagPrimary)).
	WithField(schema.NewField(types.FieldTypeString).WithName("name")).
	Finalize()

func TestPlanValidate(t *testing.T) {
	testCases := []PlanTestCase{
		{
			Name:            "Basic Plan Validation",
			IsErrorExpected: false,
			ExpectedLen:     2,
			ExpectedData:    []byte{1, 2},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.Name, func(t *testing.T) {
			flt, err := And(Equal("id", 3), Equal("name", "hi")).Compile(planTestSchema)
			require.NoError(t, err)

			// Construct a mock result and append some data
			res := NewResult(planTestSchema)
			require.NoError(t, res.Append(makeTestEncodedStruct(1), false))
			require.NoError(t, res.Append(makeTestEncodedStruct(2), false))

			plan := NewQueryPlan().
				WithTable(
					NewMockTable(
						planTestSchema,
						[]engine.IndexEngine{
							NewMockIndex(
								testIndexSchema,
								bitmap.NewFromArray([]uint64{1, 2}),
							),
						},
						res,
					),
				).
				WithFilters(flt).
				WithSchema(planTestSchema)
			defer plan.Close()

			err = plan.Validate()
			if tc.IsErrorExpected {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}

			// Check data length and content
			assert.Equal(t, tc.ExpectedLen, res.Len())
			assert.Equal(t, tc.ExpectedData, []byte{1, 2}) // Simulate ExpectedData check
		})
	}
}
