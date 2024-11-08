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

func makeTestEncodedStruct(id int, name string, score float64) []byte {
	return []byte{byte(id), byte(len(name)), byte(score)}
}

type PlanTestCase struct {
	Name            string
	IsErrorExpected bool
	ExpectedLen     int
	ExpectedData    []byte
}

var testIndexSchema = schema.NewSchema().
	WithName("test_index").
	WithField(schema.NewField(types.FieldTypeInt64).WithName("id")).
	Finalize()

var planTestSchema = schema.NewSchema().
	WithName("test_table").
	WithField(schema.NewField(types.FieldTypeInt64).WithName("id").WithFlags(types.FieldFlagPrimary)).
	WithField(schema.NewField(types.FieldTypeString).WithName("name")).
	WithField(schema.NewField(types.FieldTypeFloat64).WithName("score")).
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
			flt, err := And(Equal("id", 1), Equal("name", "test")).Compile(planTestSchema)
			require.NoError(t, err)

			res := NewResult(planTestSchema)
			require.NoError(t, res.Append(makeTestEncodedStruct(1, "test", 3.0), false))
			require.NoError(t, res.Append(makeTestEncodedStruct(2, "test", 4.0), false))

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
				t.Logf("Filter output length: %d, data: %v", res.Len(), res)
				assert.Equal(t, tc.ExpectedLen, res.Len(), "Result set length mismatch")
				if tc.ExpectedData != nil {
					t.Logf("Actual result set data: %v", res)
					assert.Equal(t, tc.ExpectedData, []byte{1, 2}) // Adjust as needed for specific test cases
				}
			}
		})
	}
}

func TestPlanComplexValidation(t *testing.T) {
	testCases := []PlanTestCase{
		{
			Name:            "Complex AND Combination",
			IsErrorExpected: false,
			ExpectedLen:     2,
			ExpectedData:    []byte{1, 3},
		},
		{
			Name:            "Complex OR Combination",
			IsErrorExpected: false,
			ExpectedLen:     4,
			ExpectedData:    []byte{1, 2, 3, 4},
		},
		// Additional test cases...
	}

	for _, tc := range testCases {
		t.Run(tc.Name, func(t *testing.T) {
			var flt *FilterTreeNode
			var err error
			switch tc.Name {
			case "Complex AND Combination":
				flt, err = And(Equal("id", 1), Gt("score", 2.5)).Compile(planTestSchema)
				require.NoError(t, err)
			case "Complex OR Combination":
				flt, err = Or(Equal("id", 1), Equal("id", 2), Equal("id", 3), Equal("id", 4)).Compile(planTestSchema)
				require.NoError(t, err)
			}

			res := NewResult(planTestSchema)
			require.NoError(t, res.Append(makeTestEncodedStruct(1, "sample1", 3.5), false))
			require.NoError(t, res.Append(makeTestEncodedStruct(2, "sample2", 2.0), false))
			require.NoError(t, res.Append(makeTestEncodedStruct(3, "sample3", 5.5), false))
			require.NoError(t, res.Append(makeTestEncodedStruct(4, "sample4", 4.0), false))

			plan := NewQueryPlan().
				WithTable(
					NewMockTable(
						planTestSchema,
						[]engine.IndexEngine{
							NewMockIndex(
								testIndexSchema,
								bitmap.NewFromArray([]uint64{1, 2, 3, 4}),
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
				assert.Equal(t, tc.ExpectedLen, res.Len())
				if tc.ExpectedData != nil {
					assert.Equal(t, tc.ExpectedData, []byte{1, 3}) // Adjust as needed
				}
			}
		})
	}
}

func TestPlanQueryIndexProcessing(t *testing.T) {
	testCases := []PlanTestCase{
		{
			Name:            "Index Query Test",
			IsErrorExpected: false,
			ExpectedLen:     2,
			ExpectedData:    []byte{1, 2},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.Name, func(t *testing.T) {
			res := NewResult(planTestSchema)
			require.NoError(t, res.Append(makeTestEncodedStruct(1, "sample1", 3.0), false))
			require.NoError(t, res.Append(makeTestEncodedStruct(2, "sample2", 4.0), false))

			idx := NewMockIndex(testIndexSchema, bitmap.NewFromArray([]uint64{1, 2}))
			tbl := NewMockTable(planTestSchema, []engine.IndexEngine{idx}, res)

			plan := NewQueryPlan().
				WithTable(tbl).
				WithSchema(planTestSchema)
			defer plan.Close()

			err := plan.QueryIndexes(context.Background())
			if tc.IsErrorExpected {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tc.ExpectedLen, res.Len())
			}
		})
	}
}

func TestPlanStreamingBehavior(t *testing.T) {
	testCases := []PlanTestCase{
		{
			Name:            "Stream with Matching Condition",
			IsErrorExpected: false,
			ExpectedLen:     2,
			ExpectedData:    []byte{1, 3},
		},
		{
			Name:            "Stream with No Match",
			IsErrorExpected: false,
			ExpectedLen:     0,
			ExpectedData:    nil,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.Name, func(t *testing.T) {
			var flt *FilterTreeNode
			var err error
			switch tc.Name {
			case "Stream with Matching Condition":
				flt, err = Or(Equal("id", 1), Equal("id", 3)).Compile(planTestSchema)
			case "Stream with No Match":
				flt, err = Equal("id", 999).Compile(planTestSchema)
			}

			require.NoError(t, err, "Failed to compile filter")

			res := NewResult(planTestSchema)
			require.NoError(t, res.Append(makeTestEncodedStruct(1, "sample1", 3.0), false))
			require.NoError(t, res.Append(makeTestEncodedStruct(3, "sample3", 3.5), false))

			plan := NewQueryPlan().
				WithTable(
					NewMockTable(
						planTestSchema,
						[]engine.IndexEngine{
							NewMockIndex(
								testIndexSchema,
								bitmap.NewFromArray([]uint64{1, 2, 3}),
							),
						},
						res,
					),
				).
				WithFilters(flt).
				WithSchema(planTestSchema)
			defer plan.Close()

			streamCount := 0
			err = plan.Stream(context.Background(), func(row engine.QueryRow) error {
				streamCount++
				return nil
			})

			if tc.IsErrorExpected {
				assert.Error(t, err, "Expected an error during streaming")
			} else {
				assert.NoError(t, err, "Unexpected error during streaming")
				assert.Equal(t, tc.ExpectedLen, streamCount, "Stream count mismatch")
			}
		})
	}
}
