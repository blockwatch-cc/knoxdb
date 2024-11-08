// Copyright (c) 2024 Blockwatch Data Inc.
// Author: oliver@blockwatch.cc

package query

import (
	"context"
	"testing"

	"blockwatch.cc/knoxdb/internal/engine"
	"blockwatch.cc/knoxdb/pkg/bitmap"
	"blockwatch.cc/knoxdb/pkg/schema"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	_ engine.IndexEngine = (*MockIndex)(nil)
	_ engine.TableEngine = (*MockTable)(nil)
)

// MockIndex implements the engine.IndexEngine interface
type MockIndex struct {
	schema     *schema.Schema
	result     bitmap.Bitmap
	queryCount int
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
	idx.queryCount++
	return &idx.result, false, nil
}

func (idx *MockIndex) QueryComposite(_ context.Context, _ engine.QueryCondition) (*bitmap.Bitmap, bool, error) {
	idx.queryCount++
	return &idx.result, false, nil
}

func (idx *MockIndex) Add(_ context.Context, _ []byte, _ []byte) error {
	return nil // Implement as a no-op for test purposes
}

func (idx *MockIndex) Del(_ context.Context, _ []byte) error {
	return nil // Implement as a no-op for test purposes
}

func (idx *MockIndex) Close(ctx context.Context) error {
	return nil // Implement as a no-op for test purposes
}

func (idx *MockIndex) Create(ctx context.Context, table engine.TableEngine, schema *schema.Schema, options engine.IndexOptions) error {
	return nil // Implement as a no-op for test purposes
}

func (idx *MockIndex) Drop(ctx context.Context) error {
	return nil // Implement as a no-op for test purposes
}

// MockTable implements the engine.TableEngine interface
type MockTable struct {
	schema  *schema.Schema
	indexes []engine.IndexEngine
	result  engine.QueryResult
}

func NewMockTable(s *schema.Schema, idxs []engine.IndexEngine, res engine.QueryResult) engine.TableEngine {
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

func (t *MockTable) InsertRows(_ context.Context, _ []byte) (uint64, error) {
	return 0, nil // Implement as a no-op for test purposes
}

func (t *MockTable) UpdateRows(_ context.Context, _ []byte, _ []byte) (uint64, error) {
	return 0, nil // Implement as a no-op for test purposes
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

func (t *MockTable) Open(ctx context.Context, _ *schema.Schema, _ engine.TableOptions) error {
	return nil // Implement as a no-op for test purposes
}

func (t *MockTable) Compact(ctx context.Context) error {
	return nil
}

func (t *MockTable) Count(ctx context.Context, plan engine.QueryPlan) (uint64, error) {
	return 0, nil
}

func (t *MockTable) Create(ctx context.Context, schema *schema.Schema, options engine.TableOptions) error {
	return nil
}

func (t *MockTable) Delete(ctx context.Context, plan engine.QueryPlan) (uint64, error) {
	return 0, nil
}

func (t *MockTable) Drop(ctx context.Context) error {
	return nil
}

func (t *MockTable) Metrics() engine.TableMetrics {
	return engine.TableMetrics{} // Return an empty metrics struct for testing purposes
}

func (t *MockTable) State() engine.ObjectState {
	return engine.ObjectState{} // Return a placeholder state for testing
}

func (t *MockTable) Sync(ctx context.Context) error {
	return nil // Implement as a no-op for test purposes
}

func (t *MockTable) Truncate(ctx context.Context) error {
	return nil // Implement as a no-op for test purposes
}

func (t *MockTable) UnuseIndex(_ engine.IndexEngine) {
	// Implement as a no-op for test purposes
}

// Mock function for makeEncodedTestStruct
func makeEncodedTestStruct(id int) []byte {
	return []byte{byte(id)}
}

// Define testIndexSchema
var testIndexSchema = schema.NewSchema().
	WithName("test_index").
	WithField(schema.NewField(schema.FieldTypeInt64).WithName("id")). // Corrected type
	Finalize()

func NewMatcher(name string) Matcher {
	return &SimpleMatcher{name: name}
}

// SimpleMatcher implements a basic Matcher for testing purposes
type SimpleMatcher struct {
	name string
}

func (m *SimpleMatcher) Match(v interface{}) bool {
	return true // Simplified matching logic for testing
}

func (m *SimpleMatcher) Len() int {
	return 1
}

func (m *SimpleMatcher) MatchBitmap(_ *bitmap.Bitmap) bool {
	return true // Implement as a simplified matching logic
}

func TestPlanExecution(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name     string
		setup    func() (*QueryPlan, *MockIndex)
		validate func(*testing.T, *QueryPlan, *MockIndex)
		wantErr  bool
	}{
		{
			name: "Simple Filter with Index",
			setup: func() (*QueryPlan, *MockIndex) {
				idx := NewMockIndex(testIndexSchema, bitmap.NewFromArray([]uint64{1, 2}))
				tbl := NewMockTable(testSchema, []engine.IndexEngine{idx}, NewResult(testSchema))

				filter := &FilterTreeNode{
					Filter: &Filter{
						Name:    "id",
						Mode:    FilterModeEqual,
						Value:   int64(1),
						Matcher: NewMatcher("id"),
					},
				}

				plan := NewQueryPlan().
					WithTable(tbl).
					WithIndex(idx).
					WithFilters(filter).
					WithLogger(nil)

				return plan, idx
			},
			validate: func(t *testing.T, p *QueryPlan, idx *MockIndex) {
				assert.Equal(t, 1, idx.queryCount, "Index should be queried once")
				assert.True(t, p.Filters.IsProcessed(), "Filters should be processed")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			plan, idx := tt.setup()

			err := plan.Compile(ctx)
			require.NoError(t, err, "Compiling the plan should not error")

			err = plan.QueryIndexes(ctx)
			if tt.wantErr {
				require.Error(t, err, "Expected an error during QueryIndexes")
				return
			}
			require.NoError(t, err, "QueryIndexes should not error")

			tt.validate(t, plan, idx)
		})
	}
}
