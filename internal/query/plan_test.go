// Copyright (c) 2024 Blockwatch Data Inc.
// Author: oliver@blockwatch.cc

package query

import (
	"context"
	"fmt"
	"testing"

	"github.com/echa/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"blockwatch.cc/knoxdb/internal/bitset"
	"blockwatch.cc/knoxdb/internal/block"
	"blockwatch.cc/knoxdb/internal/engine"
	"blockwatch.cc/knoxdb/internal/filter/bloom"
	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/internal/xroar"
	"blockwatch.cc/knoxdb/pkg/bitmap"
	"blockwatch.cc/knoxdb/pkg/schema"
)

// Use existing mockTable from join_test.go but extend it
type planTestIndex struct {
	name       string
	schema     *schema.Schema
	canMatch   bool
	bits       *bitmap.Bitmap
	collides   bool
	composite  bool
	queryCount int
	table      engine.TableEngine
	fields     []string
}

// Existing methods
func (m *planTestIndex) Schema() *schema.Schema { return m.schema }
func (m *planTestIndex) IsComposite() bool      { return m.composite }
func (m *planTestIndex) Query(ctx context.Context, cond engine.QueryCondition) (*bitmap.Bitmap, bool, error) {
	log.Debugf("Query: called on index %s (composite=%v)", m.name, m.composite)
	node, ok := cond.(*FilterTreeNode)
	if !ok {
		return nil, false, fmt.Errorf("expected FilterTreeNode, got %T", cond)
	}

	// For simple index, only handle leaf nodes
	if !m.composite {
		if !node.IsLeaf() {
			log.Debugf("Query: simple index cannot handle non-leaf node")
			return nil, false, nil
		}
		if node.Filter == nil {
			log.Debugf("Query: simple index cannot handle nil filter")
			return nil, false, nil
		}
		canMatch := node.Filter.Name == "id" && node.Filter.Mode == FilterModeEqual
		log.Debugf("Query: simple index checking field=%s mode=%v canMatch=%v",
			node.Filter.Name, node.Filter.Mode, canMatch)
		if !canMatch {
			return nil, false, nil
		}
		m.queryCount++
		node.Skip = true
		log.Debugf("Query: processed simple node, count=%d, skip=%v", m.queryCount, node.Skip)
		return m.bits, !m.collides, nil
	}

	// For composite index, delegate to QueryComposite
	log.Debugf("Query: delegating to QueryComposite")
	return m.QueryComposite(ctx, cond)
}
func (m *planTestIndex) QueryComposite(ctx context.Context, cond engine.QueryCondition) (*bitmap.Bitmap, bool, error) {
	log.Debugf("QueryComposite: called on index %s", m.name)
	if !m.composite {
		return nil, false, nil
	}
	node, ok := cond.(*FilterTreeNode)
	if !ok {
		return nil, false, fmt.Errorf("expected FilterTreeNode, got %T", cond)
	}
	m.queryCount++
	for _, child := range node.Children {
		child.Skip = true
	}
	log.Debugf("QueryComposite: processed node, count=%d", m.queryCount)
	return m.bits, !m.collides, nil
}
func (m *planTestIndex) CanMatch(cond engine.QueryCondition) bool {
	node, ok := cond.(*FilterTreeNode)
	if !ok {
		log.Debugf("CanMatch: not a FilterTreeNode: %T", cond)
		return false
	}

	// For simple index
	if !m.composite {
		if node.IsLeaf() && node.Filter != nil {
			canMatch := node.Filter.Name == "id" && node.Filter.Mode == FilterModeEqual
			log.Debugf("CanMatch(simple): checking field=%s mode=%v canMatch=%v name=%s",
				node.Filter.Name, node.Filter.Mode, canMatch, m.name)
			return canMatch
		}
		log.Debugf("CanMatch(simple): non-leaf or nil filter for index %s", m.name)
		return false
	}

	// For composite index
	if node.IsLeaf() && node.Filter != nil {
		canMatch := node.Filter.Name == "id" || node.Filter.Name == "score"
		log.Debugf("CanMatch(composite): leaf node field=%s, canMatch=%v", node.Filter.Name, canMatch)
		return canMatch
	}

	if len(node.Children) > 0 {
		log.Debugf("CanMatch: composite node with %d children", len(node.Children))
		for _, child := range node.Children {
			if !m.CanMatch(child) {
				return false
			}
		}
		return true
	}

	return false
}

// Add missing methods
func (m *planTestIndex) Create(ctx context.Context, table engine.TableEngine, schema *schema.Schema, opts engine.IndexOptions) error {
	return nil
}
func (m *planTestIndex) Drop(ctx context.Context) error                   { return nil }
func (m *planTestIndex) Del(ctx context.Context, key []byte) error        { return nil }
func (m *planTestIndex) Add(ctx context.Context, key, value []byte) error { return nil }
func (m *planTestIndex) Close(ctx context.Context) error                  { return nil }
func (m *planTestIndex) Metrics() engine.IndexMetrics                     { return engine.IndexMetrics{} }

// Add Open method to satisfy engine.IndexEngine interface
func (m *planTestIndex) Open(ctx context.Context, table engine.TableEngine, schema *schema.Schema, opts engine.IndexOptions) error {
	m.table = table
	return nil
}

// Add Rebuild method to satisfy engine.IndexEngine interface
func (m *planTestIndex) Rebuild(ctx context.Context) error {
	return nil
}

// Add Sync method to satisfy engine.IndexEngine interface
func (m *planTestIndex) Sync(ctx context.Context) error {
	return nil
}

// Add Table method to satisfy engine.IndexEngine interface
func (m *planTestIndex) Table() engine.TableEngine {
	return m.table
}

// Add Truncate method to satisfy engine.IndexEngine interface
func (m *planTestIndex) Truncate(ctx context.Context) error {
	return nil
}

// Helper functions
func newPlanTestIndex(name string, s *schema.Schema) *planTestIndex {
	bits := bitmap.New()
	bits.Set(1)
	bits.Set(2)
	bits.Set(3)

	return &planTestIndex{
		name:     name,
		schema:   s,
		canMatch: true,
		bits:     &bits,
		fields:   []string{"id"},
	}
}

// Test schema setup
var planTestSchema = schema.NewSchema().
	WithName("test").
	WithField(schema.NewField(types.FieldTypeInt64).WithName("id").WithFlags(types.FieldFlagPrimary)).
	WithField(schema.NewField(types.FieldTypeString).WithName("name")).
	WithField(schema.NewField(types.FieldTypeFloat64).WithName("score")).
	WithField(schema.NewField(types.FieldTypeDatetime).WithName("created")).
	Finalize()

// Update mockMatcher to implement the full interface
type mockMatcher struct {
	field     schema.Field
	fieldType types.FieldType
	fieldName string
	value     interface{}
	slice     []interface{}
}

func (m *mockMatcher) Field() schema.Field                  { return m.field }
func (m *mockMatcher) Type() types.FieldType                { return m.fieldType }
func (m *mockMatcher) Name() string                         { return m.fieldName }
func (m *mockMatcher) Weight() int                          { return 1 }
func (m *mockMatcher) Len() int                             { return 1 }
func (m *mockMatcher) Value() interface{}                   { return m.value }
func (m *mockMatcher) WithValue(v interface{})              { m.value = v }
func (m *mockMatcher) WithSlice(v interface{})              { m.slice = v.([]interface{}) }
func (m *mockMatcher) WithSet(bm *xroar.Bitmap)             {}
func (m *mockMatcher) MatchValue(v interface{}) bool        { return true }
func (m *mockMatcher) MatchRange(min, max interface{}) bool { return true }
func (m *mockMatcher) MatchBloom(f *bloom.Filter) bool      { return true }
func (m *mockMatcher) MatchBitmap(bm *xroar.Bitmap) bool    { return true }
func (m *mockMatcher) MatchBlock(b *block.Block, res *bitset.Bitset, mask *bitset.Bitset) *bitset.Bitset {
	if mask != nil {
		return res.Copy(mask)
	}
	return res
}

// Update NewMatcher to return a mockMatcher
func NewMatcher(f schema.Field) Matcher {
	return &mockMatcher{
		field:     f,
		fieldType: f.Type(),
		fieldName: f.Name(),
	}
}

func TestPlanValidation(t *testing.T) {
	tests := []struct {
		name    string
		setup   func() *QueryPlan
		wantErr string
	}{
		{
			name: "Missing Table",
			setup: func() *QueryPlan {
				return NewQueryPlan().
					WithFilters(&FilterTreeNode{})
			},
			wantErr: "knox: table does not exist",
		},
		{
			name: "Missing Filters",
			setup: func() *QueryPlan {
				tbl := &mockTable{schema: planTestSchema}
				return NewQueryPlan().
					WithTable(tbl).
					WithFilters(nil)
			},
			wantErr: "missing filters",
		},
		{
			name: "Invalid Request Schema",
			setup: func() *QueryPlan {
				invalidSchema := schema.NewSchema().
					WithName("invalid").
					WithField(schema.NewField(types.FieldTypeInt64).WithName("nonexistent")).
					Finalize()
				tbl := &mockTable{schema: planTestSchema}
				return NewQueryPlan().
					WithTable(tbl).
					WithFilters(&FilterTreeNode{}).
					WithSchema(invalidSchema)
			},
			wantErr: "schema",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			plan := tt.setup()
			err := plan.Validate()
			if tt.wantErr == "" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
			}
		})
	}
}

func TestPlanIndexes(t *testing.T) {
	ctx := context.Background()
	log.SetLevel(log.LevelDebug)

	tests := []struct {
		name     string
		setup    func() (*QueryPlan, *planTestIndex)
		validate func(*testing.T, *QueryPlan, *planTestIndex)
		wantErr  bool
	}{
		{
			name: "Simple Index Hit",
			setup: func() (*QueryPlan, *planTestIndex) {
				idx := newPlanTestIndex("id_idx", planTestSchema)
				tbl := &mockTable{
					schema:  planTestSchema,
					indexes: []engine.IndexEngine{idx},
				}

				field, _ := planTestSchema.FieldByName("id")
				filter := &FilterTreeNode{
					Filter: &Filter{
						Name:    field.Name(),
						Mode:    FilterModeEqual,
						Value:   int64(1),
						Matcher: NewMatcher(field),
					},
				}

				// Create plan and explicitly set indexes
				plan := NewQueryPlan().
					WithTable(tbl).
					WithLogger(log.Disabled)

				// Ensure index is properly registered
				plan.Indexes = append(plan.Indexes, idx)

				// Add filter after index registration
				plan = plan.WithFilters(filter)

				// Set up bidirectional relationship
				idx.table = tbl

				log.Debugf("Setup: plan configuration:")
				log.Debugf("  - Table: %s", tbl.Name())
				log.Debugf("  - Index: %s (composite=%v)", idx.name, idx.composite)
				log.Debugf("  - Filter: field=%s mode=%v", field.Name(), filter.Filter.Mode)
				log.Debugf("  - Registered indexes: %d", len(plan.Indexes))

				// Verify index registration
				for i, index := range plan.Indexes {
					log.Debugf("  - Index[%d]: %T name=%v", i, index, index.(*planTestIndex).name)
				}

				log.Debugf("Setup: filter tree:")
				log.Debugf("  - IsLeaf: %v", filter.IsLeaf())
				log.Debugf("  - Filter: %+v", filter.Filter)

				return plan, idx
			},
			validate: func(t *testing.T, p *QueryPlan, idx *planTestIndex) {
				log.Debugf("Validate: index query count=%d", idx.queryCount)
				log.Debugf("Validate: filter processed=%v", p.Filters.IsProcessed())

				assert.Equal(t, 1, idx.queryCount, "index should be queried once")
				assert.True(t, p.Filters.IsProcessed(), "filters should be processed")
			},
		},
		{
			name: "Composite Index Hit",
			setup: func() (*QueryPlan, *planTestIndex) {
				idx := newPlanTestIndex("composite_idx", planTestSchema)
				idx.composite = true
				tbl := &mockTable{schema: planTestSchema}

				idField, _ := planTestSchema.FieldByName("id")
				scoreField, _ := planTestSchema.FieldByName("score")
				filter := &FilterTreeNode{
					Children: []*FilterTreeNode{
						{
							Filter: &Filter{
								Name:    idField.Name(),
								Mode:    FilterModeEqual,
								Value:   int64(1),
								Matcher: NewMatcher(idField),
							},
						},
						{
							Filter: &Filter{
								Name:    scoreField.Name(),
								Mode:    FilterModeEqual,
								Value:   float64(10.0),
								Matcher: NewMatcher(scoreField),
							},
						},
					},
				}

				plan := NewQueryPlan().
					WithTable(tbl).
					WithIndex(idx).
					WithFilters(filter).
					WithLogger(log.Disabled)

				idx.table = tbl

				return plan, idx
			},
			validate: func(t *testing.T, p *QueryPlan, idx *planTestIndex) {
				assert.Equal(t, 1, idx.queryCount, "composite index should be queried once")
				assert.True(t, p.Filters.IsProcessed(), "filters should be processed")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			plan, idx := tt.setup()

			log.Debugf("Test %s: compiling plan with %d indexes", tt.name, len(plan.Indexes))
			err := plan.Compile(ctx)
			require.NoError(t, err)

			log.Debugf("Test %s: querying indexes", tt.name)
			err = plan.QueryIndexes(ctx)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)

			log.Debugf("Test %s: validating results", tt.name)
			tt.validate(t, plan, idx)
		})
	}
}
