// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package query

import (
	"bytes"
	"context"
	"testing"
	"time"

	"blockwatch.cc/knoxdb/internal/engine"
	"blockwatch.cc/knoxdb/pkg/bitmap"
	"blockwatch.cc/knoxdb/pkg/schema"
	"blockwatch.cc/knoxdb/pkg/util"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	_ engine.QueryableIndex = (*MockIndex)(nil)
	_ engine.QueryableTable = (*MockTable)(nil)
)

var (
	testData [][]byte
)

func init() {
	testData = makeStructResultsData(
		&testStruct{
			Id:       uint64(1),
			Score:    1.5,
			Name:     "Ja",
			Created:  time.Now().UTC(),
			Status:   "active",
			IsActive: true,
		},
		&testStruct{
			Id:       uint64(2),
			Score:    4.5,
			Name:     "Nja",
			Created:  time.Now().UTC(),
			Status:   "active",
			IsActive: true,
		},
		&testStruct{
			Id:       uint64(3),
			Score:    13.5,
			Name:     "Pka",
			Created:  time.Now().UTC(),
			Status:   "active",
			IsActive: true,
		},
	)
}

func IsFilterEqual(a, b *FilterTreeNode) bool {
	if len(a.Children) != len(b.Children) {
		return false
	}
	if a.Skip != b.Skip {
		return false
	}
	if a.OrKind != b.OrKind {
		return false
	}
	if !bytes.Equal(a.Bits.Bytes(), b.Bits.Bytes()) {
		return false
	}
	if a.IsEmpty() != b.IsEmpty() {
		return false
	}

	if a.IsLeaf() != b.IsLeaf() {
		return false
	}
	if a.IsLeaf() {
		if a.Filter.Name != b.Filter.Name {
			return false
		}
		if a.Filter.Index != b.Filter.Index {
			return false
		}
		if a.Filter.Mode != b.Filter.Mode {
			return false
		}
		if util.ToString(a.Filter.Value) != util.ToString(b.Filter.Value) {
			return false
		}
	} else {
		for i := range a.Children {
			if !IsFilterEqual(a.Children[i], b.Children[i]) {
				return false
			}
		}
	}

	return true
}

type IndexResult struct {
	IndexSchema *schema.Schema
	Result      []uint64
}

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

func makeMockIndex(indexSchema *schema.Schema, res bitmap.Bitmap) engine.QueryableIndex {
	return NewMockIndex(
		// index schema is a child of table schema
		indexSchema,
		// index query results (i.e. matching primary key values)
		res,
	)
}

func makeRandomResultsData(data ...int) [][]byte {
	res := make([][]byte, len(data))
	for i, v := range data {
		res[i] = makeEncodedTestStruct(testSchema, makeTestStruct(v))
	}
	return res
}

func makeStructResultsData(data ...any) [][]byte {
	res := make([][]byte, len(data))
	for i, v := range data {
		res[i] = makeEncodedTestStruct(testSchema, v)
	}
	return res
}

func makeMockTable(schema *schema.Schema, results []IndexResult, res engine.QueryResult) engine.QueryableTable {
	queryableIndexes := make([]engine.QueryableIndex, 0)
	for _, q := range results {
		queryableIndexes = append(queryableIndexes, makeMockIndex(q.IndexSchema, bitmap.NewFromArray(q.Result)))
	}
	return NewMockTable(
		schema,
		// list of indexes derived from table
		queryableIndexes,
		res,
	)
}

func TestPlanCompile(t *testing.T) {
	// define and compile initial filter conditions; the result, a tree of
	// FilterTreeNode nodes will get optimized and changed during query
	// execution steps
	type TestCase struct {
		Name         string
		Condition    Condition
		Schema       *schema.Schema
		ResultsData  [][]byte
		IndexResults []IndexResult
		ExpectedTree *FilterTreeNode
	}

	testCases := []TestCase{
		// single condition + single index
		{
			Name:         "EQ Condition",
			Condition:    Equal("id", 1),
			Schema:       testSchema,
			ResultsData:  makeRandomResultsData(1),
			IndexResults: []IndexResult{{testIndexSchema, []uint64{1}}},
			ExpectedTree: makeAndTree(makeEqualNode("id", 0, uint64(1))),
		},
		{
			Name:         "NE Condition",
			Condition:    NotEqual("id", 1),
			Schema:       testSchema,
			ResultsData:  makeRandomResultsData(1, 2),
			IndexResults: []IndexResult{{testIndexSchema, []uint64{1, 2}}},
			ExpectedTree: makeAndTree(makeNotEqualNode("id", 0, uint64(1))),
		},
		{
			Name:         "In Condition",
			Condition:    In("id", []uint64{1, 2, 3}),
			Schema:       testSchema,
			ResultsData:  makeRandomResultsData(1),
			IndexResults: []IndexResult{{testIndexSchema, []uint64{2}}},
			ExpectedTree: makeAndTree(makeInNode("id", 0, []uint64{1, 2, 3})),
		},
		{
			Name:         "In Condition Single Element",
			Condition:    In("id", []uint64{1}),
			Schema:       testSchema,
			ResultsData:  makeRandomResultsData(1),
			IndexResults: []IndexResult{{testIndexSchema, []uint64{1}}},
			ExpectedTree: makeAndTree(makeEqualNode("id", 0, uint64(1))),
		},
		{
			Name:         "NI Condition",
			Condition:    NotIn("id", []uint64{2, 3, 4}),
			Schema:       testSchema,
			ResultsData:  makeRandomResultsData(1),
			IndexResults: []IndexResult{{testIndexSchema, []uint64{1, 2}}},
			ExpectedTree: makeAndTree(makeNotInNode("id", 0, []uint64{2, 3, 4})),
		},
		{
			Name:         "LT Condition",
			Condition:    Lt("id", 1),
			Schema:       testSchema,
			ResultsData:  makeRandomResultsData(1),
			IndexResults: []IndexResult{{testIndexSchema, []uint64{2}}},
			ExpectedTree: makeAndTree(makeLtNode("id", 0, uint64(1))),
		},
		{
			Name:         "Le Condition",
			Condition:    Le("id", 2),
			Schema:       testSchema,
			ResultsData:  makeRandomResultsData(1),
			IndexResults: []IndexResult{{testIndexSchema, []uint64{1, 2}}},
			ExpectedTree: makeAndTree(makeLeNode("id", 0, uint64(2))),
		},
		{
			Name:         "GT Condition",
			Condition:    Gt("id", 1),
			Schema:       testSchema,
			ResultsData:  makeRandomResultsData(1),
			IndexResults: []IndexResult{{testIndexSchema, []uint64{2}}},
			ExpectedTree: makeAndTree(makeGtNode("id", 0, uint64(1))),
		},
		{
			Name:         "Ge Condition",
			Condition:    Ge("id", 1),
			Schema:       testSchema,
			ResultsData:  makeRandomResultsData(1),
			IndexResults: []IndexResult{{testIndexSchema, []uint64{2}}},
			ExpectedTree: makeAndTree(makeGeNode("id", 0, uint64(1))),
		},
		{
			Name:         "Regexp Condition",
			Condition:    Regexp("name", "zack"),
			Schema:       testSchema,
			ResultsData:  makeRandomResultsData(1),
			IndexResults: []IndexResult{{testIndexSchema, []uint64{2}}},
			ExpectedTree: makeAndTree(makeRegexNode("name", 2, "zack")),
		},
		{
			Name:         "Range Condition",
			Condition:    Range("id", 1, 10),
			Schema:       testSchema,
			ResultsData:  makeRandomResultsData(1),
			IndexResults: []IndexResult{{testIndexSchema, []uint64{2}}},
			ExpectedTree: makeAndTree(makeRangeNode("id", 0, uint64(1), uint64(10))),
		},

		// And Condition + 2 or more conditions + single index
		{
			Name:         "And(NotEqual(2), Range(1,10)) Condition",
			Condition:    And(NotEqual("id", 2), Range("id", 1, 10)),
			Schema:       testSchema,
			ResultsData:  makeRandomResultsData(1, 2, 3, 4),
			IndexResults: []IndexResult{{testIndexSchema, []uint64{1, 2, 3, 4}}},
			ExpectedTree: makeAndTree(makeNotEqualNode("id", 0, uint64(2)), makeRangeNode("id", 0, uint64(1), uint64(10))),
		},
		{
			Name:         "And(Le(score, 4.5), Range(id(0,10))) Condition",
			Condition:    And(Le("score", 4.5), Range("id", 0, 10)),
			Schema:       testSchema,
			ResultsData:  testData,
			IndexResults: []IndexResult{{testIndexSchema, []uint64{1, 2, 3}}},
			ExpectedTree: makeAndTree(makeLeNode("score", 1, float64(4.5)), makeRangeNode("id", 0, uint64(0), uint64(10))),
		},
		{
			Name:         "And(Le(8), Range(6,10)) Condition",
			Condition:    And(Le("id", 8), Range("id", 6, 10)),
			Schema:       testSchema,
			ResultsData:  makeRandomResultsData(1, 2, 3, 4),
			IndexResults: []IndexResult{{testIndexSchema, []uint64{1, 2, 3, 4}}},
			ExpectedTree: makeAndTree(makeRangeNode("id", 0, uint64(6), uint64(8))),
		},
		{
			Name:         "And(RG(1,10), EQ(1))",
			Condition:    And(Range("id", 1, 10), Equal("id", 1)),
			Schema:       testSchema,
			IndexResults: []IndexResult{{testIndexSchema, []uint64{2}}},
			ExpectedTree: makeAndTree(makeEqualNode("id", 0, uint64(1))),
		},
		{
			Name:         "AND(RG(1,10), RG(5,10)) Condition",
			Condition:    And(Range("id", 1, 10), Range("id", 5, 10)),
			Schema:       testSchema,
			ResultsData:  makeRandomResultsData(1, 2),
			IndexResults: []IndexResult{{testIndexSchema, []uint64{1, 2}}},
			ExpectedTree: makeAndTree(makeRangeNode("id", 0, uint64(5), uint64(10))),
		},
		{
			Name:         "AND(EQ(5), RG(0,10)) Condition",
			Condition:    And(Equal("id", 5), Range("id", 0, 10)),
			Schema:       testSchema,
			ResultsData:  makeRandomResultsData(1, 2, 3, 4),
			IndexResults: []IndexResult{{testIndexSchema, []uint64{1, 2, 3, 4}}},
			ExpectedTree: makeAndTree(makeEqualNode("id", 0, uint64(5))),
		},
		{
			Name:         "And(EQ(id, 1), EQ(name, hi)) Condition",
			Condition:    And(Equal("id", 1), Equal("name", "hi")),
			Schema:       testSchema,
			ResultsData:  makeRandomResultsData(1, 2),
			IndexResults: []IndexResult{{testIndexSchema, []uint64{1, 2}}},
			ExpectedTree: makeAndTree(makeEqualNode("id", 0, uint64(1)), makeEqualNode("name", 2, "hi")),
		},

		// Or Condition + 2 or more conditions + single index
		{
			Name:         "Or(Le(8), Range(6,10)) Condition",
			Condition:    Or(Le("id", 8), Range("id", 6, 10)),
			Schema:       testSchema,
			ResultsData:  makeRandomResultsData(1, 2, 3, 4),
			IndexResults: []IndexResult{{testIndexSchema, []uint64{1, 2, 3, 4}}},
			ExpectedTree: makeOrTree(makeLeNode("id", 0, uint64(10))),
		},
		{
			Name:         "OR(EQ(id, 1), EQ(name, hi)) Condition",
			Condition:    Or(Equal("id", 1), Equal("name", "hi")),
			Schema:       testSchema,
			ResultsData:  makeRandomResultsData(1, 2),
			IndexResults: []IndexResult{{testIndexSchema, []uint64{1, 2}}},
			ExpectedTree: makeOrTree(makeEqualNode("id", 0, uint64(1)), makeEqualNode("name", 2, "hi")),
		},
		{
			Name:         "OR(EQ, EQ) Condition",
			Condition:    Or(Equal("id", 1), Equal("id", 2)),
			Schema:       testSchema,
			ResultsData:  makeRandomResultsData(1, 2),
			IndexResults: []IndexResult{{testIndexSchema, []uint64{1, 2}}},
			ExpectedTree: makeOrTree(makeInNode("id", 0, []uint64{1, 2})),
		},
		{
			Name:         "OR(RG, EQ) Condition",
			Condition:    Or(Range("id", 1, 10), Equal("id", 10)),
			Schema:       testSchema,
			ResultsData:  makeRandomResultsData(1, 2),
			IndexResults: []IndexResult{{testIndexSchema, []uint64{1, 2}}},
			ExpectedTree: makeOrTree(makeRangeNode("id", 0, uint64(1), uint64(10))),
		},
		{
			Name:         "OR(RG, RG, EQ) Condition",
			Condition:    Or(Range("id", 1, 10), Range("id", 5, 10), Equal("id", 2)),
			Schema:       testSchema,
			ResultsData:  makeRandomResultsData(1, 2),
			IndexResults: []IndexResult{{testIndexSchema, []uint64{1, 2}}},
			ExpectedTree: makeOrTree(makeRangeNode("id", 0, uint64(1), uint64(10))),
		},
		{
			Name:         "OR(In(1,2), RG(6,10)) (In) Out of Range Condition",
			Condition:    Or(In("id", []int{1, 2}), Range("id", 6, 10)),
			Schema:       testSchema,
			ResultsData:  makeRandomResultsData(1, 2, 3, 4),
			IndexResults: []IndexResult{{testIndexSchema, []uint64{1, 2, 3, 4}}},
			// ExpectedTree: makeOrTree(makeRangeNode("id", 0, uint64(1), uint64(10))),
			ExpectedTree: makeOrTree(makeInNode("id", 0, []uint64{1, 2}), makeRangeNode("id", 0, uint64(6), uint64(10))),
		},
		{
			Name:         "OR(In(6,7), RG(6,10)) In Range Condition",
			Condition:    Or(In("id", []int{6, 7}), Range("id", 6, 10)),
			Schema:       testSchema,
			ResultsData:  makeRandomResultsData(1, 2, 3, 4),
			IndexResults: []IndexResult{{testIndexSchema, []uint64{1, 2, 3, 4}}},
			// ExpectedTree: makeOrTree(, makeRangeNode("id", 0, uint64(6), uint64(10))),
			ExpectedTree: makeOrTree(makeInNode("id", 0, []uint64{6, 7}), makeRangeNode("id", 0, uint64(6), uint64(10))),
		},
		{
			Name:         "Or(Le(10), Range(1,5)) Condition",
			Condition:    Or(Le("id", 10), Range("id", 1, 5)),
			Schema:       testSchema,
			ResultsData:  makeRandomResultsData(1, 2, 3, 4),
			IndexResults: []IndexResult{{testIndexSchema, []uint64{1, 2, 3, 4}}},
			ExpectedTree: makeOrTree(makeLeNode("id", 0, uint64(10))),
		},
		{
			Name:         "Or(Le(id, 4.5), Range(id,(0,10))) Condition",
			Condition:    Or(Le("score", 4.5), Range("id", 0, 10)),
			Schema:       testSchema,
			ResultsData:  testData,
			IndexResults: []IndexResult{{testIndexSchema, []uint64{1, 2, 3}}},
			ExpectedTree: makeOrTree(makeLeNode("score", 1, float64(4.5)), makeRangeNode("id", 0, uint64(0), uint64(10))),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.Name, func(t *testing.T) {
			flt, err := tc.Condition.Compile(tc.Schema)
			require.NoError(t, err)

			// construct a mock result and append some data
			res := NewResult(testSchema)
			for _, rd := range tc.ResultsData {
				require.NoError(t, res.Append(rd, false))
			}

			// construct a query plan for testing

			plan := NewQueryPlan().
				WithTag(tc.Name).
				WithTable(makeMockTable(tc.Schema, tc.IndexResults, res)).
				// WithFlags(QueryFlagNoIndex).
				// WithLogger(log.Log).
				// WithOrder(OrderDesc).
				// WithLimit(1).
				WithFilters(flt).
				WithSchema(testSchema)
			defer plan.Close()

			require.NoError(t, plan.Validate())

			require.NoError(t, plan.Compile(context.TODO()))
			isEqual := IsFilterEqual(tc.ExpectedTree, plan.Filters)
			assert.True(t, isEqual)
			if !isEqual {
				assert.Equal(t, tc.ExpectedTree, plan.Filters)
			}
		})
	}
}
