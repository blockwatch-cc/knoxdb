// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package query

import (
	"bytes"
	"context"
	"math"
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

func makeQueryableIndex(results []IndexResult) []engine.QueryableIndex {
	queryableIndexes := make([]engine.QueryableIndex, 0)
	for _, q := range results {
		queryableIndexes = append(queryableIndexes, makeMockIndex(q.IndexSchema, bitmap.NewFromArray(q.Result)))
	}
	return queryableIndexes
	// return NewMockTable(
	// 	schema,
	// 	// list of indexes derived from table
	// 	queryableIndexes,
	// 	res,
	// )
}

func TestPlanCompile(t *testing.T) {
	// define and compile initial filter conditions; the result, a tree of
	// FilterTreeNode nodes will get optimized and changed during query
	// execution steps
	type ParentTestCase struct {
		Name     string
		HasIndex bool
	}

	type TestCase struct {
		Name                 string
		Condition            Condition
		Schema               *schema.Schema
		ResultsData          [][]byte
		IndexResults         []IndexResult
		ExpectedToSkip       bool
		ExpectedIndexTree    *FilterTreeNode
		ExpectedNonIndexTree *FilterTreeNode
	}

	parentTestCase := []ParentTestCase{
		{
			Name:     "With Index",
			HasIndex: true,
		},
		{
			Name:     "Without Index",
			HasIndex: false,
		},
	}

	f1, _ := testSchema.FieldByName("id")
	f2, _ := testSchema.FieldByName("name")
	f3, _ := testSchema.FieldByName("score")

	testCases := []TestCase{
		// single condition + single index
		{
			Name:                 "EQ Condition",
			Condition:            Equal("id", 1),
			Schema:               testSchema,
			ResultsData:          makeRandomResultsData(1),
			IndexResults:         []IndexResult{{testIndexSchema, []uint64{1}}},
			ExpectedIndexTree:    makeAndTree(makeEqualNode(f1, uint64(1))),
			ExpectedNonIndexTree: makeAndTree(makeEqualNode(f1, uint64(1))),
		},
		{
			Name:                 "NE Condition",
			Condition:            NotEqual("id", 1),
			Schema:               testSchema,
			ResultsData:          makeRandomResultsData(1, 2),
			IndexResults:         []IndexResult{{testIndexSchema, []uint64{1, 2}}},
			ExpectedIndexTree:    makeAndTree(makeNotEqualNode(f1, uint64(1))),
			ExpectedNonIndexTree: makeAndTree(makeNotEqualNode(f1, uint64(1))),
		},
		{
			Name:                 "In Condition",
			Condition:            In("id", []uint64{1, 2, 3}),
			Schema:               testSchema,
			ResultsData:          makeRandomResultsData(1),
			IndexResults:         []IndexResult{{testIndexSchema, []uint64{2}}},
			ExpectedIndexTree:    makeAndTree(makeInNode(f1, []uint64{1, 2, 3})),
			ExpectedNonIndexTree: makeAndTree(makeInNode(f1, []uint64{1, 2, 3})),
		},
		{
			Name:                 "In Condition Single Element",
			Condition:            In("id", []uint64{1}),
			Schema:               testSchema,
			ResultsData:          makeRandomResultsData(1),
			IndexResults:         []IndexResult{{testIndexSchema, []uint64{1}}},
			ExpectedIndexTree:    makeAndTree(makeEqualNode(f1, uint64(1))),
			ExpectedNonIndexTree: makeAndTree(makeEqualNode(f1, uint64(1))),
		},
		{
			Name:                 "NI Condition",
			Condition:            NotIn("id", []uint64{2, 3, 4}),
			Schema:               testSchema,
			ResultsData:          makeRandomResultsData(1),
			IndexResults:         []IndexResult{{testIndexSchema, []uint64{1, 2}}},
			ExpectedIndexTree:    makeAndTree(makeNotInNode(f1, []uint64{2, 3, 4})),
			ExpectedNonIndexTree: makeAndTree(makeNotInNode(f1, []uint64{2, 3, 4})),
		},
		{
			Name:                 "LT Condition",
			Condition:            Lt("id", 1),
			Schema:               testSchema,
			ResultsData:          makeRandomResultsData(1),
			IndexResults:         []IndexResult{{testIndexSchema, []uint64{2}}},
			ExpectedIndexTree:    makeAndTree(makeLtNode(f1, uint64(1))),
			ExpectedNonIndexTree: makeAndTree(makeLtNode(f1, uint64(1))),
		},
		{
			Name:                 "Le Condition",
			Condition:            Le("id", 2),
			Schema:               testSchema,
			ResultsData:          makeRandomResultsData(1),
			IndexResults:         []IndexResult{{testIndexSchema, []uint64{1, 2}}},
			ExpectedIndexTree:    makeAndTree(makeLeNode(f1, uint64(2))),
			ExpectedNonIndexTree: makeAndTree(makeLeNode(f1, uint64(2))),
		},
		{
			Name:                 "GT Condition",
			Condition:            Gt("id", 1),
			Schema:               testSchema,
			ResultsData:          makeRandomResultsData(1),
			IndexResults:         []IndexResult{{testIndexSchema, []uint64{2}}},
			ExpectedIndexTree:    makeAndTree(makeGtNode(f1, uint64(1))),
			ExpectedNonIndexTree: makeAndTree(makeGtNode(f1, uint64(1))),
		},
		{
			Name:                 "Ge Condition",
			Condition:            Ge("id", 1),
			Schema:               testSchema,
			ResultsData:          makeRandomResultsData(1),
			IndexResults:         []IndexResult{{testIndexSchema, []uint64{2}}},
			ExpectedIndexTree:    makeAndTree(makeGeNode(f1, uint64(1))),
			ExpectedNonIndexTree: makeAndTree(makeGeNode(f1, uint64(1))),
		},
		{
			Name:                 "Regexp Condition",
			Condition:            Regexp("name", "zack"),
			Schema:               testSchema,
			ResultsData:          makeRandomResultsData(1),
			IndexResults:         []IndexResult{{testIndexSchema, []uint64{1}}},
			ExpectedIndexTree:    makeAndTree(makeRegexNode(f2, "zack")),
			ExpectedNonIndexTree: makeAndTree(makeRegexNode(f2, "zack")),
		},
		{
			Name:                 "Range Condition",
			Condition:            Range("id", 1, 10),
			Schema:               testSchema,
			ResultsData:          makeRandomResultsData(1),
			IndexResults:         []IndexResult{{testIndexSchema, []uint64{2}}},
			ExpectedIndexTree:    makeAndTree(makeRangeNode(f1, uint64(1), uint64(10))),
			ExpectedNonIndexTree: makeAndTree(makeRangeNode(f1, uint64(1), uint64(10))),
		},

		// And Condition + 2 or more conditions + single index
		{
			Name:                 "And(NotEqual(2), Range(1,10)) Condition",
			Condition:            And(NotEqual("id", 2), Range("id", 1, 10)),
			Schema:               testSchema,
			ResultsData:          makeRandomResultsData(1, 2, 3, 4),
			IndexResults:         []IndexResult{{testIndexSchema, []uint64{1, 2, 3, 4}}},
			ExpectedIndexTree:    makeAndTree(makeNotEqualNode(f1, uint64(2)), makeRangeNode(f1, uint64(1), uint64(10))),
			ExpectedNonIndexTree: makeAndTree(makeNotEqualNode(f1, uint64(2)), makeRangeNode(f1, uint64(1), uint64(10))),
		},
		{
			Name:                 "And(Le(8), Range(6,10)) Condition",
			Condition:            And(Le("id", 8), Range("id", 6, 10)),
			Schema:               testSchema,
			ResultsData:          makeRandomResultsData(1, 2, 3, 4),
			IndexResults:         []IndexResult{{testIndexSchema, []uint64{1, 2, 3, 4}}},
			ExpectedIndexTree:    makeAndTree(makeRangeNode(f1, uint64(6), uint64(8))),
			ExpectedNonIndexTree: makeAndTree(makeRangeNode(f1, uint64(6), uint64(8))),
		},
		{
			Name:                 "And(RG(1,10), EQ(1))",
			Condition:            And(Range("id", 1, 10), Equal("id", 1)),
			Schema:               testSchema,
			IndexResults:         []IndexResult{{testIndexSchema, []uint64{2}}},
			ExpectedIndexTree:    makeAndTree(makeEqualNode(f1, uint64(1))),
			ExpectedNonIndexTree: makeAndTree(makeEqualNode(f1, uint64(1))),
		},
		{
			Name:                 "AND(RG(1,10), RG(5,10)) Condition",
			Condition:            And(Range("id", 1, 10), Range("id", 5, 10)),
			Schema:               testSchema,
			ResultsData:          makeRandomResultsData(1, 2),
			IndexResults:         []IndexResult{{testIndexSchema, []uint64{1, 2}}},
			ExpectedIndexTree:    makeAndTree(makeRangeNode(f1, uint64(5), uint64(10))),
			ExpectedNonIndexTree: makeAndTree(makeRangeNode(f1, uint64(5), uint64(10))),
		},
		{
			Name:                 "AND(EQ(5), RG(0,10)) Condition",
			Condition:            And(Equal("id", 5), Range("id", 0, 10)),
			Schema:               testSchema,
			ResultsData:          makeRandomResultsData(1, 2, 3, 4),
			IndexResults:         []IndexResult{{testIndexSchema, []uint64{1, 2, 3, 4}}},
			ExpectedIndexTree:    makeAndTree(makeEqualNode(f1, uint64(5))),
			ExpectedNonIndexTree: makeAndTree(makeEqualNode(f1, uint64(5))),
		},
		{
			Name:                 "And(EQ(id, 1), EQ(name, hi)) Condition",
			Condition:            And(Equal("id", 1), Equal("name", "hi")),
			Schema:               testSchema,
			ResultsData:          makeRandomResultsData(1, 2),
			IndexResults:         []IndexResult{{testIndexSchema, []uint64{1, 2}}},
			ExpectedIndexTree:    makeAndTree(makeEqualNode(f1, uint64(1)), makeEqualNode(f2, "hi")),
			ExpectedNonIndexTree: makeAndTree(makeEqualNode(f1, uint64(1)), makeEqualNode(f2, "hi")),
		},

		// Or Condition + 2 or more conditions + single index
		{
			Name:                 "Or(Le(8), Range(6,10)) Condition",
			Condition:            Or(Le("id", 8), Range("id", 6, 10)),
			Schema:               testSchema,
			ResultsData:          makeRandomResultsData(1, 2, 3, 4),
			IndexResults:         []IndexResult{{testIndexSchema, []uint64{1, 2, 3, 4}}},
			ExpectedIndexTree:    makeOrTree(makeLeNode(f1, uint64(10))),
			ExpectedNonIndexTree: makeOrTree(makeLeNode(f1, uint64(10))),
		},
		{
			Name:                 "OR(EQ(id, 1), EQ(name, hi)) Condition",
			Condition:            Or(Equal("id", 1), Equal("name", "hi")),
			Schema:               testSchema,
			ResultsData:          makeRandomResultsData(1, 2),
			IndexResults:         []IndexResult{{testIndexSchema, []uint64{1, 2}}},
			ExpectedIndexTree:    makeOrTree(makeEqualNode(f1, uint64(1)), makeEqualNode(f2, "hi")),
			ExpectedNonIndexTree: makeOrTree(makeEqualNode(f1, uint64(1)), makeEqualNode(f2, "hi")),
		},
		{
			Name:                 "OR(EQ, EQ) Condition",
			Condition:            Or(Equal("id", 1), Equal("id", 2)),
			Schema:               testSchema,
			ResultsData:          makeRandomResultsData(1, 2),
			IndexResults:         []IndexResult{{testIndexSchema, []uint64{1, 2}}},
			ExpectedIndexTree:    makeOrTree(makeInNode(f1, []uint64{1, 2})),
			ExpectedNonIndexTree: makeOrTree(makeInNode(f1, []uint64{1, 2})),
		},
		{
			Name:                 "OR(RG, EQ) Condition",
			Condition:            Or(Range("id", 1, 10), Equal("id", 10)),
			Schema:               testSchema,
			ResultsData:          makeRandomResultsData(1, 2),
			IndexResults:         []IndexResult{{testIndexSchema, []uint64{1, 2}}},
			ExpectedIndexTree:    makeOrTree(makeRangeNode(f1, uint64(1), uint64(10))),
			ExpectedNonIndexTree: makeOrTree(makeRangeNode(f1, uint64(1), uint64(10))),
		},
		{
			Name:                 "OR(RG, RG, EQ) Condition",
			Condition:            Or(Range("id", 1, 10), Range("id", 5, 10), Equal("id", 2)),
			Schema:               testSchema,
			ResultsData:          makeRandomResultsData(1, 2),
			IndexResults:         []IndexResult{{testIndexSchema, []uint64{1, 2}}},
			ExpectedIndexTree:    makeOrTree(makeRangeNode(f1, uint64(1), uint64(10))),
			ExpectedNonIndexTree: makeOrTree(makeRangeNode(f1, uint64(1), uint64(10))),
		},
		{
			Name:                 "OR(In(1,2), RG(6,10)) (In) Out of Range Condition",
			Condition:            Or(In("id", []int{1, 2}), Range("id", 6, 10)),
			Schema:               testSchema,
			ResultsData:          makeRandomResultsData(1, 2, 3, 4),
			IndexResults:         []IndexResult{{testIndexSchema, []uint64{1, 2, 3, 4}}},
			ExpectedIndexTree:    makeOrTree(makeInNode(f1, []uint64{1, 2}), makeRangeNode(f1, uint64(6), uint64(10))),
			ExpectedNonIndexTree: makeOrTree(makeInNode(f1, []uint64{1, 2}), makeRangeNode(f1, uint64(6), uint64(10))),
		},
		{
			Name:                 "OR(In(6,7), RG(6,10)) In Range Condition",
			Condition:            Or(In("id", []int{6, 7}), Range("id", 6, 10)),
			Schema:               testSchema,
			ResultsData:          makeRandomResultsData(1, 2, 3, 4),
			IndexResults:         []IndexResult{{testIndexSchema, []uint64{1, 2, 3, 4}}},
			ExpectedIndexTree:    makeOrTree(makeInNode(f1, []uint64{6, 7}), makeRangeNode(f1, uint64(6), uint64(10))),
			ExpectedNonIndexTree: makeOrTree(makeInNode(f1, []uint64{6, 7}), makeRangeNode(f1, uint64(6), uint64(10))),
		},
		{
			Name:                 "Or(Le(10), Range(1,5)) Condition",
			Condition:            Or(Le("id", 10), Range("id", 1, 5)),
			Schema:               testSchema,
			ResultsData:          makeRandomResultsData(1, 2, 3, 4),
			IndexResults:         []IndexResult{{testIndexSchema, []uint64{1, 2, 3, 4}}},
			ExpectedIndexTree:    makeOrTree(makeLeNode(f1, uint64(10))),
			ExpectedNonIndexTree: makeOrTree(makeLeNode(f1, uint64(10))),
		},
		{
			Name:                 "Or(Le(id, 4.5), Range(id,(0,10))) Condition",
			Condition:            Or(Le("score", 4.5), Range("id", 0, 10)),
			Schema:               testSchema,
			ResultsData:          testData,
			IndexResults:         []IndexResult{{testIndexSchema, []uint64{1, 2, 3}}},
			ExpectedIndexTree:    makeOrTree(makeLeNode(f3, float64(4.5)), makeRangeNode(f1, uint64(0), uint64(10))),
			ExpectedNonIndexTree: makeOrTree(makeLeNode(f3, float64(4.5)), makeRangeNode(f1, uint64(0), uint64(10))),
		},

		// CAT: merge nested nodes
		{
			Name:                 "OR ( OR (A, B), C) ) => OR (A, B, C)",
			Condition:            Or(Or(Equal("id", 1), Range("id", 2, 5)), Gt("id", 6)),
			Schema:               testSchema,
			ResultsData:          makeRandomResultsData(1, 2, 3, 4, 5, 6, 7, 8, 9),
			IndexResults:         []IndexResult{{testIndexSchema, []uint64{1, 2, 3}}},
			ExpectedIndexTree:    makeOrTree(makeGtNode(f1, uint64(6)), makeEqualNode(f1, uint64(1)), makeRangeNode(f1, uint64(2), uint64(5))),
			ExpectedNonIndexTree: makeOrTree(makeGtNode(f1, uint64(6)), makeEqualNode(f1, uint64(1)), makeRangeNode(f1, uint64(2), uint64(5))),
		},
		{
			Name:                 "AND ( AND (A, B), C) => AND (A, B, C)",
			Condition:            And(And(Range("id", 1, 10), Range("id", 2, 5)), Range("id", 4, 5)),
			Schema:               testSchema,
			ResultsData:          makeRandomResultsData(1, 2, 3, 4, 5, 6, 7, 8, 9),
			IndexResults:         []IndexResult{{testIndexSchema, []uint64{1, 2, 3}}},
			ExpectedIndexTree:    makeAndTree(makeRangeNode(f1, uint64(4), uint64(5))),
			ExpectedNonIndexTree: makeAndTree(makeRangeNode(f1, uint64(4), uint64(5))),
		},
		// CAT: replace/simplify sets
		{
			Name:                 "IN(single A) => EQ(A)",
			Condition:            In("id", []uint64{1}),
			Schema:               testSchema,
			ResultsData:          makeRandomResultsData(1),
			IndexResults:         []IndexResult{{testIndexSchema, []uint64{1}}},
			ExpectedIndexTree:    makeAndTree(makeEqualNode(f1, uint64(1))),
			ExpectedNonIndexTree: makeAndTree(makeEqualNode(f1, uint64(1))),
		},
		{
			Name:                 "NI(single A) => NE(A)",
			Condition:            NotIn("id", []uint64{1}),
			Schema:               testSchema,
			ResultsData:          makeRandomResultsData(1),
			IndexResults:         []IndexResult{{testIndexSchema, []uint64{1}}},
			ExpectedIndexTree:    makeAndTree(makeNotEqualNode(f1, uint64(1))),
			ExpectedNonIndexTree: makeAndTree(makeNotEqualNode(f1, uint64(1))),
		},
		{
			Name:                 "And: EQ(A) + EQ(A) => EQ(A)",
			Condition:            And(Equal("id", 1), Equal("id", 1)),
			Schema:               testSchema,
			ResultsData:          makeRandomResultsData(1),
			IndexResults:         []IndexResult{{testIndexSchema, []uint64{1}}},
			ExpectedIndexTree:    makeAndTree(makeEqualNode(f1, uint64(1))),
			ExpectedNonIndexTree: makeAndTree(makeEqualNode(f1, uint64(1))),
		},
		{
			Name:                 "Or: EQ(A) + EQ(A) => EQ(A)",
			Condition:            Or(Equal("id", 1), Equal("id", 1)),
			Schema:               testSchema,
			ResultsData:          makeRandomResultsData(1),
			IndexResults:         []IndexResult{{testIndexSchema, []uint64{1}}},
			ExpectedIndexTree:    makeOrTree(makeEqualNode(f1, uint64(1))),
			ExpectedNonIndexTree: makeOrTree(makeEqualNode(f1, uint64(1))),
		},
		{
			Name:                 "Empty IN => false",
			Condition:            In("id", []uint64{}),
			Schema:               testSchema,
			ResultsData:          makeRandomResultsData(1),
			IndexResults:         []IndexResult{{testIndexSchema, []uint64{1}}},
			ExpectedIndexTree:    makeAndTree(makeFalseNode(f1)),
			ExpectedNonIndexTree: makeAndTree(makeFalseNode(f1)),
		}, {
			Name:                 "Empty NIN => true",
			Condition:            NotIn("id", []uint64{}),
			Schema:               testSchema,
			ResultsData:          makeRandomResultsData(1),
			IndexResults:         []IndexResult{{testIndexSchema, []uint64{1}}},
			ExpectedIndexTree:    makeAndTree(makeTrueNode(f1)),
			ExpectedNonIndexTree: makeAndTree(makeTrueNode(f1)),
		}, {
			Name:                 "and: IN(A) + IN(B) => IN(A-B)",
			Condition:            And(In("id", []uint64{1, 2, 3}), In("id", []uint64{2, 3, 4})),
			Schema:               testSchema,
			ResultsData:          makeRandomResultsData(1),
			IndexResults:         []IndexResult{{testIndexSchema, []uint64{1}}},
			ExpectedIndexTree:    makeAndTree(makeInNode(f1, []uint64{2, 3})),
			ExpectedNonIndexTree: makeAndTree(makeInNode(f1, []uint64{2, 3})),
		},
		{
			Name:                 "or: IN(A) + IN(B) => IN(A+B)",
			Condition:            Or(In("id", []uint64{1, 2, 3}), In("id", []uint64{2, 3, 4})),
			Schema:               testSchema,
			ResultsData:          makeRandomResultsData(1),
			IndexResults:         []IndexResult{{testIndexSchema, []uint64{1}}},
			ExpectedIndexTree:    makeOrTree(makeInNode(f1, []uint64{1, 2, 3, 4})),
			ExpectedNonIndexTree: makeOrTree(makeInNode(f1, []uint64{1, 2, 3, 4})),
		},
		{
			Name:                 "and: NI(A) + NI(B) => NI(A+B)",
			Condition:            And(NotIn("id", []uint64{1, 2, 3}), NotIn("id", []uint64{2, 3, 4})),
			Schema:               testSchema,
			ResultsData:          makeRandomResultsData(1),
			IndexResults:         []IndexResult{{testIndexSchema, []uint64{1}}},
			ExpectedIndexTree:    makeAndTree(makeNotInNode(f1, []uint64{1, 2, 3, 4})),
			ExpectedNonIndexTree: makeAndTree(makeNotInNode(f1, []uint64{1, 2, 3, 4})),
		},
		{
			Name:                 "or: NI(A) + NI(B) => NI(A+B)",
			Condition:            Or(NotIn("id", []uint64{1, 2, 3}), NotIn("id", []uint64{2, 3, 4})),
			Schema:               testSchema,
			ResultsData:          makeRandomResultsData(1),
			IndexResults:         []IndexResult{{testIndexSchema, []uint64{1}}},
			ExpectedIndexTree:    makeOrTree(makeNotInNode(f1, []uint64{2, 3})),
			ExpectedNonIndexTree: makeOrTree(makeNotInNode(f1, []uint64{2, 3})),
		},
		{
			Name:                 "or: IN(A) + EQ(B) => IN(A+B)",
			Condition:            Or(In("id", []uint64{1, 2, 3}), Equal("id", 4)),
			Schema:               testSchema,
			ResultsData:          makeRandomResultsData(1),
			IndexResults:         []IndexResult{{testIndexSchema, []uint64{1}}},
			ExpectedIndexTree:    makeOrTree(makeInNode(f1, []uint64{1, 2, 3, 4})),
			ExpectedNonIndexTree: makeOrTree(makeInNode(f1, []uint64{1, 2, 3, 4})),
		},
		{
			Name:                 "or: EQ(A) + EQ(B) => IN(A+B)",
			Condition:            Or(Equal("id", 1), Equal("id", 2)),
			Schema:               testSchema,
			ResultsData:          makeRandomResultsData(1),
			IndexResults:         []IndexResult{{testIndexSchema, []uint64{1}}},
			ExpectedIndexTree:    makeOrTree(makeInNode(f1, []uint64{1, 2})),
			ExpectedNonIndexTree: makeOrTree(makeInNode(f1, []uint64{1, 2})),
		},
		// CAT: replace/simplify ranges
		{
			Name:                 "and: LT|LE(A) + LT|LE(A) => LT|LE(A)", // and: LT(A) + LT(A-5) => LE(A-1)
			Condition:            And(Lt("id", 10), Lt("id", 5)),
			Schema:               testSchema,
			ResultsData:          makeRandomResultsData(1, 2, 3, 4, 5, 6, 7, 8, 9, 10),
			IndexResults:         []IndexResult{{testIndexSchema, []uint64{1}}},
			ExpectedIndexTree:    makeAndTree(makeLeNode(f1, uint64(4))),
			ExpectedNonIndexTree: makeAndTree(makeLeNode(f1, uint64(4))),
		},
		{
			Name:                 "and: LE(0)",
			Condition:            And(Le("id", 0)),
			Schema:               testSchema,
			ResultsData:          makeRandomResultsData(1, 2, 3, 4, 5, 6, 7, 8, 9, 10),
			IndexResults:         []IndexResult{{testIndexSchema, []uint64{1}}},
			ExpectedIndexTree:    makeAndTree(makeLeNode(f1, uint64(0))),
			ExpectedNonIndexTree: makeAndTree(makeLeNode(f1, uint64(0))),
		},
		{
			Name:                 "and: GT|GE(A) + GT|GE(A) => GT|GE(A)", // and: GT|GE(A) + GT|GE(A) => RG(A+1, max)
			Condition:            And(Gt("id", 2), Gt("id", 5)),
			Schema:               testSchema,
			ResultsData:          makeRandomResultsData(1, 2, 3, 4, 5, 6, 7, 8, 9, 10),
			IndexResults:         []IndexResult{{testIndexSchema, []uint64{1}}},
			ExpectedIndexTree:    makeAndTree(makeRangeNode(f1, uint64(6), uint64(math.MaxUint64))),
			ExpectedNonIndexTree: makeAndTree(makeRangeNode(f1, uint64(6), uint64(math.MaxUint64))),
		},
		{
			Name:                 "and: RG(A,B) + RG(C,D) => RG(B,C) iff C ≤ B",
			Condition:            And(Range("id", 1, 5), Range("id", 3, 10)),
			Schema:               testSchema,
			ResultsData:          makeRandomResultsData(1, 2, 3, 4, 5, 6, 7, 8, 9, 10),
			IndexResults:         []IndexResult{{testIndexSchema, []uint64{1}}},
			ExpectedIndexTree:    makeAndTree(makeRangeNode(f1, uint64(3), uint64(5))),
			ExpectedNonIndexTree: makeAndTree(makeRangeNode(f1, uint64(3), uint64(5))),
		},
		{
			Name:                 "and: RG(A,B) + RG(B,D) => EQ(B)",
			Condition:            And(Range("id", 1, 5), Range("id", 5, 10)),
			Schema:               testSchema,
			ResultsData:          makeRandomResultsData(1, 2, 3, 4, 5, 6, 7, 8, 9, 10),
			IndexResults:         []IndexResult{{testIndexSchema, []uint64{1}}},
			ExpectedIndexTree:    makeAndTree(makeEqualNode(f1, uint64(5))),
			ExpectedNonIndexTree: makeAndTree(makeEqualNode(f1, uint64(5))),
		},
		{
			Name:                 "and: RG(A,B) + EQ(C) => EQ(C) iff A ≤ C ≤ B",
			Condition:            And(Range("id", 1, 5), Equal("id", 3)),
			Schema:               testSchema,
			ResultsData:          makeRandomResultsData(1, 2, 3, 4, 5, 6, 7, 8, 9, 10),
			IndexResults:         []IndexResult{{testIndexSchema, []uint64{1}}},
			ExpectedIndexTree:    makeAndTree(makeEqualNode(f1, uint64(3))),
			ExpectedNonIndexTree: makeAndTree(makeEqualNode(f1, uint64(3))),
		},
		{
			Name:                 "and: GE(A) + LE(B) => RG(A,B) iff A ≤ B",
			Condition:            And(Ge("id", 1), Le("id", 5)),
			Schema:               testSchema,
			ResultsData:          makeRandomResultsData(1, 2, 3, 4, 5, 6, 7, 8, 9, 10),
			IndexResults:         []IndexResult{{testIndexSchema, []uint64{1}}},
			ExpectedIndexTree:    makeAndTree(makeRangeNode(f1, uint64(1), uint64(5))),
			ExpectedNonIndexTree: makeAndTree(makeRangeNode(f1, uint64(1), uint64(5))),
		},
		{
			Name:                 "and: GT(A) + LT(B) => RG(A+1,B-1) iff A ≤ B",
			Condition:            And(Gt("id", 1), Lt("id", 5)),
			Schema:               testSchema,
			ResultsData:          makeRandomResultsData(1, 2, 3, 4, 5, 6, 7, 8, 9, 10),
			IndexResults:         []IndexResult{{testIndexSchema, []uint64{1}}},
			ExpectedIndexTree:    makeAndTree(makeRangeNode(f1, uint64(2), uint64(4))),
			ExpectedNonIndexTree: makeAndTree(makeRangeNode(f1, uint64(2), uint64(4))),
		},
		{
			Name:                 "or: RG(A,B) + EQ(C) => EQ(C) iff A ≤ C ≤ B",
			Condition:            Or(Range("id", 1, 5), Equal("id", 3)),
			Schema:               testSchema,
			ResultsData:          makeRandomResultsData(1, 2, 3, 4, 5, 6, 7, 8, 9, 10),
			IndexResults:         []IndexResult{{testIndexSchema, []uint64{1}}},
			ExpectedIndexTree:    makeOrTree(makeRangeNode(f1, uint64(1), uint64(5))),
			ExpectedNonIndexTree: makeOrTree(makeRangeNode(f1, uint64(1), uint64(5))),
		},
	}

	for _, ptc := range parentTestCase {
		t.Run(ptc.Name, func(t *testing.T) {
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
					var queryableIndexes []engine.QueryableIndex
					var results engine.QueryResult
					if ptc.HasIndex {
						queryableIndexes = makeQueryableIndex(tc.IndexResults)
					}

					mockTable := NewMockTable(tc.Schema, queryableIndexes, results)

					plan := NewQueryPlan().
						WithTag(tc.Name).
						WithTable(mockTable).
						// WithFlags(QueryFlagNoIndex).
						// WithLogger(log.Log).
						// WithOrder(OrderDesc).
						// WithLimit(1).
						WithFilters(flt).
						WithSchema(testSchema)
					defer plan.Close()

					require.NoError(t, plan.Validate())

					require.NoError(t, plan.Compile(context.TODO()))
					expectedTree := tc.ExpectedNonIndexTree
					if ptc.HasIndex {
						expectedTree = tc.ExpectedIndexTree
					}
					isEqual := IsFilterEqual(expectedTree, plan.Filters)
					assert.True(t, isEqual)
					if !isEqual {
						assert.Equal(t, expectedTree, plan.Filters)
					}
				})
			}
		})

	}
}
