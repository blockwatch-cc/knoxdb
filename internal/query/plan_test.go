// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package query

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/echa/log"

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

	testSchema      *schema.Schema
	testIndexSchema *schema.Schema
)

func init() {
	var err error
	testSchema, err = schema.SchemaOf(testStruct{})
	if err != nil {
		panic(err)
	}
	testIndexSchema, err = testSchema.SelectFields("name", "id")
	if err != nil {
		panic(err)
	}

	statusEnum := schema.NewEnumDictionary("status")
	statusEnum.Append("active", "pending", "inactive")
	testSchema.WithEnum(statusEnum)
}

type testStruct struct {
	Id       uint64    `knox:"id,pk"`
	Score    float64   `knox:"score"`
	Name     string    `knox:"name,index=hash"`
	Created  time.Time `knox:"created"`
	Status   string    `knox:"status,enum"`
	IsActive bool      `knox:"is_active"`
}

// func makeTestStruct(id int) *testStruct {
// 	return &testStruct{
// 		Id:       uint64(id),
// 		Score:    util.RandFloat64(),
// 		Name:     util.RandString(4),
// 		Created:  time.Now().UTC(),
// 		Status:   "active",
// 		IsActive: id%2 == 0,
// 	}
// }

func (t *testStruct) Encode() []byte {
	enc := schema.NewEncoder(testSchema)
	buf, err := enc.Encode(t, nil)
	if err != nil {
		panic(err)
	}
	return buf
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

func makeIndex(pks ...uint64) engine.QueryableIndex {
	return NewMockIndex(testIndexSchema, bitmap.NewFromArray(pks))
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

// func makeRandomResultsData(data ...int) [][]byte {
// 	res := make([][]byte, len(data))
// 	for i, v := range data {
// 		res[i] = makeEncodedTestStruct(testSchema, makeTestStruct(v))
// 	}
// 	return res
// }

// func makeStructResultsData(data ...any) [][]byte {
// 	res := make([][]byte, len(data))
// 	for i, v := range data {
// 		res[i] = makeEncodedTestStruct(testSchema, v)
// 	}
// 	return res
// }

// func makeQueryableIndex(results ...IndexResult) []engine.QueryableIndex {
// 	queryableIndexes := make([]engine.QueryableIndex, 0)
// 	for _, q := range results {
// 		queryableIndexes = append(queryableIndexes, makeMockIndex(q.IndexSchema, bitmap.NewFromArray(q.Result)))
// 	}
// 	return queryableIndexes
// }

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

	// ignore bits unless expected
	if a.Bits.IsValid() {
		if !bytes.Equal(a.Bits.Bytes(), b.Bits.Bytes()) {
			return false
		}
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

func TestPlanCompile(t *testing.T) {
	// define and compile initial filter conditions; the result, a tree of
	// FilterTreeNode nodes will get optimized and changed during query
	// execution steps
	type TestCase struct {
		Name      string
		Condition Condition
		Expected  *FilterTreeNode
	}

	f1, _ := testSchema.FieldByName("id")
	f2, _ := testSchema.FieldByName("name")
	f3, _ := testSchema.FieldByName("score")

	testCases := []TestCase{
		// single condition + single index
		{
			Name:      "EQ Condition",
			Condition: Equal("id", 1),
			Expected:  makeAndTree(makeEqualNode(f1, uint64(1))),
		},
		{
			Name:      "NE Condition",
			Condition: NotEqual("id", 1),
			Expected:  makeAndTree(makeNotEqualNode(f1, uint64(1))),
		},
		{
			Name:      "In Condition",
			Condition: In("id", []uint64{1, 4, 8}),
			Expected:  makeAndTree(makeInNode(f1, []uint64{1, 4, 8})),
		},
		{
			Name:      "In Condition with full range",
			Condition: In("id", []uint64{1, 2, 3}),
			Expected:  makeAndTree(makeRangeNode(f1, uint64(1), uint64(3))),
		},
		{
			Name:      "In Condition Single Element",
			Condition: In("id", []uint64{1}),
			Expected:  makeAndTree(makeEqualNode(f1, uint64(1))),
		},
		{
			Name:      "NI Condition",
			Condition: NotIn("id", []uint64{2, 3, 4}),
			Expected:  makeAndTree(makeNotInNode(f1, []uint64{2, 3, 4})),
		},
		{
			Name:      "LT Condition",
			Condition: Lt("id", 1),
			Expected:  makeAndTree(makeLtNode(f1, uint64(1))),
		},
		{
			Name:      "Le Condition",
			Condition: Le("id", 2),
			Expected:  makeAndTree(makeLeNode(f1, uint64(2))),
		},
		{
			Name:      "GT Condition",
			Condition: Gt("id", 1),
			Expected:  makeAndTree(makeGtNode(f1, uint64(1))),
		},
		{
			Name:      "Ge Condition",
			Condition: Ge("id", 1),
			Expected:  makeAndTree(makeGeNode(f1, uint64(1))),
		},
		{
			Name:      "Regexp Condition",
			Condition: Regexp("name", "zack"),
			Expected:  makeAndTree(makeRegexNode(f2, "zack")),
		},
		{
			Name:      "Range Condition",
			Condition: Range("id", 1, 10),
			Expected:  makeAndTree(makeRangeNode(f1, uint64(1), uint64(10))),
		},

		// And Condition + 2 or more conditions + single index
		{
			Name:      "And(NotEqual(2), Range(1,10)) Condition",
			Condition: And(NotEqual("id", 2), Range("id", 1, 10)),
			Expected:  makeAndTree(makeNotEqualNode(f1, uint64(2)), makeRangeNode(f1, uint64(1), uint64(10))),
		},
		{
			Name:      "And(Le(8), Range(6,10)) Condition",
			Condition: And(Le("id", 8), Range("id", 6, 10)),
			Expected:  makeAndTree(makeRangeNode(f1, uint64(6), uint64(8))),
		},
		{
			Name:      "And(RG(1,10), EQ(1))",
			Condition: And(Range("id", 1, 10), Equal("id", 1)),
			Expected:  makeAndTree(makeEqualNode(f1, uint64(1))),
		},
		{
			Name:      "AND(RG(1,10), RG(5,10)) Condition",
			Condition: And(Range("id", 1, 10), Range("id", 5, 10)),
			Expected:  makeAndTree(makeRangeNode(f1, uint64(5), uint64(10))),
		},
		{
			Name:      "AND(EQ(5), RG(0,10)) Condition",
			Condition: And(Equal("id", 5), Range("id", 0, 10)),
			Expected:  makeAndTree(makeEqualNode(f1, uint64(5))),
		},
		{
			Name:      "And(EQ(id, 1), EQ(name, hi)) Condition",
			Condition: And(Equal("id", 1), Equal("name", "hi")),
			Expected:  makeAndTree(makeEqualNode(f1, uint64(1)), makeEqualNode(f2, "hi")),
		},

		// Or Condition + 2 or more conditions + single index
		{
			Name:      "Or(Le(8), Range(6,10)) Condition",
			Condition: Or(Le("id", 8), Range("id", 6, 10)),
			Expected:  makeOrTree(makeLeNode(f1, uint64(10))),
		},
		{
			Name:      "OR(EQ(id, 1), EQ(name, hi)) Condition",
			Condition: Or(Equal("id", 1), Equal("name", "hi")),
			Expected:  makeOrTree(makeEqualNode(f1, uint64(1)), makeEqualNode(f2, "hi")),
		},
		{
			Name:      "OR(EQ, EQ) Condition",
			Condition: Or(Equal("id", 1), Equal("id", 3)),
			Expected:  makeOrTree(makeInNode(f1, []uint64{1, 3})),
		},
		{
			Name:      "OR(EQ, EQ) Condition with full range",
			Condition: Or(Equal("id", 1), Equal("id", 2)),
			Expected:  makeOrTree(makeRangeNode(f1, uint64(1), uint64(2))),
		},
		{
			Name:      "OR(RG, EQ) Condition",
			Condition: Or(Range("id", 1, 10), Equal("id", 10)),
			Expected:  makeOrTree(makeRangeNode(f1, uint64(1), uint64(10))),
		},
		{
			Name:      "OR(RG, RG, EQ) Condition",
			Condition: Or(Range("id", 1, 10), Range("id", 5, 10), Equal("id", 2)),
			Expected:  makeOrTree(makeRangeNode(f1, uint64(1), uint64(10))),
		},
		{
			Name:      "OR(In(1,2), RG(6,10)) (In) Out of Range Condition",
			Condition: Or(In("id", []int{1, 2}), Range("id", 6, 10)),
			Expected:  makeOrTree(makeRangeNode(f1, uint64(1), uint64(2)), makeRangeNode(f1, uint64(6), uint64(10))),
		},
		{
			Name:      "OR(In(6,7), RG(6,10)) In Range Condition",
			Condition: Or(In("id", []int{6, 7}), Range("id", 6, 10)),
			Expected:  makeOrTree(makeRangeNode(f1, uint64(6), uint64(10))),
		},
		{
			Name:      "Or(Le(10), Range(1,5)) Condition",
			Condition: Or(Le("id", 10), Range("id", 1, 5)),
			Expected:  makeOrTree(makeLeNode(f1, uint64(10))),
		},
		{
			Name:      "Or(Le(score, 4.5), Range(id,(0,10))) Condition",
			Condition: Or(Le("score", 4.5), Range("id", 1, 10)),
			Expected:  makeOrTree(makeLeNode(f3, float64(4.5)), makeRangeNode(f1, uint64(1), uint64(10))),
		},

		// CAT: merge nested nodes
		{
			Name:      "OR ( OR (A, B), C) ) => OR (A, B, C)",
			Condition: Or(Or(Equal("id", 1), Range("score", 2.0, 5.0)), Gt("name", "hey")),
			Expected:  makeOrTree(makeEqualNode(f1, uint64(1)), makeRangeNode(f3, float64(2.0), float64(5.0)), makeGtNode(f2, "hey")),
		},
		{
			Name:      "AND ( AND (A, B), C) => AND (A, B, C)",
			Condition: And(And(Range("id", 1, 10), Range("id", 2, 5)), Range("id", 4, 5)),
			Expected:  makeAndTree(makeRangeNode(f1, uint64(4), uint64(5))),
		},
		// CAT: replace/simplify sets
		{
			Name:      "IN(single A) => EQ(A)",
			Condition: In("id", []uint64{1}),
			Expected:  makeAndTree(makeEqualNode(f1, uint64(1))),
		},
		{
			Name:      "NI(single A) => NE(A)",
			Condition: NotIn("id", []uint64{1}),
			Expected:  makeAndTree(makeNotEqualNode(f1, uint64(1))),
		},
		{
			Name:      "And: EQ(A) + EQ(A) => EQ(A)",
			Condition: And(Equal("id", 1), Equal("id", 1)),
			Expected:  makeAndTree(makeEqualNode(f1, uint64(1))),
		},
		{
			Name:      "Or: EQ(A) + EQ(A) => EQ(A)",
			Condition: Or(Equal("id", 1), Equal("id", 1)),
			Expected:  makeOrTree(makeEqualNode(f1, uint64(1))),
		},
		{
			Name:      "Empty IN => false",
			Condition: In("id", []uint64{}),
			Expected:  makeAndTree(makeFalseNode(f1)),
		}, {
			Name:      "Empty NIN => true",
			Condition: NotIn("id", []uint64{}),
			Expected:  makeAndTree(makeTrueNode(f1)),
		},
		{
			Name:      "and: IN(A) + IN(B) => IN(A-B)",
			Condition: And(In("id", []uint64{1, 4, 8}), In("id", []uint64{4, 8, 10})),
			Expected:  makeAndTree(makeInNode(f1, []uint64{4, 8})),
		},
		{
			Name:      "and: IN(A) + IN(B) => RG(A-B) iff full range",
			Condition: And(In("id", []uint64{1, 2, 3}), In("id", []uint64{2, 3, 4})),
			Expected:  makeAndTree(makeRangeNode(f1, uint64(2), uint64(3))),
		},
		{
			Name:      "or: IN(A) + IN(B) => IN(A+B)",
			Condition: Or(In("id", []uint64{1, 4, 8}), In("id", []uint64{8, 5, 9})),
			Expected:  makeOrTree(makeInNode(f1, []uint64{1, 4, 5, 8, 9})),
		},
		{
			Name:      "or: IN(A) + IN(B) => RG(A,B) iff full range",
			Condition: Or(In("id", []uint64{1, 2, 3}), In("id", []uint64{2, 3, 4})),
			Expected:  makeOrTree(makeRangeNode(f1, uint64(1), uint64(4))),
		},
		{
			Name:      "and: NI(A) + NI(B) => NI(A+B)",
			Condition: And(NotIn("id", []uint64{1, 2, 3}), NotIn("id", []uint64{2, 3, 4})),
			Expected:  makeAndTree(makeNotInNode(f1, []uint64{1, 2, 3, 4})),
		},
		{
			Name:      "or: NI(A) + NI(B) => NI(A+B)",
			Condition: Or(NotIn("id", []uint64{1, 2, 3}), NotIn("id", []uint64{2, 3, 4})),
			Expected:  makeOrTree(makeNotInNode(f1, []uint64{2, 3})),
		},
		{
			Name:      "or: IN(A) + EQ(B) => IN(A+B)",
			Condition: Or(In("id", []uint64{1, 4, 8}), Equal("id", 2)),
			Expected:  makeOrTree(makeInNode(f1, []uint64{1, 2, 4, 8})),
		},
		{
			Name:      "or: IN(A) + EQ(B) => RG(A,B) iff B = A+1",
			Condition: Or(In("id", []uint64{1, 2, 3}), Equal("id", 4)),
			Expected:  makeOrTree(makeRangeNode(f1, uint64(1), uint64(4))),
		},
		{
			Name:      "or: EQ(A) + EQ(B) => IN(A+B)",
			Condition: Or(Equal("id", 1), Equal("id", 3)),
			Expected:  makeOrTree(makeInNode(f1, []uint64{1, 3})),
		},
		{
			Name:      "or: EQ(A) + EQ(B) => RG(A,B) iff B = A+1",
			Condition: Or(Equal("id", 1), Equal("id", 2)),
			Expected:  makeOrTree(makeRangeNode(f1, uint64(1), uint64(2))),
		},
		// CAT: replace/simplify ranges
		{
			Name:      "and: LT(A) + LT(A) => LT(A)",
			Condition: And(Lt("id", 10), Lt("id", 5)),
			Expected:  makeAndTree(makeLtNode(f1, uint64(5))),
		},
		{
			Name:      "and: LE(A) + LE(A) => LE(A)",
			Condition: And(Le("id", 10), Le("id", 5)),
			Expected:  makeAndTree(makeLeNode(f1, uint64(5))),
		},
		{
			Name:      "and: LE(0)",
			Condition: And(Le("id", 0)),
			Expected:  makeAndTree(makeEqualNode(f1, uint64(0))),
		},
		{
			Name:      "and: GT(A) + GT(A) => GT(A)",
			Condition: And(Gt("id", 2), Gt("id", 5)),
			Expected:  makeAndTree(makeGtNode(f1, uint64(5))),
		},
		{
			Name:      "and: GE(A) + GE(A) => GE(A)",
			Condition: And(Ge("id", 2), Ge("id", 5)),
			Expected:  makeAndTree(makeGeNode(f1, uint64(5))),
		},
		{
			Name:      "and: RG(A,B) + RG(C,D) => RG(B,C) iff C ≤ B",
			Condition: And(Range("id", 1, 5), Range("id", 3, 10)),
			Expected:  makeAndTree(makeRangeNode(f1, uint64(3), uint64(5))),
		},
		{
			Name:      "and: RG(A,B) + RG(B,D) => EQ(B)",
			Condition: And(Range("id", 1, 5), Range("id", 5, 10)),
			Expected:  makeAndTree(makeEqualNode(f1, uint64(5))),
		},
		{
			Name:      "and: RG(A,B) + EQ(C) => EQ(C) iff A ≤ C ≤ B",
			Condition: And(Range("id", 1, 5), Equal("id", 3)),
			Expected:  makeAndTree(makeEqualNode(f1, uint64(3))),
		},
		{
			Name:      "and: GE(A) + LE(B) => RG(A,B) iff A ≤ B",
			Condition: And(Ge("id", 1), Le("id", 5)),
			Expected:  makeAndTree(makeRangeNode(f1, uint64(1), uint64(5))),
		},
		{
			Name:      "and: GT(A) + LT(B) => RG(A+1,B-1) iff A ≤ B",
			Condition: And(Gt("id", 1), Lt("id", 5)),
			Expected:  makeAndTree(makeRangeNode(f1, uint64(2), uint64(4))),
		},
		{
			Name:      "or: RG(A,B) + EQ(C) => EQ(C) iff A ≤ C ≤ B",
			Condition: Or(Range("id", 1, 5), Equal("id", 3)),
			Expected:  makeOrTree(makeRangeNode(f1, uint64(1), uint64(5))),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.Name, func(t *testing.T) {
			// compile test filter conditions
			flt, err := tc.Condition.Compile(testSchema)
			require.NoError(t, err)

			// construct mock table from schema without index and result
			mockTable := NewMockTable(testSchema, nil, nil)

			// construct a query plan for testing
			plan := NewQueryPlan().
				WithTag(tc.Name).
				WithTable(mockTable).
				WithFilters(flt).
				WithSchema(testSchema)
			defer plan.Close()

			if testing.Verbose() {
				plan.WithLogger(log.Log).WithFlags(QueryFlagDebug)
			}

			// validate
			require.NoError(t, plan.Validate(), "validation failed")
			require.NoError(t, plan.Compile(context.TODO()), "compile failed")
			isEqual := IsFilterEqual(tc.Expected, plan.Filters)
			assert.True(t, isEqual, "unexpected filters %s", plan.Filters)
			if !isEqual {
				assert.Equal(t, tc.Expected, plan.Filters)
			}
		})
	}
}

func TestPlanQueryIndexes(t *testing.T) {
	type TestCase struct {
		Name      string
		Condition Condition
		Index     engine.QueryableIndex
		Expected  *FilterTreeNode
	}

	f1, _ := testSchema.FieldByName("id")
	// f2, _ := testSchema.FieldByName("name")
	// f3, _ := testSchema.FieldByName("score")

	testCases := []TestCase{
		// single condition + single index
		{
			Name:      "EQ Single",
			Condition: Equal("name", "a"),
			Index:     makeIndex(1),
			Expected:  makeAndTree(makeEqualNode(f1, uint64(1))),
		},
		{
			Name:      "EQ Double",
			Condition: Equal("name", "a"),
			Index:     makeIndex(1, 2),
			Expected:  makeAndTree(makeRangeNode(f1, uint64(1), uint64(2))),
		},
		{
			Name:      "EQ Triple",
			Condition: Equal("name", "a"),
			Index:     makeIndex(1, 2, 4),
			Expected:  makeAndTree(makeInNode(f1, []uint64{1, 2, 4})),
		},
		{
			Name:      "IN",
			Condition: In("name", []string{"a", "b", "c"}),
			Index:     makeIndex(1, 4, 5),
			Expected:  makeAndTree(makeInNode(f1, []uint64{1, 4, 5})),
		},
		{
			Name:      "Empty",
			Condition: Equal("name", "a"),
			Index:     makeIndex(),
			Expected:  makeAndTree(makeFalseNode(f1)),
		},
		// extra pk condition
		{
			Name:      "EQ(INDEX) AND IN(PK)",
			Condition: And(Equal("name", "a"), In("id", []uint64{1, 2})),
			Index:     makeIndex(1),
			Expected:  makeAndTree(makeEqualNode(f1, uint64(1))),
		},
		{
			Name:      "IN(INDEX) AND GT(PK)",
			Condition: And(In("name", []string{"a", "b", "c"}), Gt("id", uint64(4))),
			Index:     makeIndex(1, 4, 5),
			Expected:  makeAndTree(makeEqualNode(f1, uint64(5))),
		},
	}
	for _, tc := range testCases {
		t.Run(tc.Name, func(t *testing.T) {
			// compile test filter conditions
			flt, err := tc.Condition.Compile(testSchema)
			require.NoError(t, err)

			// construct mock table from schema, mock index and mock result
			mockTable := NewMockTable(
				testSchema,
				[]engine.QueryableIndex{tc.Index},
				nil,
			)

			// construct a query plan for testing
			plan := NewQueryPlan().
				WithTag(tc.Name).
				WithTable(mockTable).
				WithFilters(flt).
				WithSchema(testSchema)
			defer plan.Close()

			if testing.Verbose() {
				plan.WithLogger(log.Log).WithFlags(QueryFlagDebug)
			}

			// validate, compile and run index query
			require.NoError(t, plan.Validate())
			require.NoError(t, plan.Compile(context.TODO()))
			require.NoError(t, plan.QueryIndexes(context.TODO()))

			// fully processed trees should habe a top level bitmap
			if plan.Filters.IsProcessed() {
				require.True(t, plan.Filters.Bits.IsValid(), "missing bits for %s", plan.Filters)
			}

			// check
			isEqual := IsFilterEqual(tc.Expected, plan.Filters)
			assert.True(t, isEqual, "unexpected filter %s, want %s", plan.Filters, tc.Expected)
			if !isEqual {
				assert.Equal(t, tc.Expected, plan.Filters)
			}
		})
	}
}
