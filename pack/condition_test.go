// Copyright (c) 2018-2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc
//go:build ignore
// +build ignore

package pack

import (
	"testing"
)

func assertNoError(t *testing.T, err error) {
	if err != nil {
		t.Errorf("%v", err)
	}
}

// test condition tree construction methods
var (
	fields = FieldList{
		{
			Index: 0,
			Name:  "A",
			Type:  FieldTypeInt64,
		},
		{
			Index: 1,
			Name:  "B",
			Type:  FieldTypeInt64,
		},
		{
			Index: 2,
			Name:  "C",
			Type:  FieldTypeInt64,
		},
		{
			Index: 3,
			Name:  "D",
			Type:  FieldTypeInt64,
		},
	}
	conds = []*Condition{
		{
			Field: fields[0],
			Mode:  FilterModeEqual,
			Value: 1,
		},
		{
			Field: fields[1],
			Mode:  FilterModeEqual,
			Value: 1,
		},
		{
			Field: fields[2],
			Mode:  FilterModeEqual,
			Value: 1,
		},
	}
)

func checkNode(
	t *testing.T,
	name string,
	node ConditionTreeNode,
	kind bool,
	empty, leaf, cond bool,
	size, depth, children int) {

	t.Logf("%s: %s\n", name, node.Dump())

	if got, want := node.Empty(), empty; got != want {
		t.Errorf("%s node isempty got=%t exp=%t", name, got, want)
	}
	if got, want := node.Cond != nil, cond; got != want {
		t.Errorf("%s node cond exist got=%t exp=%t", name, got, want)
	}
	if got, want := node.Leaf(), leaf; got != want {
		t.Errorf("%s node isleaf got=%t exp=%t", name, got, want)
	}
	if got, want := node.OrKind, kind; got != want {
		t.Errorf("%s node kind got=%t exp=%t", name, got, want)
	}
	if got, want := node.Size(), size; got != want {
		t.Errorf("%s node tree size got=%d exp=%d", name, got, want)
	}
	if got, want := node.Depth(), depth; got != want {
		t.Errorf("%s node tree depth got=%d exp=%d", name, got, want)
	}
	if got, want := len(node.Children), children; got != want {
		t.Errorf("%s node children len got=%d exp=%d", name, got, want)
	}
}

// Test for adding single nodes one-by-one to a root node. Root invariants
// are maintained, so the root cannot be a leaf.
func TestConditionTreeAdd(t *testing.T) {
	node := ConditionTreeNode{}
	checkNode(t, "EMPTY", node, COND_AND, true, false, false, 0, 0, 0)

	node.AddAndCondition(conds[0])
	checkNode(t, "Single", node, COND_AND, false, false, false, 1, 2, 1)

	node.AddAndCondition(conds[1])
	checkNode(t, "Double", node, COND_AND, false, false, false, 2, 2, 2)

	node.AddOrCondition(conds[2])
	checkNode(t, "Triple", node, COND_AND, false, false, false, 3, 2, 3)
}

// Test for binding nested tree nodes. There is no root node invariant established
// in this test. Its meant for building tree fragments.
func TestConditionTreeBind(t *testing.T) {
	table := &PackTable{
		name:   "test",
		fields: fields,
	}
	node := And(Equal("A", 1)).Bind(table.fields)
	checkNode(t, "AND(A)", node, COND_AND, false, false, false, 1, 2, 1)

	node = And(Equal("A", 1), Equal("B", 1)).Bind(table.fields)
	checkNode(t, "AND(A,B)", node, COND_AND, false, false, false, 2, 2, 2)

	node = Or(Equal("A", 1), Equal("B", 1)).Bind(table.fields)
	checkNode(t, "OR(A,B)", node, COND_OR, false, false, false, 2, 2, 2)

	node = And(
		Equal("A", 1),
		Or(Equal("B", 1), Equal("C", 1)),
	).Bind(table.fields)
	checkNode(t, "AND(A,OR(B,C))", node, COND_AND, false, false, false, 3, 3, 2)

	node = And(
		Or(Equal("B", 1), Equal("C", 1)),
		Equal("A", 1),
	).Bind(table.fields)
	checkNode(t, "AND(OR(B,C),A)", node, COND_AND, false, false, false, 3, 3, 2)
	// 1st branch is an inner node
	checkNode(t, "->OR(B,C)", node.Children[0], COND_OR, false, false, false, 2, 2, 2)
	// 2nd branch is a leaf
	checkNode(t, "->AND(A)", node.Children[1], COND_AND, false, true, true, 1, 1, 0)
}

// Tests tree construction and bind with root-node invariants as it happens in queries.
func TestConditionTreeQuery(t *testing.T) {
	table := &PackTable{
		name:   "test",
		fields: fields,
	}

	// Note: AND nodes become direct children of the root
	q := NewQuery("test").WithTable(table).AndCondition(Equal("A", 1))
	assertNoError(t, q.Compile())
	checkNode(t, "AND(A)", q.conds, COND_AND, false, false, false, 1, 2, 1)

	q = NewQuery("test").WithTable(table).AndCondition(Equal("A", 1), Equal("B", 1))
	assertNoError(t, q.Compile())
	checkNode(t, "AND(A,B)", q.conds, COND_AND, false, false, false, 2, 2, 2)

	// Note: OR nodes increase tree depth, adds 1 inner OR node and its children
	q = NewQuery("test").WithTable(table).OrCondition(Equal("A", 1), Equal("B", 1))
	assertNoError(t, q.Compile())
	checkNode(t, "OR(A,B)", q.conds, COND_AND, false, false, false, 2, 3, 1)
	checkNode(t, "OR(A,B)[0]", q.conds.Children[0], COND_OR, false, false, false, 2, 2, 2)

	q = NewQuery("test").
		WithTable(table).
		AndCondition(
			Equal("A", 1),
			Or(Equal("B", 1), Equal("C", 1)),
		)
	assertNoError(t, q.Compile())
	checkNode(t, "AND(A,OR(B,C))", q.conds, COND_AND, false, false, false, 3, 3, 2)

	q = NewQuery("test").
		WithTable(table).
		AndCondition(
			Or(Equal("B", 1), Equal("C", 1)),
			Equal("A", 1),
		)

	// Note: branches are optimized (reordered) by weight !
	assertNoError(t, q.Compile())
	checkNode(t, "AND(OR(B,C),A)", q.conds, COND_AND, false, false, false, 3, 3, 2)
	// 1st branch is AND (leaf)
	checkNode(t, "AND(A,OR(B,C))[0]", q.conds.Children[0], COND_AND, false, true, true, 1, 1, 0)
	// 2nd branch is OR node
	checkNode(t, "AND(A,OR(B,C))[1]", q.conds.Children[1], COND_OR, false, false, false, 2, 2, 2)
}
