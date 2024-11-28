// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package types

type JoinType byte

const (
	InnerJoin  JoinType = iota // INNER JOIN (maybe EQUI JOIN)
	LeftJoin                   // LEFT OUTER JOIN
	RightJoin                  // RIGHT OUTER JOIN
	FullJoin                   // FULL OUTER JOIN
	CrossJoin                  // CROSS JOIN
	SelfJoin                   // unused
	AsOfJoin                   // see https://code.kx.com/q4m3/9_Queries_q-sql/#998-as-of-joins
	WindowJoin                 // see https://code.kx.com/q4m3/9_Queries_q-sql/#999-window-join
)

func (t JoinType) String() string {
	switch t {
	case InnerJoin:
		return "inner_join"
	case LeftJoin:
		return "left_join"
	case RightJoin:
		return "right_join"
	case FullJoin:
		return "full_join"
	case CrossJoin:
		return "cross_join"
	case SelfJoin:
		return "self_join"
	case AsOfJoin:
		return "as_of_join"
	case WindowJoin:
		return "window_join"
	default:
		return "invalid_join"
	}
}

func (t JoinType) IsValid() bool {
	return t >= InnerJoin && t <= WindowJoin
}
