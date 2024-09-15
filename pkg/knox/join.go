// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package knox

import (
	"blockwatch.cc/knoxdb/internal/query"
	"blockwatch.cc/knoxdb/internal/types"
)

type (
	JoinType = types.JoinType
)

type Join struct {
	Type  JoinType
	On    JoinCondition
	Left  JoinTable
	Right JoinTable
}

type JoinCondition struct {
	Left  string
	Right string
	Mode  FilterMode
}

type JoinTable struct {
	Table  string
	Where  Condition
	Select []string // use all fields when empty
	As     []string
	Limit  uint32
}

func (j Join) Compile() (*query.JoinPlan, error) {
	// lookup tables
	// lookup fields
	// compile conditions
	return nil, ErrNotImplemented
}

// TODO: user interface
// res, err := NewJoin[T]().
//         WithTag("my_super_duper_join").
//         WithTables(tableA, tableB).
//         WithType(JoinTypeLeft).
//         WithOn("field_in_a", "field_in_b", FilterModeEqual).
//         WithOnEqual("field_in_a", "field_in_b").
//         WithFieldsAs(map[string]string{{from, to}, {from, to}}) // or map[int]string by pos
//         Run(ctx)
//         Execute(ctx, &dst)
//         Stream(ctx, func(*T)error{ ...  })
