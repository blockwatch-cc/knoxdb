// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package schema

type Model interface {
	Key() string
}

type BaseModel struct {
	Id uint64 `knox:"id,pk"`
}

func (_ BaseModel) Key() string { return "" }
