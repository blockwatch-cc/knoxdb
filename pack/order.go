// Copyright (c) 2018-2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc
//go:build ignore
// +build ignore

package pack

import (
	"fmt"
)

type OrderType int

const (
	OrderAsc OrderType = iota
	OrderDesc
)

func (o OrderType) String() string {
	switch o {
	case OrderAsc:
		return "asc"
	case OrderDesc:
		return "desc"
	default:
		return "asc"
	}
}

func ParseOrderType(s string) (OrderType, error) {
	switch s {
	case "asc":
		return OrderAsc, nil
	case "desc":
		return OrderDesc, nil
	default:
		return OrderAsc, fmt.Errorf("pack: invalid order '%s'", s)
	}
}

func (o OrderType) MarshalText() ([]byte, error) {
	return []byte(o.String()), nil
}

func (o *OrderType) UnmarshalText(data []byte) error {
	typ, err := ParseOrderType(string(data))
	if err != nil {
		return err
	}
	*o = typ
	return nil
}
