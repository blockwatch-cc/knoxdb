// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package types

import (
	"fmt"
)

type OrderType byte

const (
	OrderAsc OrderType = iota
	OrderDesc
	OrderAscCaseInsensitive
	OrderDescCaseInsensitive
)

var (
	orderTypeString  = "__asc_desc_iasc_idesc"
	orderTypeIdx     = [...]int{0, 2, 6, 11, 16, 22}
	orderTypeReverse = map[string]OrderType{}
)

func init() {
	for t := OrderAsc; t <= OrderDescCaseInsensitive; t++ {
		orderTypeReverse[t.String()] = t
	}
}

func (t OrderType) IsForward() bool {
	return t == OrderAsc || t == OrderAscCaseInsensitive
}

func (t OrderType) IsReverse() bool {
	return t == OrderDesc || t == OrderDescCaseInsensitive
}

func (t OrderType) IsValid() bool {
	return t >= OrderAsc && t <= OrderDescCaseInsensitive
}

func (t OrderType) String() string {
	return orderTypeString[orderTypeIdx[t] : orderTypeIdx[t+1]-1]
}

func (t OrderType) IsCaseSensitive() bool {
	switch t {
	case OrderAscCaseInsensitive, OrderDescCaseInsensitive:
		return true
	default:
		return false
	}
}

func ParseOrderType(s string) (OrderType, error) {
	t, ok := orderTypeReverse[s]
	if ok {
		return t, nil
	}
	return OrderAsc, fmt.Errorf("invalid order %q", s)
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
