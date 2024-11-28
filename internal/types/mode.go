// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package types

import (
	"strings"
)

type FilterMode int16

const (
	FilterModeInvalid FilterMode = iota
	FilterModeEqual
	FilterModeNotEqual
	FilterModeGt
	FilterModeGe
	FilterModeLt
	FilterModeLe
	FilterModeIn
	FilterModeNotIn
	FilterModeRange
	FilterModeRegexp
	FilterModeTrue
	FilterModeFalse
)

var filterModeOperators = [...]string{
	FilterModeInvalid:  "",
	FilterModeEqual:    "eq",
	FilterModeNotEqual: "ne",
	FilterModeGt:       "gt",
	FilterModeGe:       "ge",
	FilterModeLt:       "lt",
	FilterModeLe:       "le",
	FilterModeIn:       "in",
	FilterModeNotIn:    "ni",
	FilterModeRange:    "rg",
	FilterModeRegexp:   "re",
	FilterModeTrue:     "++",
	FilterModeFalse:    "--",
}

var filterModeSymbols = [...]string{
	FilterModeEqual:    "=",
	FilterModeNotEqual: "!=",
	FilterModeGt:       ">",
	FilterModeGe:       ">=",
	FilterModeLt:       "<",
	FilterModeLe:       "<=",
	FilterModeIn:       "IN",
	FilterModeNotIn:    "NOT IN",
	FilterModeRange:    "RANGE",
	FilterModeRegexp:   "~=",
	FilterModeTrue:     "TRUE",
	FilterModeFalse:    "FALSE",
}

func ParseFilterMode(s string) FilterMode {
	switch strings.ToLower(s) {
	case "", "eq":
		return FilterModeEqual
	case "ne":
		return FilterModeNotEqual
	case "gt":
		return FilterModeGt
	case "ge", "gte":
		return FilterModeGe
	case "lt":
		return FilterModeLt
	case "le", "lte":
		return FilterModeLe
	case "in":
		return FilterModeIn
	case "ni", "nin":
		return FilterModeNotIn
	case "rg":
		return FilterModeRange
	case "re":
		return FilterModeRegexp
	default:
		return FilterModeInvalid
	}
}

func (m FilterMode) IsValid() bool {
	return m > FilterModeInvalid && m <= FilterModeFalse
}

func (m FilterMode) Symbol() string {
	return filterModeSymbols[m]
}

func (m FilterMode) String() string {
	return filterModeOperators[m]
}
