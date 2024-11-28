// Copyright (c) 2018-2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package pack

import (
	"strings"
	"time"
)

type FilterMode int

var zeroTime = time.Time{}

const (
	FilterModeInvalid FilterMode = iota
	FilterModeEqual
	FilterModeNotEqual
	FilterModeGt
	FilterModeGte
	FilterModeLt
	FilterModeLte
	FilterModeIn
	FilterModeNotIn
	FilterModeRange
	FilterModeRegexp
)

func ParseFilterMode(s string) FilterMode {
	switch strings.ToLower(s) {
	case "", "eq":
		return FilterModeEqual
	case "ne":
		return FilterModeNotEqual
	case "gt":
		return FilterModeGt
	case "gte":
		return FilterModeGte
	case "lt":
		return FilterModeLt
	case "lte":
		return FilterModeLte
	case "in":
		return FilterModeIn
	case "nin":
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
	return m != FilterModeInvalid
}

func (m FilterMode) IsScalar() bool {
	switch m {
	case FilterModeInvalid, FilterModeRange, FilterModeIn, FilterModeNotIn:
		return false
	default:
		return true
	}
}

func (m FilterMode) String() string {
	switch m {
	case FilterModeEqual:
		return "eq"
	case FilterModeNotEqual:
		return "ne"
	case FilterModeGt:
		return "gt"
	case FilterModeGte:
		return "gte"
	case FilterModeLt:
		return "lt"
	case FilterModeLte:
		return "lte"
	case FilterModeIn:
		return "in"
	case FilterModeNotIn:
		return "nin"
	case FilterModeRange:
		return "rg"
	case FilterModeRegexp:
		return "re"
	default:
		return "invalid"
	}
}

func (m FilterMode) Op() string {
	switch m {
	case FilterModeEqual:
		return "="
	case FilterModeNotEqual:
		return "!="
	case FilterModeGt:
		return ">"
	case FilterModeGte:
		return ">="
	case FilterModeLt:
		return "<"
	case FilterModeLte:
		return "<="
	case FilterModeIn:
		return "IN"
	case FilterModeNotIn:
		return "NOT IN"
	case FilterModeRange:
		return "RANGE"
	case FilterModeRegexp:
		return "~=" // LIKE in SQL, but using different syntax
	default:
		return "="
	}
}
