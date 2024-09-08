// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package cmp

import (
	"bytes"
	"fmt"

	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/num"
	"blockwatch.cc/knoxdb/pkg/util"
)

var (
	EQ = makeMatchFn(types.FilterModeEqual)
	NE = makeMatchFn(types.FilterModeNotEqual)
	GT = makeMatchFn(types.FilterModeGt)
	GE = makeMatchFn(types.FilterModeGe)
	LT = makeMatchFn(types.FilterModeLt)
	LE = makeMatchFn(types.FilterModeLe)
	IN = makeMatchFn(types.FilterModeIn)
	NI = makeMatchFn(types.FilterModeNotIn)
	RG = makeMatchFn(types.FilterModeRange)
	RE = makeMatchFn(types.FilterModeRegexp)
)

func makeMatchFn(m types.FilterMode) func(t types.BlockType, a, b any) bool {
	return func(t types.BlockType, a, b any) bool {
		return Match(m, t, a, b)
	}
}

func Match(m types.FilterMode, t types.BlockType, a, b any) bool {
	var c int

	// compare by type
	switch t {
	case types.BlockInt64, types.BlockTime:
		c = util.Cmp(a.(int64), b.(int64))
	case types.BlockUint64:
		c = util.Cmp(a.(uint64), b.(uint64))
	case types.BlockFloat64:
		c = util.Cmp(a.(float64), b.(float64))
	case types.BlockBool:
		var x, y byte
		if a.(bool) {
			x = 1
		}
		if b.(bool) {
			y = 1
		}
		c = util.Cmp(x, y)
	case types.BlockString:
		c = util.Cmp(a.(string), b.(string))
	case types.BlockBytes:
		c = bytes.Compare(a.([]byte), b.([]byte))
	case types.BlockInt32:
		c = util.Cmp(a.(int32), b.(int32))
	case types.BlockInt16:
		c = util.Cmp(a.(int16), b.(int16))
	case types.BlockInt8:
		c = util.Cmp(a.(int8), b.(int8))
	case types.BlockUint32:
		c = util.Cmp(a.(uint32), b.(uint32))
	case types.BlockUint16:
		c = util.Cmp(a.(uint16), b.(uint16))
	case types.BlockUint8:
		c = util.Cmp(a.(uint8), b.(uint8))
	case types.BlockFloat32:
		c = util.Cmp(a.(float32), b.(float32))
	case types.BlockInt128:
		c = a.(num.Int128).Cmp(b.(num.Int128))
	case types.BlockInt256:
		c = a.(num.Int256).Cmp(b.(num.Int256))
	default:
		panic(fmt.Errorf("unsupported block type %s ", t))
	}

	// check by mode
	switch m {
	case types.FilterModeEqual:
		return c == 0
	case types.FilterModeNotEqual:
		return c != 0
	case types.FilterModeGt:
		return c == 1
	case types.FilterModeGe:
		return c >= 1
	case types.FilterModeLt:
		return c < 0
	case types.FilterModeLe:
		return c <= 0
	default:
		panic(fmt.Errorf("illegal generic filter mode %s ", m))
	}
}
