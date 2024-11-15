// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package cmp

import (
	"bytes"
	"fmt"
	"math"

	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/num"
	"blockwatch.cc/knoxdb/pkg/slicex"
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

func Min(t types.BlockType, a, b any) any {
	if Cmp(t, a, b) < 0 {
		return a
	}
	return b
}

func Max(t types.BlockType, a, b any) any {
	if Cmp(t, a, b) < 0 {
		return b
	}
	return a
}

func MinNumericVal(t types.BlockType) any {
	switch t {
	case types.BlockInt64, types.BlockTime:
		return int64(math.MinInt64)
	case types.BlockInt32:
		return int32(math.MinInt32)
	case types.BlockInt16:
		return int16(math.MinInt16)
	case types.BlockInt8:
		return int8(math.MinInt8)
	case types.BlockUint64:
		return uint64(0)
	case types.BlockUint32:
		return uint32(0)
	case types.BlockUint16:
		return uint16(0)
	case types.BlockUint8:
		return uint8(0)
	case types.BlockFloat32:
		return float32(-math.MaxFloat32)
	case types.BlockFloat64:
		return float64(-math.MaxFloat64)
	case types.BlockBool:
		return false
	case types.BlockInt128:
		return num.MinInt128
	case types.BlockInt256:
		return num.MinInt256
	case types.BlockString, types.BlockBytes:
		return []byte{}
	default:
		return nil
	}
}

func MaxNumericVal(t types.BlockType) any {
	switch t {
	case types.BlockInt64, types.BlockTime:
		return int64(math.MaxInt64)
	case types.BlockInt32:
		return int32(math.MaxInt32)
	case types.BlockInt16:
		return int16(math.MaxInt16)
	case types.BlockInt8:
		return int8(math.MaxInt8)
	case types.BlockUint64:
		return uint64(math.MaxUint64)
	case types.BlockUint32:
		return uint32(math.MaxUint32)
	case types.BlockUint16:
		return uint16(math.MaxUint16)
	case types.BlockUint8:
		return uint8(math.MaxUint8)
	case types.BlockFloat32:
		return float32(math.MaxFloat32)
	case types.BlockFloat64:
		return float64(math.MaxFloat64)
	case types.BlockBool:
		return true
	case types.BlockInt128:
		return num.MaxInt128
	case types.BlockInt256:
		return num.MaxInt256
	case types.BlockString, types.BlockBytes:
		return nil
	default:
		return nil
	}
}

func Cmp(t types.BlockType, a, b any) (c int) {
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
	case types.BlockString, types.BlockBytes:
		switch {
		case a == nil && b == nil:
			return 0
		case a == nil:
			return -1
		case b == nil:
			return 1
		}
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
		panic(fmt.Errorf("cmp: unsupported block type %s ", t))
	}
	return
}

func Match(m types.FilterMode, t types.BlockType, a, b any) bool {
	c := Cmp(t, a, b)

	// check by mode
	switch m {
	case types.FilterModeEqual:
		return c == 0
	case types.FilterModeNotEqual:
		return c != 0
	case types.FilterModeGt:
		return c > 0
	case types.FilterModeGe:
		return c >= 0
	case types.FilterModeLt:
		return c < 0
	case types.FilterModeLe:
		return c <= 0
	default:
		panic(fmt.Errorf("match: illegal generic filter mode %s ", m))
	}
}

func Intersect(t types.BlockType, a, b any) any {
	// compare by type
	switch t {
	case types.BlockInt64, types.BlockTime:
		x := slicex.NewOrderedNumbers[int64](a.([]int64)).SetUnique()
		y := slicex.NewOrderedNumbers[int64](b.([]int64)).SetUnique()
		return x.Intersect(y).Values
	case types.BlockUint64:
		x := slicex.NewOrderedNumbers[uint64](a.([]uint64)).SetUnique()
		y := slicex.NewOrderedNumbers[uint64](b.([]uint64)).SetUnique()
		return x.Intersect(y).Values
	case types.BlockFloat64:
		x := slicex.NewOrderedNumbers[float64](a.([]float64)).SetUnique()
		y := slicex.NewOrderedNumbers[float64](b.([]float64)).SetUnique()
		return x.Intersect(y).Values
	case types.BlockBytes, types.BlockString:
		x := slicex.NewOrderedBytes(a.([][]byte)).SetUnique()
		y := slicex.NewOrderedBytes(b.([][]byte)).SetUnique()
		return x.Intersect(y).Values
	case types.BlockInt32:
		x := slicex.NewOrderedNumbers[int32](a.([]int32)).SetUnique()
		y := slicex.NewOrderedNumbers[int32](b.([]int32)).SetUnique()
		return x.Intersect(y).Values
	case types.BlockInt16:
		x := slicex.NewOrderedNumbers[int16](a.([]int16)).SetUnique()
		y := slicex.NewOrderedNumbers[int16](b.([]int16)).SetUnique()
		return x.Intersect(y).Values
	case types.BlockInt8:
		x := slicex.NewOrderedNumbers[int8](a.([]int8)).SetUnique()
		y := slicex.NewOrderedNumbers[int8](b.([]int8)).SetUnique()
		return x.Intersect(y).Values
	case types.BlockUint32:
		x := slicex.NewOrderedNumbers[uint32](a.([]uint32)).SetUnique()
		y := slicex.NewOrderedNumbers[uint32](b.([]uint32)).SetUnique()
		return x.Intersect(y).Values
	case types.BlockUint16:
		x := slicex.NewOrderedNumbers[uint16](a.([]uint16)).SetUnique()
		y := slicex.NewOrderedNumbers[uint16](b.([]uint16)).SetUnique()
		return x.Intersect(y).Values
	case types.BlockUint8:
		x := slicex.NewOrderedNumbers[uint8](a.([]uint8)).SetUnique()
		y := slicex.NewOrderedNumbers[uint8](b.([]uint8)).SetUnique()
		return x.Intersect(y).Values
	case types.BlockFloat32:
		x := slicex.NewOrderedNumbers[float32](a.([]float32)).SetUnique()
		y := slicex.NewOrderedNumbers[float32](b.([]float32)).SetUnique()
		return x.Intersect(y).Values
	case types.BlockBool:
		x, y := toBoolBits(a.([]bool)...), toBoolBits(b.([]bool)...)
		return fromBoolBits(x & y)
	case types.BlockInt128:
		return num.Int128Intersect(a.([]num.Int128), b.([]num.Int128))
	case types.BlockInt256:
		return num.Int256Intersect(a.([]num.Int256), b.([]num.Int256))
	default:
		panic(fmt.Errorf("intersect: unsupported block type %s ", t))
	}
}

func toBoolBits(b ...bool) (r byte) {
	for _, v := range b {
		if r == 3 {
			break
		}
		if v {
			r |= 0x2
		} else {
			r |= 0x1
		}
	}
	return
}

func fromBoolBits(r byte) []bool {
	switch r {
	default:
		return []bool{}
	case 1:
		return []bool{false}
	case 2:
		return []bool{true}
	case 3:
		return []bool{false, true}
	}
}

func Union(t types.BlockType, a, b any) any {
	// compare by type
	switch t {
	case types.BlockInt64, types.BlockTime:
		x := slicex.NewOrderedNumbers[int64](a.([]int64)).SetUnique()
		y := slicex.NewOrderedNumbers[int64](b.([]int64)).SetUnique()
		return x.Union(y).Values
	case types.BlockUint64:
		x := slicex.NewOrderedNumbers[uint64](a.([]uint64)).SetUnique()
		y := slicex.NewOrderedNumbers[uint64](b.([]uint64)).SetUnique()
		return x.Union(y).Values
	case types.BlockFloat64:
		x := slicex.NewOrderedNumbers[float64](a.([]float64)).SetUnique()
		y := slicex.NewOrderedNumbers[float64](b.([]float64)).SetUnique()
		return x.Union(y).Values
	case types.BlockBytes, types.BlockString:
		x := slicex.NewOrderedBytes(a.([][]byte)).SetUnique()
		y := slicex.NewOrderedBytes(b.([][]byte)).SetUnique()
		return x.Union(y).Values
	case types.BlockInt32:
		x := slicex.NewOrderedNumbers[int32](a.([]int32)).SetUnique()
		y := slicex.NewOrderedNumbers[int32](b.([]int32)).SetUnique()
		return x.Union(y).Values
	case types.BlockInt16:
		x := slicex.NewOrderedNumbers[int16](a.([]int16)).SetUnique()
		y := slicex.NewOrderedNumbers[int16](b.([]int16)).SetUnique()
		return x.Union(y).Values
	case types.BlockInt8:
		x := slicex.NewOrderedNumbers[int8](a.([]int8)).SetUnique()
		y := slicex.NewOrderedNumbers[int8](b.([]int8)).SetUnique()
		return x.Union(y).Values
	case types.BlockUint32:
		x := slicex.NewOrderedNumbers[uint32](a.([]uint32)).SetUnique()
		y := slicex.NewOrderedNumbers[uint32](b.([]uint32)).SetUnique()
		return x.Union(y).Values
	case types.BlockUint16:
		x := slicex.NewOrderedNumbers[uint16](a.([]uint16)).SetUnique()
		y := slicex.NewOrderedNumbers[uint16](b.([]uint16)).SetUnique()
		return x.Union(y).Values
	case types.BlockUint8:
		x := slicex.NewOrderedNumbers[uint8](a.([]uint8)).SetUnique()
		y := slicex.NewOrderedNumbers[uint8](b.([]uint8)).SetUnique()
		return x.Union(y).Values
	case types.BlockFloat32:
		x := slicex.NewOrderedNumbers[float32](a.([]float32)).SetUnique()
		y := slicex.NewOrderedNumbers[float32](b.([]float32)).SetUnique()
		return x.Union(y).Values
	case types.BlockBool:
		x, y := toBoolBits(a.([]bool)...), toBoolBits(b.([]bool)...)
		return fromBoolBits(x | y)
	case types.BlockInt128:
		return num.Int128Union(a.([]num.Int128), b.([]num.Int128))
	case types.BlockInt256:
		return num.Int256Union(a.([]num.Int256), b.([]num.Int256))
	default:
		panic(fmt.Errorf("union: unsupported block type %s ", t))
	}
}

func Difference(t types.BlockType, a, b any) any {
	// compare by type
	switch t {
	case types.BlockInt64, types.BlockTime:
		x := slicex.NewOrderedNumbers[int64](a.([]int64)).SetUnique()
		y := slicex.NewOrderedNumbers[int64](b.([]int64)).SetUnique()
		return x.Difference(y).Values
	case types.BlockUint64:
		x := slicex.NewOrderedNumbers[uint64](a.([]uint64)).SetUnique()
		y := slicex.NewOrderedNumbers[uint64](b.([]uint64)).SetUnique()
		return x.Difference(y).Values
	case types.BlockFloat64:
		x := slicex.NewOrderedNumbers[float64](a.([]float64)).SetUnique()
		y := slicex.NewOrderedNumbers[float64](b.([]float64)).SetUnique()
		return x.Difference(y).Values
	case types.BlockBytes, types.BlockString:
		x := slicex.NewOrderedBytes(a.([][]byte)).SetUnique()
		y := slicex.NewOrderedBytes(b.([][]byte)).SetUnique()
		return x.Difference(y).Values
	case types.BlockInt32:
		x := slicex.NewOrderedNumbers[int32](a.([]int32)).SetUnique()
		y := slicex.NewOrderedNumbers[int32](b.([]int32)).SetUnique()
		return x.Difference(y).Values
	case types.BlockInt16:
		x := slicex.NewOrderedNumbers[int16](a.([]int16)).SetUnique()
		y := slicex.NewOrderedNumbers[int16](b.([]int16)).SetUnique()
		return x.Difference(y).Values
	case types.BlockInt8:
		x := slicex.NewOrderedNumbers[int8](a.([]int8)).SetUnique()
		y := slicex.NewOrderedNumbers[int8](b.([]int8)).SetUnique()
		return x.Difference(y).Values
	case types.BlockUint32:
		x := slicex.NewOrderedNumbers[uint32](a.([]uint32)).SetUnique()
		y := slicex.NewOrderedNumbers[uint32](b.([]uint32)).SetUnique()
		return x.Difference(y).Values
	case types.BlockUint16:
		x := slicex.NewOrderedNumbers[uint16](a.([]uint16)).SetUnique()
		y := slicex.NewOrderedNumbers[uint16](b.([]uint16)).SetUnique()
		return x.Difference(y).Values
	case types.BlockUint8:
		x := slicex.NewOrderedNumbers[uint8](a.([]uint8)).SetUnique()
		y := slicex.NewOrderedNumbers[uint8](b.([]uint8)).SetUnique()
		return x.Difference(y).Values
	case types.BlockFloat32:
		x := slicex.NewOrderedNumbers[float32](a.([]float32)).SetUnique()
		y := slicex.NewOrderedNumbers[float32](b.([]float32)).SetUnique()
		return x.Difference(y).Values
	case types.BlockBool:
		x, y := toBoolBits(a.([]bool)...), toBoolBits(b.([]bool)...)
		return fromBoolBits(x &^ y)
	case types.BlockInt128:
		return num.Int128Difference(a.([]num.Int128), b.([]num.Int128))
	case types.BlockInt256:
		return num.Int256Difference(a.([]num.Int256), b.([]num.Int256))
	default:
		panic(fmt.Errorf("union: unsupported block type %s ", t))
	}
}
