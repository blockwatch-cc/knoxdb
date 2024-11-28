// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package cmp

import (
	"bytes"
	"fmt"
	"math"

	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/internal/xroar"
	"blockwatch.cc/knoxdb/pkg/num"
	"blockwatch.cc/knoxdb/pkg/slicex"
	"blockwatch.cc/knoxdb/pkg/util"
	"golang.org/x/exp/constraints"
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

func makeMatchFn(mode types.FilterMode) func(typ types.BlockType, a, b any) bool {
	return func(typ types.BlockType, a, b any) bool {
		return Match(mode, typ, a, b)
	}
}

func Min(typ types.BlockType, a, b any) any {
	if Cmp(typ, a, b) < 0 {
		return a
	}
	return b
}

func Max(typ types.BlockType, a, b any) any {
	if Cmp(typ, a, b) < 0 {
		return b
	}
	return a
}

func MinNumericVal(typ types.BlockType) any {
	switch typ {
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

func MaxNumericVal(typ types.BlockType) any {
	switch typ {
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

func Cmp(typ types.BlockType, a, b any) (c int) {
	switch typ {
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
			return 1 // max is nil
		case b == nil:
			return -1 // max is nil
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
		panic(fmt.Errorf("cmp: unsupported block type %s ", typ))
	}
	return
}

func Match(mode types.FilterMode, typ types.BlockType, a, b any) bool {
	c := Cmp(typ, a, b)
	switch mode {
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
		panic(fmt.Errorf("match: illegal generic filter mode %s ", mode))
	}
}

func Unique(typ types.BlockType, a any) any {
	switch typ {
	case types.BlockInt64, types.BlockTime:
		return slicex.Unique(a.([]int64))
	case types.BlockUint64:
		return slicex.Unique(a.([]uint64))
	case types.BlockFloat64:
		return slicex.Unique(a.([]float64))
	case types.BlockBytes, types.BlockString:
		return slicex.UniqueBytes(a.([][]byte))
	case types.BlockInt32:
		return slicex.Unique(a.([]int32))
	case types.BlockInt16:
		return slicex.Unique(a.([]int16))
	case types.BlockInt8:
		return slicex.Unique(a.([]int8))
	case types.BlockUint32:
		return slicex.Unique(a.([]uint32))
	case types.BlockUint16:
		return slicex.Unique(a.([]uint16))
	case types.BlockUint8:
		return slicex.Unique(a.([]uint8))
	case types.BlockFloat32:
		return slicex.Unique(a.([]float32))
	case types.BlockBool:
		return slicex.UniqueBools(a.([]bool))
	case types.BlockInt128:
		return num.Int128Unique(a.([]num.Int128))
	case types.BlockInt256:
		return num.Int256Unique(a.([]num.Int256))
	default:
		panic(fmt.Errorf("unique: unsupported block type %s ", typ))
	}
}

func Intersect(typ types.BlockType, a, b any) any {
	switch typ {
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
		x, y := slicex.ToBoolBits(a.([]bool)...), slicex.ToBoolBits(b.([]bool)...)
		return slicex.FromBoolBits(x & y)
	case types.BlockInt128:
		return num.Int128Intersect(a.([]num.Int128), b.([]num.Int128))
	case types.BlockInt256:
		return num.Int256Intersect(a.([]num.Int256), b.([]num.Int256))
	default:
		panic(fmt.Errorf("intersect: unsupported block type %s ", typ))
	}
}

func Union(typ types.BlockType, a, b any) any {
	switch typ {
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
		x, y := slicex.ToBoolBits(a.([]bool)...), slicex.ToBoolBits(b.([]bool)...)
		return slicex.FromBoolBits(x | y)
	case types.BlockInt128:
		return num.Int128Union(a.([]num.Int128), b.([]num.Int128))
	case types.BlockInt256:
		return num.Int256Union(a.([]num.Int256), b.([]num.Int256))
	default:
		panic(fmt.Errorf("union: unsupported block type %s ", typ))
	}
}

func Difference(typ types.BlockType, a, b any) any {
	switch typ {
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
		x, y := slicex.ToBoolBits(a.([]bool)...), slicex.ToBoolBits(b.([]bool)...)
		return slicex.FromBoolBits(x &^ y)
	case types.BlockInt128:
		return num.Int128Difference(a.([]num.Int128), b.([]num.Int128))
	case types.BlockInt256:
		return num.Int256Difference(a.([]num.Int256), b.([]num.Int256))
	default:
		panic(fmt.Errorf("difference: unsupported block type %s ", typ))
	}
}

// Range returns min, max of a set and whether all values between min and
// max are present, i.e. the set is complete.
func Range(typ types.BlockType, set any) (minv any, maxv any, isComplete bool) {
	if bs, ok := set.(*xroar.Bitmap); ok {
		minU64 := bs.Minimum()
		maxU64 := bs.Maximum()
		isComplete = maxU64-minU64+1 == uint64(bs.GetCardinality())
		minv, _ = Cast(typ, minU64)
		maxv, _ = Cast(typ, maxU64)
		return
	}
	switch typ {
	case types.BlockInt64, types.BlockTime:
		x := slicex.NewOrderedNumbers[int64](set.([]int64))
		minv, maxv = x.MinMax()
		isComplete = x.IsFull()
	case types.BlockInt32:
		x := slicex.NewOrderedNumbers[int32](set.([]int32))
		minv, maxv = x.MinMax()
		isComplete = x.IsFull()
	case types.BlockInt16:
		x := slicex.NewOrderedNumbers[int16](set.([]int16))
		minv, maxv = x.MinMax()
		isComplete = x.IsFull()
	case types.BlockInt8:
		x := slicex.NewOrderedNumbers[int8](set.([]int8))
		minv, maxv = x.MinMax()
		isComplete = x.IsFull()
	case types.BlockUint64:
		x := slicex.NewOrderedNumbers[uint64](set.([]uint64))
		minv, maxv = x.MinMax()
		isComplete = x.IsFull()
	case types.BlockUint32:
		x := slicex.NewOrderedNumbers[uint32](set.([]uint32))
		minv, maxv = x.MinMax()
		isComplete = x.IsFull()
	case types.BlockUint16:
		x := slicex.NewOrderedNumbers[uint16](set.([]uint16))
		minv, maxv = x.MinMax()
		isComplete = x.IsFull()
	case types.BlockUint8:
		x := slicex.NewOrderedNumbers[uint8](set.([]uint8))
		minv, maxv = x.MinMax()
		isComplete = x.IsFull()
	case types.BlockInt128:
		i128s := set.([]num.Int128)
		mini, maxi := num.Int128MinMax(num.Int128Sort(i128s))
		minv, maxv = mini, maxi
		isComplete = int(maxi.Sub(mini).Int64()+1) == len(i128s)
	case types.BlockInt256:
		i256s := set.([]num.Int256)
		mini, maxi := num.Int256MinMax(num.Int256Sort(i256s))
		minv, maxv = mini, maxi
		isComplete = int(maxi.Sub(mini).Int64()+1) == len(i256s)
	case types.BlockFloat64:
		x := slicex.NewOrderedNumbers(set.([]float64))
		minv, maxv = x.MinMax()
		isComplete = false
	case types.BlockFloat32:
		x := slicex.NewOrderedNumbers(set.([]float32))
		minv, maxv = x.MinMax()
		isComplete = false
	case types.BlockBool:
		switch slicex.ToBoolBits(set.([]bool)...) {
		case 0:
			minv, maxv, isComplete = false, false, false
		case 1:
			minv, maxv, isComplete = false, false, false
		case 2:
			minv, maxv, isComplete = true, true, false
		case 3:
			minv, maxv, isComplete = false, true, true
		}
	case types.BlockString, types.BlockBytes:
		x := slicex.NewOrderedBytes(set.([][]byte))
		minv, maxv = x.MinMax()
		isComplete = false
	default:
		panic(fmt.Errorf("range: unsupported block type %s ", typ))
	}
	return
}

// Cast casts any Go integer type into a compatible type
// for a block.
func Cast(typ types.BlockType, val any) (res any, ok bool) {
	switch typ {
	case types.BlockInt64, types.BlockTime:
		res, ok = cast[int64](val)
	case types.BlockInt32:
		res, ok = cast[int32](val)
	case types.BlockInt16:
		res, ok = cast[int16](val)
	case types.BlockInt8:
		res, ok = cast[int8](val)
	case types.BlockUint64:
		res, ok = cast[uint64](val)
	case types.BlockUint32:
		res, ok = cast[uint32](val)
	case types.BlockUint16:
		res, ok = cast[uint16](val)
	case types.BlockUint8:
		res, ok = cast[uint8](val)
	default:
		ok = false
	}
	return
}

func cast[T constraints.Integer](val any) (t T, ok bool) {
	ok = true
	switch v := val.(type) {
	case int:
		t = T(v)
	case int64:
		t = T(v)
	case int32:
		t = T(v)
	case int16:
		t = T(v)
	case int8:
		t = T(v)
	case uint:
		t = T(v)
	case uint64:
		t = T(v)
	case uint32:
		t = T(v)
	case uint16:
		t = T(v)
	case uint8:
		t = T(v)
	default:
		ok = false
	}
	return
}

func Inc(typ types.BlockType, v any) any {
	switch typ {
	case types.BlockUint64:
		return v.(uint64) + 1
	case types.BlockUint32:
		return v.(uint32) + 1
	case types.BlockUint16:
		return v.(uint16) + 1
	case types.BlockUint8:
		return v.(uint8) + 1
	case types.BlockInt64:
		return v.(int64) + 1
	case types.BlockInt32:
		return v.(int32) + 1
	case types.BlockInt16:
		return v.(int16) + 1
	case types.BlockInt8:
		return v.(int8) + 1
	case types.BlockInt128:
		return v.(num.Int128).Add64(1)
	case types.BlockInt256:
		return v.(num.Int256).Add64(1)
	case types.BlockBool:
		return true
	case types.BlockFloat64:
		return math.Nextafter(v.(float64), MaxNumericVal(typ).(float64))
	case types.BlockFloat32:
		return math.Nextafter32(v.(float32), MaxNumericVal(typ).(float32))
	case types.BlockString, types.BlockBytes:
		c := bytes.Clone(v.([]byte))
		var ok bool
		for i := len(c) - 1; i >= 0; i-- {
			if c[i] < 0xff {
				c[i] += 1
				ok = true
				break
			}
			c[i] = 0
		}
		if !ok {
			c = append([]byte{1}, c...)
		}
		return c
	default:
		panic(fmt.Errorf("inc: unsupported block type %s ", typ))
	}
}

func Dec(typ types.BlockType, v any) any {
	switch typ {
	case types.BlockUint64:
		return v.(uint64) - 1
	case types.BlockUint32:
		return v.(uint32) - 1
	case types.BlockUint16:
		return v.(uint16) - 1
	case types.BlockUint8:
		return v.(uint8) - 1
	case types.BlockInt64:
		return v.(int64) - 1
	case types.BlockInt32:
		return v.(int32) - 1
	case types.BlockInt16:
		return v.(int16) - 1
	case types.BlockInt8:
		return v.(int8) - 1
	case types.BlockInt128:
		return v.(num.Int128).Sub64(1)
	case types.BlockInt256:
		return v.(num.Int256).Sub64(1)
	case types.BlockBool:
		return false
	case types.BlockFloat64:
		return math.Nextafter(v.(float64), MinNumericVal(typ).(float64))
	case types.BlockFloat32:
		return math.Nextafter32(v.(float32), MinNumericVal(typ).(float32))
	case types.BlockString, types.BlockBytes:
		c := bytes.Clone(v.([]byte))
		var ok bool
		for i := len(c) - 1; i >= 0; i-- {
			if c[i] > 0 {
				c[i] -= 1
				ok = true
				break
			}
		}
		if !ok && len(c) > 0 {
			c = c[:len(c)-1]
		}
		return c
	default:
		panic(fmt.Errorf("dec: unsupported block type %s ", typ))
	}
}

func RemoveRange(typ types.BlockType, s, from, to any) any {
	switch typ {
	case types.BlockInt64, types.BlockTime:
		return slicex.NewOrderedNumbers[int64](s.([]int64)).
			RemoveRange(from.(int64), to.(int64)).Values
	case types.BlockUint64:
		return slicex.NewOrderedNumbers[uint64](s.([]uint64)).
			RemoveRange(from.(uint64), to.(uint64)).Values
	case types.BlockFloat64:
		return slicex.NewOrderedNumbers[float64](s.([]float64)).
			RemoveRange(from.(float64), to.(float64)).Values
	case types.BlockBytes, types.BlockString:
		return slicex.NewOrderedBytes(s.([][]byte)).
			RemoveRange(from.([]byte), to.([]byte)).Values
	case types.BlockInt32:
		return slicex.NewOrderedNumbers[int32](s.([]int32)).
			RemoveRange(from.(int32), to.(int32)).Values
	case types.BlockInt16:
		return slicex.NewOrderedNumbers[int16](s.([]int16)).
			RemoveRange(from.(int16), to.(int16)).Values
	case types.BlockInt8:
		return slicex.NewOrderedNumbers[int8](s.([]int8)).
			RemoveRange(from.(int8), to.(int8)).Values
	case types.BlockUint32:
		return slicex.NewOrderedNumbers[uint32](s.([]uint32)).
			RemoveRange(from.(uint32), to.(uint32)).Values
	case types.BlockUint16:
		return slicex.NewOrderedNumbers[uint16](s.([]uint16)).
			RemoveRange(from.(uint16), to.(uint16)).Values
	case types.BlockUint8:
		return slicex.NewOrderedNumbers[uint8](s.([]uint8)).
			RemoveRange(from.(uint8), to.(uint8)).Values
	case types.BlockFloat32:
		return slicex.NewOrderedNumbers[float32](s.([]float32)).
			RemoveRange(from.(float32), to.(float32)).Values
	case types.BlockBool:
		x := slicex.ToBoolBits(s.([]bool)...)
		x &^= slicex.ToBoolBits(from.(bool))
		x &^= slicex.ToBoolBits(to.(bool))
		return slicex.FromBoolBits(x)
	case types.BlockInt128:
		return num.Int128RemoveRange(s.([]num.Int128), from.(num.Int128), to.(num.Int128))
	case types.BlockInt256:
		return num.Int256RemoveRange(s.([]num.Int256), from.(num.Int256), to.(num.Int256))
	default:
		panic(fmt.Errorf("remove range: unsupported block type %s ", typ))
	}
}

func IntersectRange(typ types.BlockType, s, from, to any) any {
	switch typ {
	case types.BlockInt64, types.BlockTime:
		return slicex.NewOrderedNumbers[int64](s.([]int64)).
			IntersectRange(from.(int64), to.(int64)).Values
	case types.BlockUint64:
		return slicex.NewOrderedNumbers[uint64](s.([]uint64)).
			IntersectRange(from.(uint64), to.(uint64)).Values
	case types.BlockFloat64:
		return slicex.NewOrderedNumbers[float64](s.([]float64)).
			IntersectRange(from.(float64), to.(float64)).Values
	case types.BlockBytes, types.BlockString:
		return slicex.NewOrderedBytes(s.([][]byte)).
			IntersectRange(from.([]byte), to.([]byte)).Values
	case types.BlockInt32:
		return slicex.NewOrderedNumbers[int32](s.([]int32)).
			IntersectRange(from.(int32), to.(int32)).Values
	case types.BlockInt16:
		return slicex.NewOrderedNumbers[int16](s.([]int16)).
			IntersectRange(from.(int16), to.(int16)).Values
	case types.BlockInt8:
		return slicex.NewOrderedNumbers[int8](s.([]int8)).
			IntersectRange(from.(int8), to.(int8)).Values
	case types.BlockUint32:
		return slicex.NewOrderedNumbers[uint32](s.([]uint32)).
			IntersectRange(from.(uint32), to.(uint32)).Values
	case types.BlockUint16:
		return slicex.NewOrderedNumbers[uint16](s.([]uint16)).
			IntersectRange(from.(uint16), to.(uint16)).Values
	case types.BlockUint8:
		return slicex.NewOrderedNumbers[uint8](s.([]uint8)).
			IntersectRange(from.(uint8), to.(uint8)).Values
	case types.BlockFloat32:
		return slicex.NewOrderedNumbers[float32](s.([]float32)).
			IntersectRange(from.(float32), to.(float32)).Values
	case types.BlockBool:
		x := slicex.ToBoolBits(s.([]bool)...)
		x &= slicex.ToBoolBits(from.(bool))
		x &= slicex.ToBoolBits(to.(bool))
		return slicex.FromBoolBits(x)
	case types.BlockInt128:
		return num.Int128IntersectRange(s.([]num.Int128), from.(num.Int128), to.(num.Int128))
	case types.BlockInt256:
		return num.Int256IntersectRange(s.([]num.Int256), from.(num.Int256), to.(num.Int256))
	default:
		panic(fmt.Errorf("intersect range: unsupported block type %s ", typ))
	}
}
