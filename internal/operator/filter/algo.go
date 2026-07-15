// Copyright (c) 2026 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package filter

import (
	"bytes"
	"cmp"
	"errors"
	"math"

	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/pkg/num"
	"blockwatch.cc/knoxdb/pkg/slicex"
)

type ValueType BlockType

func ToValueType(ft types.FieldType) ValueType {
	return ValueType(types.ToBlockType(ft))
}

func (v ValueType) String() string {
	return BlockType(v).String()
}

func (t ValueType) IsInt() bool {
	switch BlockType(t) {
	case BlockInt64, BlockInt32, BlockInt16, BlockInt8,
		BlockUint64, BlockUint32, BlockUint16, BlockUint8:
		return true
	default:
		return false
	}
}

func (t ValueType) Min(a, b any) any {
	if t.Cmp(a, b) < 0 {
		return a
	}
	return b
}

func (t ValueType) Max(a, b any) any {
	if t.Cmp(a, b) < 0 {
		return b
	}
	return a
}

func (t ValueType) MinNumericVal() any {
	switch BlockType(t) {
	case BlockInt64:
		return int64(math.MinInt64)
	case BlockInt32:
		return int32(math.MinInt32)
	case BlockInt16:
		return int16(math.MinInt16)
	case BlockInt8:
		return int8(math.MinInt8)
	case BlockUint64:
		return uint64(0)
	case BlockUint32:
		return uint32(0)
	case BlockUint16:
		return uint16(0)
	case BlockUint8:
		return uint8(0)
	case BlockFloat32:
		return float32(-math.MaxFloat32)
	case BlockFloat64:
		return float64(-math.MaxFloat64)
	case BlockBool:
		return false
	case BlockInt128:
		return num.MinInt128
	case BlockInt256:
		return num.MinInt256
	case BlockBytes:
		return []byte{}
	default:
		panic(errors.New("min: unsupported block type " + t.String()))
	}
}

func (t ValueType) MaxNumericVal() any {
	switch BlockType(t) {
	case BlockInt64:
		return int64(math.MaxInt64)
	case BlockInt32:
		return int32(math.MaxInt32)
	case BlockInt16:
		return int16(math.MaxInt16)
	case BlockInt8:
		return int8(math.MaxInt8)
	case BlockUint64:
		return uint64(math.MaxUint64)
	case BlockUint32:
		return uint32(math.MaxUint32)
	case BlockUint16:
		return uint16(math.MaxUint16)
	case BlockUint8:
		return uint8(math.MaxUint8)
	case BlockFloat32:
		return float32(math.MaxFloat32)
	case BlockFloat64:
		return float64(math.MaxFloat64)
	case BlockBool:
		return true
	case BlockInt128:
		return num.MaxInt128
	case BlockInt256:
		return num.MaxInt256
	case BlockBytes:
		return nil
	default:
		panic(errors.New("max: unsupported block type " + t.String()))
	}
}

func (t ValueType) Add(a, b any) any {
	switch BlockType(t) {
	case BlockUint64:
		return a.(uint64) + b.(uint64)
	case BlockUint32:
		return a.(uint32) + b.(uint32)
	case BlockUint16:
		return a.(uint16) + b.(uint16)
	case BlockUint8:
		return a.(uint8) + b.(uint8)
	case BlockInt64:
		return a.(int64) + b.(int64)
	case BlockInt32:
		return a.(int32) + b.(int32)
	case BlockInt16:
		return a.(int16) + b.(int16)
	case BlockInt8:
		return a.(int8) + b.(int8)
	case BlockInt128:
		return a.(num.Int128).Add(b.(num.Int128))
	case BlockInt256:
		return a.(num.Int256).Add(b.(num.Int256))
	case BlockBool:
		return a.(bool) || b.(bool)
	case BlockFloat64:
		return a.(float64) + b.(float64)
	case BlockFloat32:
		return a.(float32) + b.(float32)
	case BlockBytes:
		return append(bytes.Clone(a.([]byte)), b.([]byte)...)
	default:
		panic(errors.New("add: unsupported block type " + t.String()))
	}
}

func (t ValueType) Inc(v any) any {
	switch BlockType(t) {
	case BlockUint64:
		return v.(uint64) + 1
	case BlockUint32:
		return v.(uint32) + 1
	case BlockUint16:
		return v.(uint16) + 1
	case BlockUint8:
		return v.(uint8) + 1
	case BlockInt64:
		return v.(int64) + 1
	case BlockInt32:
		return v.(int32) + 1
	case BlockInt16:
		return v.(int16) + 1
	case BlockInt8:
		return v.(int8) + 1
	case BlockInt128:
		return v.(num.Int128).Add64(1)
	case BlockInt256:
		return v.(num.Int256).Add64(1)
	case BlockBool:
		return true
	case BlockFloat64:
		return math.Nextafter(v.(float64), math.MaxFloat64)
	case BlockFloat32:
		return math.Nextafter32(v.(float32), math.MaxFloat32)
	case BlockBytes:
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
		panic(errors.New("inc: unsupported block type " + t.String()))
	}
}

func (t ValueType) Dec(v any) any {
	switch BlockType(t) {
	case BlockUint64:
		return v.(uint64) - 1
	case BlockUint32:
		return v.(uint32) - 1
	case BlockUint16:
		return v.(uint16) - 1
	case BlockUint8:
		return v.(uint8) - 1
	case BlockInt64:
		return v.(int64) - 1
	case BlockInt32:
		return v.(int32) - 1
	case BlockInt16:
		return v.(int16) - 1
	case BlockInt8:
		return v.(int8) - 1
	case BlockInt128:
		return v.(num.Int128).Sub64(1)
	case BlockInt256:
		return v.(num.Int256).Sub64(1)
	case BlockBool:
		return false
	case BlockFloat64:
		return math.Nextafter(v.(float64), -math.MaxFloat64)
	case BlockFloat32:
		return math.Nextafter32(v.(float32), -math.MaxFloat32)
	case BlockBytes:
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
		panic(errors.New("dec: unsupported block type " + t.String()))
	}
}

func (t ValueType) Zero() any {
	switch BlockType(t) {
	case BlockUint64:
		return uint64(0)
	case BlockUint32:
		return uint32(0)
	case BlockUint16:
		return uint16(0)
	case BlockUint8:
		return uint8(0)
	case BlockInt64:
		return int64(0)
	case BlockInt32:
		return int32(0)
	case BlockInt16:
		return int16(0)
	case BlockInt8:
		return int8(0)
	case BlockInt128:
		return num.ZeroInt128
	case BlockInt256:
		return num.ZeroInt256
	case BlockBool:
		return false
	case BlockFloat64:
		return float64(0)
	case BlockFloat32:
		return float32(0)
	case BlockBytes:
		return []byte{}
	default:
		panic(errors.New("zero: unsupported block type " + t.String()))
	}
}

func Cast[T types.Integer](val any) (t T, ok bool) {
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

// Cast casts any Go integer type into a compatible Go type for a block.
func (t ValueType) Cast(val any) (res any, ok bool) {
	switch BlockType(t) {
	case BlockInt64:
		res, ok = Cast[int64](val)
	case BlockInt32:
		res, ok = Cast[int32](val)
	case BlockInt16:
		res, ok = Cast[int16](val)
	case BlockInt8:
		res, ok = Cast[int8](val)
	case BlockUint64:
		res, ok = Cast[uint64](val)
	case BlockUint32:
		res, ok = Cast[uint32](val)
	case BlockUint16:
		res, ok = Cast[uint16](val)
	case BlockUint8:
		res, ok = Cast[uint8](val)
	default:
		ok = false
	}
	return
}

func (t ValueType) Cmp(a, b any) (c int) {
	switch BlockType(t) {
	case BlockInt64:
		c = cmp.Compare(a.(int64), b.(int64))
	case BlockUint64:
		c = cmp.Compare(a.(uint64), b.(uint64))
	case BlockFloat64:
		c = cmp.Compare(a.(float64), b.(float64))
	case BlockBool:
		c = CmpBool(a.(bool), b.(bool))
	case BlockBytes:
		// check nil interface (nil == empty slice)
		switch {
		case a == nil && b == nil:
			return 0
		case a == nil:
			c = 1 // nil < empty < []{...}
		case b == nil:
			c = -1 // nil < empty < []{...}
		default:
			c = bytes.Compare(a.([]byte), b.([]byte))
		}
	case BlockInt32:
		c = cmp.Compare(a.(int32), b.(int32))
	case BlockInt16:
		c = cmp.Compare(a.(int16), b.(int16))
	case BlockInt8:
		c = cmp.Compare(a.(int8), b.(int8))
	case BlockUint32:
		c = cmp.Compare(a.(uint32), b.(uint32))
	case BlockUint16:
		c = cmp.Compare(a.(uint16), b.(uint16))
	case BlockUint8:
		c = cmp.Compare(a.(uint8), b.(uint8))
	case BlockFloat32:
		c = cmp.Compare(a.(float32), b.(float32))
	case BlockInt128:
		c = a.(num.Int128).Cmp(b.(num.Int128))
	case BlockInt256:
		c = a.(num.Int256).Cmp(b.(num.Int256))
	default:
		panic(errors.New("cmp: unsupported block type " + t.String()))
	}
	return
}

// false < true
func CmpBool(a, b bool) int {
	switch {
	case a == b:
		return 0
	case b:
		return -1
	default:
		return 1
	}
}

func (t ValueType) Match(mode FilterMode, a, b any) bool {
	c := t.Cmp(a, b)
	switch mode {
	case FilterModeEqual:
		return c == 0
	case FilterModeNotEqual:
		return c != 0
	case FilterModeGt:
		return c > 0
	case FilterModeGe:
		return c >= 0
	case FilterModeLt:
		return c < 0
	case FilterModeLe:
		return c <= 0
	default:
		panic(errors.New("match: illegal generic filter mode " + mode.String()))
	}
}

func (t ValueType) EQ(a, b any) bool { return t.Cmp(a, b) == 0 }
func (t ValueType) NE(a, b any) bool { return t.Cmp(a, b) != 0 }
func (t ValueType) GT(a, b any) bool { return t.Cmp(a, b) > 0 }
func (t ValueType) GE(a, b any) bool { return t.Cmp(a, b) >= 0 }
func (t ValueType) LT(a, b any) bool { return t.Cmp(a, b) < 0 }
func (t ValueType) LE(a, b any) bool { return t.Cmp(a, b) <= 0 }

type MinMaxSet interface {
	Min() uint64
	Max() uint64
	Count() int
}

// Range returns min, max of a set and whether all values between min and
// max are present, i.e. the set is complete.
func (t ValueType) Range(set any) (minv any, maxv any, isContinuous bool) {
	if bs, ok := set.(MinMaxSet); ok {
		minU64 := bs.Min()
		maxU64 := bs.Max()
		isContinuous = maxU64-minU64+1 == uint64(bs.Count())
		minv, _ = t.Cast(minU64)
		maxv, _ = t.Cast(maxU64)
		return
	}
	switch BlockType(t) {
	case BlockInt64:
		minv, maxv, isContinuous = slicex.Range(set.([]int64))
	case BlockInt32:
		minv, maxv, isContinuous = slicex.Range(set.([]int32))
	case BlockInt16:
		minv, maxv, isContinuous = slicex.Range(set.([]int16))
	case BlockInt8:
		minv, maxv, isContinuous = slicex.Range(set.([]int8))
	case BlockUint64:
		minv, maxv, isContinuous = slicex.Range(set.([]uint64))
	case BlockUint32:
		minv, maxv, isContinuous = slicex.Range(set.([]uint32))
	case BlockUint16:
		minv, maxv, isContinuous = slicex.Range(set.([]uint16))
	case BlockUint8:
		minv, maxv, isContinuous = slicex.Range(set.([]uint8))
	case BlockInt128:
		i128s := set.([]num.Int128)
		mini, maxi := num.MinMaxInt128(num.SortInt128(i128s))
		minv, maxv = mini, maxi
		isContinuous = int(maxi.Sub(mini).Int64()+1) == len(i128s)
	case BlockInt256:
		i256s := set.([]num.Int256)
		mini, maxi := num.MinMaxInt256(num.SortInt256(i256s))
		minv, maxv = mini, maxi
		isContinuous = int(maxi.Sub(mini).Int64()+1) == len(i256s)
	case BlockFloat64:
		minv, maxv, isContinuous = slicex.RangeFloat(set.([]float64))
	case BlockFloat32:
		minv, maxv, isContinuous = slicex.RangeFloat(set.([]float32))
	case BlockBool:
		switch slicex.ToBoolBits(set.([]bool)...) {
		case 0:
			minv, maxv, isContinuous = false, false, false
		case 1:
			minv, maxv, isContinuous = false, false, false
		case 2:
			minv, maxv, isContinuous = true, true, false
		case 3:
			minv, maxv, isContinuous = false, true, true
		}
	case BlockBytes:
		minv, maxv = slicex.RangeBytes(set.([][]byte))
		isContinuous = false
	default:
		panic(errors.New("range: unsupported block type " + t.String()))
	}
	return
}

func (t ValueType) IntersectRange(s, from, to any) any {
	switch BlockType(t) {
	case BlockInt64:
		return slicex.IntersectRange(s.([]int64), from.(int64), to.(int64))
	case BlockUint64:
		return slicex.IntersectRange(s.([]uint64), from.(uint64), to.(uint64))
	case BlockFloat64:
		return slicex.IntersectRangeFloat(s.([]float64), from.(float64), to.(float64))
	case BlockBytes:
		return slicex.IntersectRangeBytes(s.([][]byte), from.([]byte), to.([]byte))
	case BlockInt32:
		return slicex.IntersectRange(s.([]int32), from.(int32), to.(int32))
	case BlockInt16:
		return slicex.IntersectRange(s.([]int16), from.(int16), to.(int16))
	case BlockInt8:
		return slicex.IntersectRange(s.([]int8), from.(int8), to.(int8))
	case BlockUint32:
		return slicex.IntersectRange(s.([]uint32), from.(uint32), to.(uint32))
	case BlockUint16:
		return slicex.IntersectRange(s.([]uint16), from.(uint16), to.(uint16))
	case BlockUint8:
		return slicex.IntersectRange(s.([]uint8), from.(uint8), to.(uint8))
	case BlockFloat32:
		return slicex.IntersectRangeFloat(s.([]float32), from.(float32), to.(float32))
	case BlockBool:
		x := slicex.ToBoolBits(s.([]bool)...)
		x &= slicex.ToBoolBits(from.(bool))
		x &= slicex.ToBoolBits(to.(bool))
		return slicex.FromBoolBits(x)
	case BlockInt128:
		return num.IntersectRangeInt128(s.([]num.Int128), from.(num.Int128), to.(num.Int128))
	case BlockInt256:
		return num.IntersectRangeInt256(s.([]num.Int256), from.(num.Int256), to.(num.Int256))
	default:
		panic(errors.New("intersect range: unsupported block type " + t.String()))
	}
}

func (t ValueType) Unique(a any) any {
	switch BlockType(t) {
	case BlockInt64:
		return slicex.Unique(a.([]int64))
	case BlockUint64:
		return slicex.Unique(a.([]uint64))
	case BlockFloat64:
		return slicex.UniqueFloat(a.([]float64))
	case BlockBytes:
		return slicex.UniqueBytes(a.([][]byte))
	case BlockInt32:
		return slicex.Unique(a.([]int32))
	case BlockInt16:
		return slicex.Unique(a.([]int16))
	case BlockInt8:
		return slicex.Unique(a.([]int8))
	case BlockUint32:
		return slicex.Unique(a.([]uint32))
	case BlockUint16:
		return slicex.Unique(a.([]uint16))
	case BlockUint8:
		return slicex.Unique(a.([]uint8))
	case BlockFloat32:
		return slicex.UniqueFloat(a.([]float32))
	case BlockBool:
		return slicex.UniqueBool(a.([]bool))
	case BlockInt128:
		return num.UniqueInt128(a.([]num.Int128))
	case BlockInt256:
		return num.UniqueInt256(a.([]num.Int256))
	default:
		panic(errors.New("unique: unsupported block type " + t.String()))
	}
}

func (t ValueType) Intersect(a, b any) any {
	switch BlockType(t) {
	case BlockInt64:
		return slicex.Intersect(a.([]int64), b.([]int64))
	case BlockUint64:
		return slicex.Intersect(a.([]uint64), b.([]uint64))
	case BlockFloat64:
		return slicex.IntersectFloat(a.([]float64), b.([]float64))
	case BlockBytes:
		return slicex.IntersectBytes(a.([][]byte), b.([][]byte))
	case BlockInt32:
		return slicex.Intersect(a.([]int32), b.([]int32))
	case BlockInt16:
		return slicex.Intersect(a.([]int16), b.([]int16))
	case BlockInt8:
		return slicex.Intersect(a.([]int8), b.([]int8))
	case BlockUint32:
		return slicex.Intersect(a.([]uint32), b.([]uint32))
	case BlockUint16:
		return slicex.Intersect(a.([]uint16), b.([]uint16))
	case BlockUint8:
		return slicex.Intersect(a.([]uint8), b.([]uint8))
	case BlockFloat32:
		return slicex.IntersectFloat(a.([]float32), b.([]float32))
	case BlockBool:
		x, y := slicex.ToBoolBits(a.([]bool)...), slicex.ToBoolBits(b.([]bool)...)
		return slicex.FromBoolBits(x & y)
	case BlockInt128:
		return num.IntersectInt128(a.([]num.Int128), b.([]num.Int128))
	case BlockInt256:
		return num.IntersectInt256(a.([]num.Int256), b.([]num.Int256))
	default:
		panic(errors.New("intersect: unsupported block type " + t.String()))
	}
}

func (t ValueType) Union(a, b any) any {
	switch BlockType(t) {
	case BlockInt64:
		return slicex.Union(a.([]int64), b.([]int64))
	case BlockUint64:
		return slicex.Union(a.([]uint64), b.([]uint64))
	case BlockFloat64:
		return slicex.UnionFloat(a.([]float64), b.([]float64))
	case BlockBytes:
		return slicex.UnionBytes(a.([][]byte), b.([][]byte))
	case BlockInt32:
		return slicex.Union(a.([]int32), b.([]int32))
	case BlockInt16:
		return slicex.Union(a.([]int16), b.([]int16))
	case BlockInt8:
		return slicex.Union(a.([]int8), b.([]int8))
	case BlockUint32:
		return slicex.Union(a.([]uint32), b.([]uint32))
	case BlockUint16:
		return slicex.Union(a.([]uint16), b.([]uint16))
	case BlockUint8:
		return slicex.Union(a.([]uint8), b.([]uint8))
	case BlockFloat32:
		return slicex.UnionFloat(a.([]float32), b.([]float32))
	case BlockBool:
		x, y := slicex.ToBoolBits(a.([]bool)...), slicex.ToBoolBits(b.([]bool)...)
		return slicex.FromBoolBits(x | y)
	case BlockInt128:
		return num.UnionInt128(a.([]num.Int128), b.([]num.Int128))
	case BlockInt256:
		return num.UnionInt256(a.([]num.Int256), b.([]num.Int256))
	default:
		panic(errors.New("union: unsupported block type " + t.String()))
	}
}

func (t ValueType) Difference(a, b any) any {
	switch BlockType(t) {
	case BlockInt64:
		return slicex.Remove(a.([]int64), b.([]int64)...)
	case BlockUint64:
		return slicex.Remove(a.([]uint64), b.([]uint64)...)
	case BlockFloat64:
		return slicex.RemoveFloat(a.([]float64), b.([]float64)...)
	case BlockBytes:
		return slicex.RemoveBytes(a.([][]byte), b.([][]byte)...)
	case BlockInt32:
		return slicex.Remove(a.([]int32), b.([]int32)...)
	case BlockInt16:
		return slicex.Remove(a.([]int16), b.([]int16)...)
	case BlockInt8:
		return slicex.Remove(a.([]int8), b.([]int8)...)
	case BlockUint32:
		return slicex.Remove(a.([]uint32), b.([]uint32)...)
	case BlockUint16:
		return slicex.Remove(a.([]uint16), b.([]uint16)...)
	case BlockUint8:
		return slicex.Remove(a.([]uint8), b.([]uint8)...)
	case BlockFloat32:
		return slicex.RemoveFloat(a.([]float32), b.([]float32)...)
	case BlockBool:
		x, y := slicex.ToBoolBits(a.([]bool)...), slicex.ToBoolBits(b.([]bool)...)
		return slicex.FromBoolBits(x &^ y)
	case BlockInt128:
		return num.DifferenceInt128(a.([]num.Int128), b.([]num.Int128))
	case BlockInt256:
		return num.DifferenceInt256(a.([]num.Int256), b.([]num.Int256))
	default:
		panic(errors.New("difference: unsupported block type " + t.String()))
	}
}
