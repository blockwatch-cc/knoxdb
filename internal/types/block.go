// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package types

import (
	"bytes"
	"fmt"
	"math"

	"blockwatch.cc/knoxdb/internal/xroar"
	"blockwatch.cc/knoxdb/pkg/num"
	"blockwatch.cc/knoxdb/pkg/slicex"
	"blockwatch.cc/knoxdb/pkg/util"
)

// Note: uses 5 bit encoding (max 32 values)
type BlockType byte

const (
	BlockInvalid BlockType = iota // 0
	BlockInt64                    // 1
	BlockInt32                    // 2
	BlockInt16                    // 3
	BlockInt8                     // 4
	BlockUint64                   // 5
	BlockUint32                   // 6
	BlockUint16                   // 7
	BlockUint8                    // 8
	BlockFloat64                  // 9
	BlockFloat32                  // 10
	BlockBool                     // 11
	BlockBytes                    // 12
	BlockInt128                   // 13
	BlockInt256                   // 14
)

type BlockKind byte

const (
	BlockKindInvalid BlockKind = iota
	BlockKindInteger
	BlockKindFloat
	BlockKindBytes
	BlockKindBitmap
	BlockKindInt128
	BlockKindInt256
)

type BlockCompression byte

const (
	BlockCompressNone BlockCompression = iota
	BlockCompressSnappy
	BlockCompressLZ4
	BlockCompressZstd
)

func (i BlockCompression) Is(f BlockCompression) bool {
	return i&f > 0
}

var (
	blockTypeNames    = "__i64_i32_i16_i8_u64_u32_u16_u8_f64_f32_bool_bytes_i128_i256"
	blockTypeNamesOfs = []int{0, 2, 6, 10, 14, 17, 21, 25, 29, 32, 36, 40, 45, 51, 65, 61}

	blockTypeDataSize = [...]int{
		BlockInvalid: 0,
		BlockInt64:   8,
		BlockInt32:   4,
		BlockInt16:   2,
		BlockInt8:    1,
		BlockUint64:  8,
		BlockUint32:  4,
		BlockUint16:  2,
		BlockUint8:   1,
		BlockFloat64: 8,
		BlockFloat32: 4,
		BlockBool:    1,
		BlockBytes:   0, // fixed or variable
		BlockInt128:  16,
		BlockInt256:  32,
	}

	BlockTypes = [...]BlockType{
		FieldTypeInvalid:    BlockInvalid,
		FieldTypeDatetime:   BlockInt64,
		FieldTypeInt64:      BlockInt64,
		FieldTypeUint64:     BlockUint64,
		FieldTypeFloat64:    BlockFloat64,
		FieldTypeBoolean:    BlockBool,
		FieldTypeString:     BlockBytes,
		FieldTypeBytes:      BlockBytes,
		FieldTypeInt32:      BlockInt32,
		FieldTypeInt16:      BlockInt16,
		FieldTypeInt8:       BlockInt8,
		FieldTypeUint32:     BlockUint32,
		FieldTypeUint16:     BlockUint16,
		FieldTypeUint8:      BlockUint8,
		FieldTypeFloat32:    BlockFloat32,
		FieldTypeInt256:     BlockInt256,
		FieldTypeInt128:     BlockInt128,
		FieldTypeDecimal256: BlockInt256,
		FieldTypeDecimal128: BlockInt128,
		FieldTypeDecimal64:  BlockInt64,
		FieldTypeDecimal32:  BlockInt32,
		FieldTypeBigint:     BlockBytes,
	}
)

func (t BlockType) IsValid() bool {
	return t > 0 && t <= BlockInt256
}

func (t BlockType) String() string {
	if !t.IsValid() {
		return "invalid block type"
	}
	return blockTypeNames[blockTypeNamesOfs[t] : blockTypeNamesOfs[t+1]-1]
}

func (t BlockType) Size() int {
	if int(t) < len(blockTypeDataSize) {
		return blockTypeDataSize[t]
	}
	return 0
}

func (t BlockType) Kind() BlockKind {
	switch t {
	case BlockInt64, BlockInt32, BlockInt16, BlockInt8,
		BlockUint64, BlockUint32, BlockUint16, BlockUint8:
		return BlockKindInteger
	case BlockFloat32, BlockFloat64:
		return BlockKindFloat
	case BlockBool:
		return BlockKindBitmap
	case BlockBytes:
		return BlockKindBytes
	case BlockInt128:
		return BlockKindInt128
	case BlockInt256:
		return BlockKindInt256
	default:
		return BlockKindInvalid
	}
}

func (t BlockType) IsInt() bool {
	switch t {
	case BlockInt64, BlockInt32, BlockInt16, BlockInt8,
		BlockUint64, BlockUint32, BlockUint16, BlockUint8:
		return true
	default:
		return false
	}
}

func (t BlockType) IsFloat() bool {
	switch t {
	case BlockFloat32, BlockFloat64:
		return true
	default:
		return false
	}
}

func (t BlockType) IsSigned() bool {
	switch t {
	case BlockInt64, BlockInt32, BlockInt16, BlockInt8:
		return true
	default:
		return false
	}
}

func (t BlockType) IsUnsigned() bool {
	switch t {
	case BlockUint64, BlockUint32, BlockUint16, BlockUint8:
		return true
	default:
		return false
	}
}

func (t BlockType) Min(a, b any) any {
	if t.Cmp(a, b) < 0 {
		return a
	}
	return b
}

func (t BlockType) Max(a, b any) any {
	if t.Cmp(a, b) < 0 {
		return b
	}
	return a
}

func (t BlockType) MinNumericVal() any {
	switch t {
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
		panic(fmt.Errorf("min: unsupported block type %s", t))
	}
}

func (t BlockType) MaxNumericVal() any {
	switch t {
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
		panic(fmt.Errorf("max: unsupported block type %s", t))
	}
}

func (t BlockType) Add(a, b any) any {
	switch t {
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
		panic(fmt.Errorf("add: unsupported block type %s", t))
	}
}

func (t BlockType) Inc(v any) any {
	switch t {
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
		return math.Nextafter(v.(float64), MaxVal[float64]())
	case BlockFloat32:
		return math.Nextafter32(v.(float32), MaxVal[float32]())
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
		panic(fmt.Errorf("inc: unsupported block type %s", t))
	}
}

func (t BlockType) Dec(v any) any {
	switch t {
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
		return math.Nextafter(v.(float64), MinVal[float64]())
	case BlockFloat32:
		return math.Nextafter32(v.(float32), MinVal[float32]())
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
		panic(fmt.Errorf("dec: unsupported block type %s", t))
	}
}

func (t BlockType) Zero() any {
	switch t {
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
		panic(fmt.Errorf("zero: unsupported block type %s", t))
	}
}

// Cast casts any Go integer type into a compatible Go type for a block.
func (t BlockType) Cast(val any) (res any, ok bool) {
	switch t {
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

func (t BlockType) Cmp(a, b any) (c int) {
	switch t {
	case BlockInt64:
		c = util.Cmp(a.(int64), b.(int64))
	case BlockUint64:
		c = util.Cmp(a.(uint64), b.(uint64))
	case BlockFloat64:
		c = util.Cmp(a.(float64), b.(float64))
	case BlockBool:
		c = util.CmpBool(a.(bool), b.(bool))
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
		c = util.Cmp(a.(int32), b.(int32))
	case BlockInt16:
		c = util.Cmp(a.(int16), b.(int16))
	case BlockInt8:
		c = util.Cmp(a.(int8), b.(int8))
	case BlockUint32:
		c = util.Cmp(a.(uint32), b.(uint32))
	case BlockUint16:
		c = util.Cmp(a.(uint16), b.(uint16))
	case BlockUint8:
		c = util.Cmp(a.(uint8), b.(uint8))
	case BlockFloat32:
		c = util.Cmp(a.(float32), b.(float32))
	case BlockInt128:
		c = a.(num.Int128).Cmp(b.(num.Int128))
	case BlockInt256:
		c = a.(num.Int256).Cmp(b.(num.Int256))
	default:
		panic(fmt.Errorf("cmp: unsupported block type %s", t))
	}
	return
}

func (t BlockType) Match(mode FilterMode, a, b any) bool {
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
		panic(fmt.Errorf("match: illegal generic filter mode %s", mode))
	}
}

func (t BlockType) EQ(a, b any) bool { return t.Cmp(a, b) == 0 }
func (t BlockType) NE(a, b any) bool { return t.Cmp(a, b) != 0 }
func (t BlockType) GT(a, b any) bool { return t.Cmp(a, b) > 0 }
func (t BlockType) GE(a, b any) bool { return t.Cmp(a, b) >= 0 }
func (t BlockType) LT(a, b any) bool { return t.Cmp(a, b) < 0 }
func (t BlockType) LE(a, b any) bool { return t.Cmp(a, b) <= 0 }

// TODO
// func (t BlockType) IN(a,b any) bool {}
// func (t BlockType) NI(a,b any) bool {}
// func (t BlockType) RG(a,b any) bool {}
// func (t BlockType) RE(a,b any) bool {}

func (t BlockType) Unique(a any) any {
	switch t {
	case BlockInt64:
		return slicex.Unique(a.([]int64))
	case BlockUint64:
		return slicex.Unique(a.([]uint64))
	case BlockFloat64:
		return slicex.UniqueFloats(a.([]float64))
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
		return slicex.UniqueFloats(a.([]float32))
	case BlockBool:
		return slicex.UniqueBools(a.([]bool))
	case BlockInt128:
		return num.Int128Unique(a.([]num.Int128))
	case BlockInt256:
		return num.Int256Unique(a.([]num.Int256))
	default:
		panic(fmt.Errorf("unique: unsupported block type %s", t))
	}
}

func (t BlockType) Intersect(a, b any) any {
	switch t {
	case BlockInt64:
		x := slicex.NewOrderedIntegers(a.([]int64)).SetUnique()
		y := slicex.NewOrderedIntegers(b.([]int64)).SetUnique()
		return x.Intersect(y).Values
	case BlockUint64:
		x := slicex.NewOrderedIntegers(a.([]uint64)).SetUnique()
		y := slicex.NewOrderedIntegers(b.([]uint64)).SetUnique()
		return x.Intersect(y).Values
	case BlockFloat64:
		x := slicex.NewOrderedFloats(a.([]float64)).SetUnique()
		y := slicex.NewOrderedFloats(b.([]float64)).SetUnique()
		return x.Intersect(y).Values
	case BlockBytes:
		x := slicex.NewOrderedBytes(a.([][]byte)).SetUnique()
		y := slicex.NewOrderedBytes(b.([][]byte)).SetUnique()
		return x.Intersect(y).Values
	case BlockInt32:
		x := slicex.NewOrderedIntegers(a.([]int32)).SetUnique()
		y := slicex.NewOrderedIntegers(b.([]int32)).SetUnique()
		return x.Intersect(y).Values
	case BlockInt16:
		x := slicex.NewOrderedIntegers(a.([]int16)).SetUnique()
		y := slicex.NewOrderedIntegers(b.([]int16)).SetUnique()
		return x.Intersect(y).Values
	case BlockInt8:
		x := slicex.NewOrderedIntegers(a.([]int8)).SetUnique()
		y := slicex.NewOrderedIntegers(b.([]int8)).SetUnique()
		return x.Intersect(y).Values
	case BlockUint32:
		x := slicex.NewOrderedIntegers(a.([]uint32)).SetUnique()
		y := slicex.NewOrderedIntegers(b.([]uint32)).SetUnique()
		return x.Intersect(y).Values
	case BlockUint16:
		x := slicex.NewOrderedIntegers(a.([]uint16)).SetUnique()
		y := slicex.NewOrderedIntegers(b.([]uint16)).SetUnique()
		return x.Intersect(y).Values
	case BlockUint8:
		x := slicex.NewOrderedIntegers(a.([]uint8)).SetUnique()
		y := slicex.NewOrderedIntegers(b.([]uint8)).SetUnique()
		return x.Intersect(y).Values
	case BlockFloat32:
		x := slicex.NewOrderedFloats(a.([]float32)).SetUnique()
		y := slicex.NewOrderedFloats(b.([]float32)).SetUnique()
		return x.Intersect(y).Values
	case BlockBool:
		x, y := slicex.ToBoolBits(a.([]bool)...), slicex.ToBoolBits(b.([]bool)...)
		return slicex.FromBoolBits(x & y)
	case BlockInt128:
		return num.Int128Intersect(a.([]num.Int128), b.([]num.Int128))
	case BlockInt256:
		return num.Int256Intersect(a.([]num.Int256), b.([]num.Int256))
	default:
		panic(fmt.Errorf("intersect: unsupported block type %s", t))
	}
}

func (t BlockType) Union(a, b any) any {
	switch t {
	case BlockInt64:
		x := slicex.NewOrderedIntegers(a.([]int64)).SetUnique()
		y := slicex.NewOrderedIntegers(b.([]int64)).SetUnique()
		return x.Union(y).Values
	case BlockUint64:
		x := slicex.NewOrderedIntegers(a.([]uint64)).SetUnique()
		y := slicex.NewOrderedIntegers(b.([]uint64)).SetUnique()
		return x.Union(y).Values
	case BlockFloat64:
		x := slicex.NewOrderedFloats(a.([]float64)).SetUnique()
		y := slicex.NewOrderedFloats(b.([]float64)).SetUnique()
		return x.Union(y).Values
	case BlockBytes:
		x := slicex.NewOrderedBytes(a.([][]byte)).SetUnique()
		y := slicex.NewOrderedBytes(b.([][]byte)).SetUnique()
		return x.Union(y).Values
	case BlockInt32:
		x := slicex.NewOrderedIntegers(a.([]int32)).SetUnique()
		y := slicex.NewOrderedIntegers(b.([]int32)).SetUnique()
		return x.Union(y).Values
	case BlockInt16:
		x := slicex.NewOrderedIntegers(a.([]int16)).SetUnique()
		y := slicex.NewOrderedIntegers(b.([]int16)).SetUnique()
		return x.Union(y).Values
	case BlockInt8:
		x := slicex.NewOrderedIntegers(a.([]int8)).SetUnique()
		y := slicex.NewOrderedIntegers(b.([]int8)).SetUnique()
		return x.Union(y).Values
	case BlockUint32:
		x := slicex.NewOrderedIntegers(a.([]uint32)).SetUnique()
		y := slicex.NewOrderedIntegers(b.([]uint32)).SetUnique()
		return x.Union(y).Values
	case BlockUint16:
		x := slicex.NewOrderedIntegers(a.([]uint16)).SetUnique()
		y := slicex.NewOrderedIntegers(b.([]uint16)).SetUnique()
		return x.Union(y).Values
	case BlockUint8:
		x := slicex.NewOrderedIntegers(a.([]uint8)).SetUnique()
		y := slicex.NewOrderedIntegers(b.([]uint8)).SetUnique()
		return x.Union(y).Values
	case BlockFloat32:
		x := slicex.NewOrderedFloats(a.([]float32)).SetUnique()
		y := slicex.NewOrderedFloats(b.([]float32)).SetUnique()
		return x.Union(y).Values
	case BlockBool:
		x, y := slicex.ToBoolBits(a.([]bool)...), slicex.ToBoolBits(b.([]bool)...)
		return slicex.FromBoolBits(x | y)
	case BlockInt128:
		return num.Int128Union(a.([]num.Int128), b.([]num.Int128))
	case BlockInt256:
		return num.Int256Union(a.([]num.Int256), b.([]num.Int256))
	default:
		panic(fmt.Errorf("union: unsupported block type %s", t))
	}
}

func (t BlockType) Difference(a, b any) any {
	switch t {
	case BlockInt64:
		x := slicex.NewOrderedIntegers(a.([]int64)).SetUnique()
		y := slicex.NewOrderedIntegers(b.([]int64)).SetUnique()
		return x.Difference(y).Values
	case BlockUint64:
		x := slicex.NewOrderedIntegers(a.([]uint64)).SetUnique()
		y := slicex.NewOrderedIntegers(b.([]uint64)).SetUnique()
		return x.Difference(y).Values
	case BlockFloat64:
		x := slicex.NewOrderedFloats(a.([]float64)).SetUnique()
		y := slicex.NewOrderedFloats(b.([]float64)).SetUnique()
		return x.Difference(y).Values
	case BlockBytes:
		x := slicex.NewOrderedBytes(a.([][]byte)).SetUnique()
		y := slicex.NewOrderedBytes(b.([][]byte)).SetUnique()
		return x.Difference(y).Values
	case BlockInt32:
		x := slicex.NewOrderedIntegers(a.([]int32)).SetUnique()
		y := slicex.NewOrderedIntegers(b.([]int32)).SetUnique()
		return x.Difference(y).Values
	case BlockInt16:
		x := slicex.NewOrderedIntegers(a.([]int16)).SetUnique()
		y := slicex.NewOrderedIntegers(b.([]int16)).SetUnique()
		return x.Difference(y).Values
	case BlockInt8:
		x := slicex.NewOrderedIntegers(a.([]int8)).SetUnique()
		y := slicex.NewOrderedIntegers(b.([]int8)).SetUnique()
		return x.Difference(y).Values
	case BlockUint32:
		x := slicex.NewOrderedIntegers(a.([]uint32)).SetUnique()
		y := slicex.NewOrderedIntegers(b.([]uint32)).SetUnique()
		return x.Difference(y).Values
	case BlockUint16:
		x := slicex.NewOrderedIntegers(a.([]uint16)).SetUnique()
		y := slicex.NewOrderedIntegers(b.([]uint16)).SetUnique()
		return x.Difference(y).Values
	case BlockUint8:
		x := slicex.NewOrderedIntegers(a.([]uint8)).SetUnique()
		y := slicex.NewOrderedIntegers(b.([]uint8)).SetUnique()
		return x.Difference(y).Values
	case BlockFloat32:
		x := slicex.NewOrderedFloats(a.([]float32)).SetUnique()
		y := slicex.NewOrderedFloats(b.([]float32)).SetUnique()
		return x.Difference(y).Values
	case BlockBool:
		x, y := slicex.ToBoolBits(a.([]bool)...), slicex.ToBoolBits(b.([]bool)...)
		return slicex.FromBoolBits(x &^ y)
	case BlockInt128:
		return num.Int128Difference(a.([]num.Int128), b.([]num.Int128))
	case BlockInt256:
		return num.Int256Difference(a.([]num.Int256), b.([]num.Int256))
	default:
		panic(fmt.Errorf("difference: unsupported block type %s", t))
	}
}

// Range returns min, max of a set and whether all values between min and
// max are present, i.e. the set is complete.
func (t BlockType) Range(set any) (minv any, maxv any, isContinuous bool) {
	if bs, ok := set.(*xroar.Bitmap); ok {
		minU64 := bs.Minimum()
		maxU64 := bs.Maximum()
		isContinuous = maxU64-minU64+1 == uint64(bs.Count())
		minv, _ = t.Cast(minU64)
		maxv, _ = t.Cast(maxU64)
		return
	}
	switch t {
	case BlockInt64:
		x := slicex.NewOrderedIntegers(set.([]int64))
		minv, maxv = x.MinMax()
		isContinuous = x.IsContinuous()
	case BlockInt32:
		x := slicex.NewOrderedIntegers(set.([]int32))
		minv, maxv = x.MinMax()
		isContinuous = x.IsContinuous()
	case BlockInt16:
		x := slicex.NewOrderedIntegers(set.([]int16))
		minv, maxv = x.MinMax()
		isContinuous = x.IsContinuous()
	case BlockInt8:
		x := slicex.NewOrderedIntegers(set.([]int8))
		minv, maxv = x.MinMax()
		isContinuous = x.IsContinuous()
	case BlockUint64:
		x := slicex.NewOrderedIntegers(set.([]uint64))
		minv, maxv = x.MinMax()
		isContinuous = x.IsContinuous()
	case BlockUint32:
		x := slicex.NewOrderedIntegers(set.([]uint32))
		minv, maxv = x.MinMax()
		isContinuous = x.IsContinuous()
	case BlockUint16:
		x := slicex.NewOrderedIntegers(set.([]uint16))
		minv, maxv = x.MinMax()
		isContinuous = x.IsContinuous()
	case BlockUint8:
		x := slicex.NewOrderedIntegers(set.([]uint8))
		minv, maxv = x.MinMax()
		isContinuous = x.IsContinuous()
	case BlockInt128:
		i128s := set.([]num.Int128)
		mini, maxi := num.Int128MinMax(num.Int128Sort(i128s))
		minv, maxv = mini, maxi
		isContinuous = int(maxi.Sub(mini).Int64()+1) == len(i128s)
	case BlockInt256:
		i256s := set.([]num.Int256)
		mini, maxi := num.Int256MinMax(num.Int256Sort(i256s))
		minv, maxv = mini, maxi
		isContinuous = int(maxi.Sub(mini).Int64()+1) == len(i256s)
	case BlockFloat64:
		x := slicex.NewOrderedFloats(set.([]float64))
		minv, maxv = x.MinMax()
		isContinuous = false
	case BlockFloat32:
		x := slicex.NewOrderedFloats(set.([]float32))
		minv, maxv = x.MinMax()
		isContinuous = false
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
		x := slicex.NewOrderedBytes(set.([][]byte))
		minv, maxv = x.MinMax()
		isContinuous = false
	default:
		panic(fmt.Errorf("range: unsupported block type %s", t))
	}
	return
}

func (t BlockType) RemoveRange(s, from, to any) any {
	switch t {
	case BlockInt64:
		return slicex.NewOrderedIntegers(s.([]int64)).
			RemoveRange(from.(int64), to.(int64)).Values
	case BlockUint64:
		return slicex.NewOrderedIntegers(s.([]uint64)).
			RemoveRange(from.(uint64), to.(uint64)).Values
	case BlockFloat64:
		return slicex.NewOrderedFloats(s.([]float64)).
			RemoveRange(from.(float64), to.(float64)).Values
	case BlockBytes:
		return slicex.NewOrderedBytes(s.([][]byte)).
			RemoveRange(from.([]byte), to.([]byte)).Values
	case BlockInt32:
		return slicex.NewOrderedIntegers(s.([]int32)).
			RemoveRange(from.(int32), to.(int32)).Values
	case BlockInt16:
		return slicex.NewOrderedIntegers(s.([]int16)).
			RemoveRange(from.(int16), to.(int16)).Values
	case BlockInt8:
		return slicex.NewOrderedIntegers(s.([]int8)).
			RemoveRange(from.(int8), to.(int8)).Values
	case BlockUint32:
		return slicex.NewOrderedIntegers(s.([]uint32)).
			RemoveRange(from.(uint32), to.(uint32)).Values
	case BlockUint16:
		return slicex.NewOrderedIntegers(s.([]uint16)).
			RemoveRange(from.(uint16), to.(uint16)).Values
	case BlockUint8:
		return slicex.NewOrderedIntegers(s.([]uint8)).
			RemoveRange(from.(uint8), to.(uint8)).Values
	case BlockFloat32:
		return slicex.NewOrderedFloats(s.([]float32)).
			RemoveRange(from.(float32), to.(float32)).Values
	case BlockBool:
		x := slicex.ToBoolBits(s.([]bool)...)
		x &^= slicex.ToBoolBits(from.(bool))
		x &^= slicex.ToBoolBits(to.(bool))
		return slicex.FromBoolBits(x)
	case BlockInt128:
		return num.Int128RemoveRange(s.([]num.Int128), from.(num.Int128), to.(num.Int128))
	case BlockInt256:
		return num.Int256RemoveRange(s.([]num.Int256), from.(num.Int256), to.(num.Int256))
	default:
		panic(fmt.Errorf("remove range: unsupported block type %s", t))
	}
}

func (t BlockType) IntersectRange(s, from, to any) any {
	switch t {
	case BlockInt64:
		return slicex.NewOrderedIntegers(s.([]int64)).
			IntersectRange(from.(int64), to.(int64)).Values
	case BlockUint64:
		return slicex.NewOrderedIntegers(s.([]uint64)).
			IntersectRange(from.(uint64), to.(uint64)).Values
	case BlockFloat64:
		return slicex.NewOrderedFloats(s.([]float64)).
			IntersectRange(from.(float64), to.(float64)).Values
	case BlockBytes:
		return slicex.NewOrderedBytes(s.([][]byte)).
			IntersectRange(from.([]byte), to.([]byte)).Values
	case BlockInt32:
		return slicex.NewOrderedIntegers(s.([]int32)).
			IntersectRange(from.(int32), to.(int32)).Values
	case BlockInt16:
		return slicex.NewOrderedIntegers(s.([]int16)).
			IntersectRange(from.(int16), to.(int16)).Values
	case BlockInt8:
		return slicex.NewOrderedIntegers(s.([]int8)).
			IntersectRange(from.(int8), to.(int8)).Values
	case BlockUint32:
		return slicex.NewOrderedIntegers(s.([]uint32)).
			IntersectRange(from.(uint32), to.(uint32)).Values
	case BlockUint16:
		return slicex.NewOrderedIntegers(s.([]uint16)).
			IntersectRange(from.(uint16), to.(uint16)).Values
	case BlockUint8:
		return slicex.NewOrderedIntegers(s.([]uint8)).
			IntersectRange(from.(uint8), to.(uint8)).Values
	case BlockFloat32:
		return slicex.NewOrderedFloats(s.([]float32)).
			IntersectRange(from.(float32), to.(float32)).Values
	case BlockBool:
		x := slicex.ToBoolBits(s.([]bool)...)
		x &= slicex.ToBoolBits(from.(bool))
		x &= slicex.ToBoolBits(to.(bool))
		return slicex.FromBoolBits(x)
	case BlockInt128:
		return num.Int128IntersectRange(s.([]num.Int128), from.(num.Int128), to.(num.Int128))
	case BlockInt256:
		return num.Int256IntersectRange(s.([]num.Int256), from.(num.Int256), to.(num.Int256))
	default:
		panic(fmt.Errorf("intersect range: unsupported block type %s", t))
	}
}
