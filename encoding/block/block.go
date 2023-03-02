// Copyright (c) 2018-2022 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package block

import (
	"bytes"
	"encoding"
	"encoding/binary"
	"fmt"
	"math"
	"reflect"
	"regexp"
	"strings"
	"sync/atomic"
	"time"

	"blockwatch.cc/knoxdb/encoding/compress"
	"blockwatch.cc/knoxdb/encoding/decimal"
	"blockwatch.cc/knoxdb/encoding/dedup"
	"blockwatch.cc/knoxdb/encoding/num"
	"blockwatch.cc/knoxdb/filter/bloom"
	"blockwatch.cc/knoxdb/filter/loglogbeta"
	"blockwatch.cc/knoxdb/hash/xxhash"
	"blockwatch.cc/knoxdb/util"
	"blockwatch.cc/knoxdb/vec"
)

var bigEndian = binary.BigEndian

// FixMe: check if this works correctly
var BlockSz = int(reflect.TypeOf(blockCommon{}).Size()) + 8

var (
	//	textUnmarshalerType   = reflect.TypeOf((*encoding.TextUnmarshaler)(nil)).Elem()
	textMarshalerType = reflect.TypeOf((*encoding.TextMarshaler)(nil)).Elem()
	//	binaryUnmarshalerType = reflect.TypeOf((*encoding.BinaryUnmarshaler)(nil)).Elem()
	binaryMarshalerType = reflect.TypeOf((*encoding.BinaryMarshaler)(nil)).Elem()
	stringerType        = reflect.TypeOf((*fmt.Stringer)(nil)).Elem()

// byteSliceType         = reflect.TypeOf([]byte(nil))
)

var zeroTime = time.Time{}

type Compression byte

const (
	NoCompression Compression = iota
	SnappyCompression
	LZ4Compression
)

func (c Compression) String() string {
	switch c {
	case NoCompression:
		return "no"
	case SnappyCompression:
		return "snappy"
	case LZ4Compression:
		return "lz4"
	default:
		return "invalid compression"
	}
}

func (c Compression) HeaderSize(n int) int {
	switch c {
	case SnappyCompression:
		return 8*n>>16 + 18
	case LZ4Compression:
		return 32*n>>22 + 32
	default:
		return 0
	}
}

type Filter byte

const (
	NoFilter Filter = iota
	BloomFilter
)

func (f Filter) String() string {
	switch f {
	case NoFilter:
		return "no"
	case BloomFilter:
		return "bloom"
	default:
		return "invalid filter"
	}
}

type FieldFlags int

const (
	FlagPrimary FieldFlags = 1 << iota
	FlagIndexed
	FlagCompressSnappy
	FlagCompressLZ4
	FlagBloom

	// internal type conversion flags used when a struct field's Go type
	// does not directly match the requested field type
	flagFloatType
	flagIntType
	flagUintType
	flagStringerType
	flagBinaryMarshalerType
	flagTextMarshalerType
)

func (f FieldFlags) Contains(i FieldFlags) bool {
	return f&i > 0
}

// Note: uses 5 bit encoding (max 32 values)
type BlockType byte

const (
	BlockTypeTime       = BlockType(0)
	BlockTypeInt64      = BlockType(1)
	BlockTypeUint64     = BlockType(2)
	BlockTypeFloat64    = BlockType(3)
	BlockTypeBool       = BlockType(4)
	BlockTypeString     = BlockType(5)
	BlockTypeBytes      = BlockType(6)
	BlockTypeInt32      = BlockType(7)
	BlockTypeInt16      = BlockType(8)
	BlockTypeInt8       = BlockType(9)
	BlockTypeUint32     = BlockType(10)
	BlockTypeUint16     = BlockType(11)
	BlockTypeUint8      = BlockType(12)
	BlockTypeFloat32    = BlockType(13)
	BlockTypeInt128     = BlockType(14)
	BlockTypeInt256     = BlockType(15)
	BlockTypeDecimal32  = BlockType(16)
	BlockTypeDecimal64  = BlockType(17)
	BlockTypeDecimal128 = BlockType(18)
	BlockTypeDecimal256 = BlockType(19)
	BlockTypeInvalid    = BlockType(20)
)

func (t BlockType) IsValid() bool {
	return t < BlockTypeInvalid
}

func (t BlockType) String() string {
	switch t {
	case BlockTypeTime:
		return "time"
	case BlockTypeInt64:
		return "int64"
	case BlockTypeInt32:
		return "int32"
	case BlockTypeInt16:
		return "int16"
	case BlockTypeInt8:
		return "int8"
	case BlockTypeUint64:
		return "uint64"
	case BlockTypeUint32:
		return "uint32"
	case BlockTypeUint16:
		return "uint16"
	case BlockTypeUint8:
		return "uint8"
	case BlockTypeFloat64:
		return "float64"
	case BlockTypeFloat32:
		return "float32"
	case BlockTypeBool:
		return "bool"
	case BlockTypeString:
		return "string"
	case BlockTypeBytes:
		return "bytes"
	case BlockTypeInt128:
		return "int128"
	case BlockTypeInt256:
		return "int256"
	case BlockTypeDecimal32:
		return "decimal32"
	case BlockTypeDecimal64:
		return "decimal64"
	case BlockTypeDecimal128:
		return "decimal128"
	case BlockTypeDecimal256:
		return "decimal256"
	default:
		return "invalid block type"
	}
}

type Number interface {
	int64 | int32 | int16 | int8 | uint64 | uint32 | uint16 | uint8 | float64 | float32
}

type Block interface {
	DecRef() int64
	IncRef() int64
	HeapSize() int

	Type() BlockType
	setType(BlockType)
	IsDirty() bool
	SetDirty()
	Scale() int
	SetScale(int)
	Flags() FieldFlags
	SetFlags(FieldFlags)
	Compression() Compression
	SetCompression(Compression)

	Len() int
	Cap() int
	Release()
	Clear()
	MaxStoredSize() int
	CompressedSize() int

	Encode(*bytes.Buffer) (int, error)
	Decode([]byte, int, int) error

	ReadAtWithInfo(int, reflect.Value) error
	FieldAt(int) interface{}
	Elem(int) interface{}
	IsZeroAt(int, bool) bool
	Slice() interface{}
	RangeSlice(int, int) interface{}

	SetWithCast(int, reflect.Value) error
	SetFieldAt(int, reflect.Value) error
	Append(reflect.Value) error
	Delete(int, int)
	Grow(int)
	Copy(Block)
	AppendFrom(Block, int, int)
	ReplaceFrom(Block, int, int, int)
	InsertFrom(Block, int, int, int)
	Swap(int, int)

	Less(int, int) bool
	MatchEqual(interface{}, *vec.Bitset, *vec.Bitset) *vec.Bitset
	MatchNotEqual(interface{}, *vec.Bitset, *vec.Bitset) *vec.Bitset
	MatchGreaterThan(interface{}, *vec.Bitset, *vec.Bitset) *vec.Bitset
	MatchGreaterThanEqual(interface{}, *vec.Bitset, *vec.Bitset) *vec.Bitset
	MatchLessThan(interface{}, *vec.Bitset, *vec.Bitset) *vec.Bitset
	MatchLessThanEqual(interface{}, *vec.Bitset, *vec.Bitset) *vec.Bitset
	MatchBetween(interface{}, interface{}, *vec.Bitset, *vec.Bitset) *vec.Bitset
	MatchRegExp(string, *vec.Bitset, *vec.Bitset) *vec.Bitset
	EqualAt(int, interface{}) bool
	GtAt(int, interface{}) bool
	GteAt(int, interface{}) bool
	LtAt(int, interface{}) bool
	LteAt(int, interface{}) bool
	BetweenAt(int, interface{}, interface{}) bool
	RegExpAt(int, string) bool

	Hashes([]uint64) []uint64
	EstimateCardinality(uint) uint32
	BuildBloomFilter(int) *bloom.Filter
	MinMax() (interface{}, interface{})
	Optimize()
	Materialize()
	// FIXME: Dump should not be part of Block interface
	Dump() []byte
}

type blockCommon struct {
	refCount int64
	typ      BlockType
	comp     Compression
	dirty    bool
	size     int // stored size, debug data
	scale    int
	flags    FieldFlags
}

type BlockNum[T Number] struct {
	blockCommon
	data num.NumArray[T]
}

type BlockInt64 struct {
	BlockNum[int64]
}

type BlockInt32 struct {
	BlockNum[int32]
}

type BlockInt16 struct {
	BlockNum[int16]
}

type BlockInt8 struct {
	BlockNum[int8]
}

type BlockUint64 struct {
	BlockNum[uint64]
}

type BlockUint32 struct {
	BlockNum[uint32]
}

type BlockUint16 struct {
	BlockNum[uint16]
}

type BlockUint8 struct {
	BlockNum[uint8]
}

type BlockFloat64 struct {
	BlockNum[float64]
}

type BlockFloat32 struct {
	BlockNum[float32]
}

type BlockBytes struct {
	blockCommon
	data dedup.ByteArray
}

type BlockString struct {
	BlockBytes
}

type BlockBool struct {
	blockCommon
	data *vec.Bitset
}

type BlockInt128 struct {
	blockCommon
	data vec.Int128LLSlice
}

type BlockInt256 struct {
	blockCommon
	data vec.Int256LLSlice
}

type BlockDec32 struct {
	BlockInt32
}

type BlockDec64 struct {
	BlockInt64
}

type BlockDec128 struct {
	BlockInt128
}

type BlockDec256 struct {
	BlockInt256
}

type BlockTime struct {
	BlockInt64
}

func (b *blockCommon) IncRef() int64 {
	return atomic.AddInt64(&b.refCount, 1)
}

func (b *BlockNum[T]) DecRef() int64 {
	val := atomic.AddInt64(&b.refCount, -1)
	if val == 0 {
		b.Release()
	}
	return val
}

func (b *BlockBool) DecRef() int64 {
	val := atomic.AddInt64(&b.refCount, -1)
	if val == 0 {
		b.Release()
	}
	return val
}

func (b *BlockBytes) DecRef() int64 {
	val := atomic.AddInt64(&b.refCount, -1)
	if val == 0 {
		b.Release()
	}
	return val
}

func (b *BlockInt128) DecRef() int64 {
	val := atomic.AddInt64(&b.refCount, -1)
	if val == 0 {
		b.Release()
	}
	return val
}

func (b *BlockInt256) DecRef() int64 {
	val := atomic.AddInt64(&b.refCount, -1)
	if val == 0 {
		b.Release()
	}
	return val
}

func (b *blockCommon) Type() BlockType {
	return b.typ
}

func (b *blockCommon) setType(typ BlockType) {
	b.typ = typ
}

func (b blockCommon) IsInt() bool {
	return false
}

func (b BlockInt64) IsInt() bool {
	return true
}

func (b BlockInt32) IsInt() bool {
	return true
}

func (b BlockInt16) IsInt() bool {
	return true
}

func (b BlockInt8) IsInt() bool {
	return true
}

func (b BlockUint64) IsInt() bool {
	return true
}

func (b BlockUint32) IsInt() bool {
	return true
}

func (b BlockUint16) IsInt() bool {
	return true
}

func (b BlockUint8) IsInt() bool {
	return true
}

func (b blockCommon) IsSint() bool {
	return false
}

func (b BlockInt64) IsSint() bool {
	return true
}

func (b BlockInt32) IsSint() bool {
	return true
}

func (b BlockInt16) IsSint() bool {
	return true
}

func (b BlockInt8) IsSint() bool {
	return true
}

func (b blockCommon) IsUint() bool {
	return false
}

func (b BlockUint64) IsUint() bool {
	return true
}

func (b BlockUint32) IsUint() bool {
	return true
}

func (b BlockUint16) IsUint() bool {
	return true
}

func (b BlockUint8) IsUint() bool {
	return true
}

func (b blockCommon) IsFloat() bool {
	return false
}

func (b BlockFloat64) IsFloat() bool {
	return true
}

func (b BlockFloat32) IsFloat() bool {
	return true
}

func (b *blockCommon) Compression() Compression {
	return b.comp
}

func (b *blockCommon) CompressedSize() int {
	return b.size
}

func (b *blockCommon) IsDirty() bool {
	return b.dirty
}

func (b *blockCommon) SetDirty() {
	b.dirty = true
}

func (b *blockCommon) SetScale(scale int) {
	b.scale = scale
}

func (b *blockCommon) Scale() int {
	return b.scale
}

func (b *blockCommon) SetFlags(flags FieldFlags) {
	b.flags = flags
}

func (b *blockCommon) Flags() FieldFlags {
	return b.flags
}

func (b *blockCommon) SetCompression(c Compression) {
	b.comp = c
}

func (b *BlockNum[N]) Slice() interface{} {
	return b.data.Slice()
}

func (b *BlockTime) Slice() interface{} {
	res := make([]time.Time, b.Len())
	for i, v := range b.data.Slice() {
		if v > 0 {
			res[i] = time.Unix(0, v)
		} else {
			res[i] = zeroTime
		}
	}
	return res
}
func (b *BlockBool) Slice() interface{} {
	return b.data.Slice()
}

func (b *BlockInt128) Slice() interface{} {
	return b.data.Int128Slice()
}

func (b *BlockInt256) Slice() interface{} {
	return b.data.Int256Slice()
}

func (b *BlockBytes) Slice() interface{} {
	return b.data.Slice()
}

func (b *BlockString) Slice() interface{} {
	s := make([]string, b.data.Len())
	for i, v := range b.data.Slice() {
		s[i] = compress.UnsafeGetString(v)
	}
	return s
}

func (b *BlockDec256) Slice() interface{} {
	return decimal.Decimal256Slice{Int256: b.data.Int256Slice(), Scale: b.scale}
}

func (b *BlockDec128) Slice() interface{} {
	return decimal.Decimal128Slice{Int128: b.data.Int128Slice(), Scale: b.scale}
}

func (b *BlockDec64) Slice() interface{} {
	return decimal.Decimal64Slice{Int64: b.data.Slice(), Scale: b.scale}
}

func (b *BlockDec32) Slice() interface{} {
	return decimal.Decimal32Slice{Int32: b.data.Slice(), Scale: b.scale}
}

func (b *BlockNum[T]) RangeSlice(start, end int) interface{} {
	return b.data.RangeSlice(start, end)
}

func (b *BlockTime) RangeSlice(start, end int) interface{} {
	res := make([]time.Time, end-start+1)
	for i, v := range b.data.RangeSlice(start, end) {
		if v > 0 {
			res[i+start] = time.Unix(0, v)
		} else {
			res[i+start] = zeroTime
		}
	}
	return res
}

func (b *BlockBool) RangeSlice(start, end int) interface{} {
	return b.data.SubSlice(start, end-start+1)
}

func (b *BlockString) RangeSlice(start, end int) interface{} {
	s := make([]string, end-start+1)
	for i, v := range b.data.Subslice(start, end) {
		s[i] = compress.UnsafeGetString(v)
	}
	return s
}

func (b *BlockBytes) RangeSlice(start, end int) interface{} {
	return b.data.Subslice(start, end)
}

func (b *BlockInt128) RangeSlice(start, end int) interface{} {
	return b.data.Subslice(start, end).Int128Slice()
}

func (b *BlockInt256) RangeSlice(start, end int) interface{} {
	return b.data.Subslice(start, end).Int256Slice()
}

func (b *BlockDec256) RangeSlice(start, end int) interface{} {
	return decimal.Decimal256Slice{Int256: b.data.Subslice(start, end).Int256Slice(), Scale: b.scale}
}

func (b *BlockDec128) RangeSlice(start, end int) interface{} {
	return decimal.Decimal128Slice{Int128: b.data.Subslice(start, end).Int128Slice(), Scale: b.scale}
}

func (b *BlockDec64) RangeSlice(start, end int) interface{} {
	return decimal.Decimal64Slice{Int64: b.data.RangeSlice(start, end), Scale: b.scale}
}

func (b *BlockDec32) RangeSlice(start, end int) interface{} {
	return decimal.Decimal32Slice{Int32: b.data.RangeSlice(start, end), Scale: b.scale}
}

func (b *BlockNum[T]) Elem(idx int) interface{} {
	if idx >= b.Len() {
		return nil
	}
	return b.data.Elem(idx)
}

func (b *BlockBool) Elem(idx int) interface{} {
	if idx >= b.Len() {
		return nil
	}
	return b.data.IsSet(idx)
}

func (b *BlockBytes) Elem(idx int) interface{} {
	if idx >= b.Len() {
		return nil
	}
	return b.data.Elem(idx)
}

func (b *BlockString) Elem(idx int) interface{} {
	if idx >= b.Len() {
		return nil
	}
	return compress.UnsafeGetString(b.data.Elem(idx))
}

func (b *BlockInt128) Elem(idx int) interface{} {
	if idx >= b.Len() {
		return nil
	}
	return b.data.Elem(idx)
}

func (b *BlockInt256) Elem(idx int) interface{} {
	if idx >= b.Len() {
		return nil
	}
	return b.data.Elem(idx)
}

func (b *BlockBytes) FieldAt(i int) interface{} {
	return b.data.Elem(i)
}

func (b *BlockString) FieldAt(i int) interface{} {
	return compress.UnsafeGetString(b.data.Elem(i))
}

func (b *BlockTime) FieldAt(i int) interface{} {
	if ts := b.data.Elem(i); ts > 0 {
		return time.Unix(0, ts)
	}
	return zeroTime
}

func (b *BlockBool) FieldAt(i int) interface{} {
	return b.data.IsSet(i)
}

func (b *BlockNum[T]) FieldAt(i int) interface{} {
	return b.data.Elem(i)
}

func (b *BlockInt256) FieldAt(i int) interface{} {
	return b.data.Elem(i)
}

func (b *BlockInt128) FieldAt(i int) interface{} {
	return b.data.Elem(i)
}

func (b *BlockDec256) FieldAt(i int) interface{} {
	return decimal.NewDecimal256(b.data.Elem(i), b.scale)
}

func (b *BlockDec128) FieldAt(i int) interface{} {
	return decimal.NewDecimal128(b.data.Elem(i), b.scale)
}

func (b *BlockDec64) FieldAt(i int) interface{} {
	return decimal.NewDecimal64(b.data.Elem(i), b.scale)
}

func (b *BlockDec32) FieldAt(i int) interface{} {
	return decimal.NewDecimal32(b.data.Elem(i), b.scale)
}

func (b *BlockNum[T]) IsZeroAt(i int, zeroIsNull bool) bool {
	return zeroIsNull && b.data.Elem(i) == 0
}

func (b *BlockFloat32) IsZeroAt(i int, zeroIsNull bool) bool {
	v := float64(b.data.Elem(i))
	return math.IsNaN(v) || math.IsInf(v, 0) || (zeroIsNull && v == 0.0)
}

func (b *BlockFloat64) IsZeroAt(i int, zeroIsNull bool) bool {
	v := b.data.Elem(i)
	return math.IsNaN(v) || math.IsInf(v, 0) || (zeroIsNull && v == 0.0)
}

func (b *BlockBool) IsZeroAt(i int, zeroIsNull bool) bool {
	return zeroIsNull && !b.data.IsSet(i)
}

func (b *BlockTime) IsZeroAt(i int, zeroIsNull bool) bool {
	val := b.data.Elem(i)
	return val == 0 || (zeroIsNull && time.Unix(0, val).IsZero())
}

func (b *BlockBytes) IsZeroAt(i int, zeroIsNull bool) bool {
	return len(b.data.Elem(i)) == 0
}

func (b *BlockInt256) IsZeroAt(i int, zeroIsNull bool) bool {
	return zeroIsNull && b.data.Elem(i).IsZero()
}

func (b *BlockInt128) IsZeroAt(i int, zeroIsNull bool) bool {
	return zeroIsNull && b.data.Elem(i).IsZero()
}

func (b *BlockNum[T]) ReadAtWithInfo(i int, val reflect.Value) error {
	switch {
	case val.CanInt():
		val.SetInt(int64(b.data.Elem(i)))
	case val.CanUint():
		val.SetUint(uint64(b.data.Elem(i)))
	case val.CanFloat():
		val.SetFloat(float64(b.data.Elem(i)))
	}
	return nil
}

func (b *BlockTime) ReadAtWithInfo(i int, val reflect.Value) error {
	if ts := b.data.Elem(i); ts > 0 {
		val.Set(reflect.ValueOf(time.Unix(0, ts)))
	} else {
		val.Set(reflect.ValueOf(zeroTime))
	}
	return nil
}

func (b *BlockBool) ReadAtWithInfo(i int, val reflect.Value) error {
	if b.data.IsSet(i) {
		val.SetBool(true)
	} else {
		val.SetBool(false)
	}
	return nil
}

func (b *BlockBytes) ReadAtWithInfo(i int, val reflect.Value) error {
	if b.flags.Contains(flagBinaryMarshalerType) {
		// decode using unmarshaler, requires the unmarshaler makes a copy
		if err := val.Addr().Interface().(encoding.BinaryUnmarshaler).UnmarshalBinary(b.data.Elem(i)); err != nil {
			return err
		}
	} else {
		// copy to avoid memleaks of large blocks
		elm := b.data.Elem(i)
		buf := make([]byte, len(elm))
		copy(buf, elm)
		val.SetBytes(buf)
	}
	return nil
}

func (b *BlockString) ReadAtWithInfo(i int, val reflect.Value) error {
	if b.flags.Contains(flagTextMarshalerType) {
		if err := val.Addr().Interface().(encoding.TextUnmarshaler).UnmarshalText(b.data.Elem(i)); err != nil {
			return err
		}
	} else {
		// copy to avoid memleaks of large blocks
		// dst.SetString(compress.UnsafeGetString(b.Bytes.Elem(pos)))
		val.SetString(compress.UnsafeGetString(b.data.Elem(i)))
	}
	return nil
}

func (b *BlockInt256) ReadAtWithInfo(i int, val reflect.Value) error {
	val.Set(reflect.ValueOf(b.data.Elem(i)))
	return nil
}

func (b *BlockInt128) ReadAtWithInfo(i int, val reflect.Value) error {
	val.Set(reflect.ValueOf(b.data.Elem(i)))
	return nil
}

func (b *BlockDec256) ReadAtWithInfo(i int, val reflect.Value) error {
	switch {
	case b.flags.Contains(flagUintType):
		val.SetUint(uint64(b.data.Elem(i).Int64()))
	case b.flags.Contains(flagIntType):
		val.SetInt(b.data.Elem(i).Int64())
	case b.flags.Contains(flagFloatType):
		val.SetFloat(decimal.NewDecimal256(b.data.Elem(i), b.scale).Float64())
	default:
		v := decimal.NewDecimal256(b.data.Elem(i), b.scale)
		val.Set(reflect.ValueOf(v))
	}
	return nil
}

func (b *BlockDec128) ReadAtWithInfo(i int, val reflect.Value) error {
	switch {
	case b.flags.Contains(flagUintType):
		val.SetUint(uint64(b.data.Elem(i).Int64()))
	case b.flags.Contains(flagIntType):
		val.SetInt(b.data.Elem(i).Int64())
	case b.flags.Contains(flagFloatType):
		val.SetFloat(decimal.NewDecimal128(b.data.Elem(i), b.scale).Float64())
	default:
		v := decimal.NewDecimal128(b.data.Elem(i), b.scale)
		val.Set(reflect.ValueOf(v))
	}
	return nil
}

func (b *BlockDec64) ReadAtWithInfo(i int, val reflect.Value) error {
	switch {
	case b.flags.Contains(flagUintType):
		val.SetUint(uint64(b.data.Elem(i)))
	case b.flags.Contains(flagIntType):
		val.SetInt(b.data.Elem(i))
	case b.flags.Contains(flagFloatType):
		val.SetFloat(decimal.NewDecimal64(b.data.Elem(i), b.scale).Float64())
	default:
		v := decimal.NewDecimal64(b.data.Elem(i), b.scale)
		val.Set(reflect.ValueOf(v))
	}
	return nil
}

func (b *BlockDec32) ReadAtWithInfo(i int, val reflect.Value) error {
	switch {
	case b.flags.Contains(flagUintType):
		val.SetUint(uint64(b.data.Elem(i)))
	case b.flags.Contains(flagIntType):
		val.SetInt(int64(b.data.Elem(i)))
	case b.flags.Contains(flagFloatType):
		val.SetFloat(decimal.NewDecimal32(b.data.Elem(i), b.scale).Float64())
	default:
		v := decimal.NewDecimal32(b.data.Elem(i), b.scale)
		val.Set(reflect.ValueOf(v))
	}
	return nil
}

func (b *BlockNum[T]) SetWithCast(i int, val reflect.Value) error {
	switch {
	case val.CanInt():
		b.data.Set(i, T(val.Int()))
	case val.CanUint():
		b.data.Set(i, T(val.Uint()))
	case val.CanFloat():
		b.data.Set(i, T(val.Float()))
	}
	return nil
}

func (b *BlockNum[T]) SetFieldAt(i int, val reflect.Value) error {
	return b.SetWithCast(i, val)
}

func (b *BlockTime) SetWithCast(i int, val reflect.Value) error {
	b.data.Set(i, val.Interface().(time.Time).UnixNano())
	return nil
}

func (b *BlockTime) SetFieldAt(i int, val reflect.Value) error {
	return b.SetWithCast(i, val)
}

func (b *BlockInt128) SetWithCast(i int, val reflect.Value) error {
	b.data.Set(i, val.Interface().(vec.Int128))
	return nil
}

func (b *BlockInt128) SetFieldAt(i int, val reflect.Value) error {
	return b.SetWithCast(i, val)
}

func (b *BlockInt256) SetWithCast(i int, val reflect.Value) error {
	b.data.Set(i, val.Interface().(vec.Int256))
	return nil
}

func (b *BlockInt256) SetFieldAt(i int, val reflect.Value) error {
	return b.SetWithCast(i, val)
}

func (b *BlockBool) SetWithCast(i int, val reflect.Value) error {
	if val.Bool() {
		b.data.Set(i)
	} else {
		b.data.Clear(i)
	}
	return nil
}

func (b *BlockBool) SetFieldAt(i int, val reflect.Value) error {
	return b.SetWithCast(i, val)
}

func (b *BlockBytes) SetWithCast(i int, val reflect.Value) error {
	if b.flags.Contains(flagBinaryMarshalerType) {
		buf, err := val.Interface().(encoding.BinaryMarshaler).MarshalBinary()
		if err != nil {
			return err
		}
		b.data.Set(i, buf)
	} else {
		b.data.Set(i, val.Bytes())
	}
	return nil
}

func (b *BlockBytes) SetFieldAt(i int, val reflect.Value) error {
	// explicit check if type implements Marshaler (v != struct type)
	if val.CanInterface() && val.Type().Implements(binaryMarshalerType) {
		buf, err := val.Interface().(encoding.BinaryMarshaler).MarshalBinary()
		if err != nil {
			return err
		}
		b.data.Set(i, buf)
	} else {
		b.data.Set(i, val.Bytes())
	}
	return nil
}

func (b *BlockString) SetWithCast(i int, val reflect.Value) error {
	if b.flags.Contains(flagTextMarshalerType) {
		buf, err := val.Interface().(encoding.TextMarshaler).MarshalText()
		if err != nil {
			return err
		}
		b.data.Set(i, buf)
	} else if b.flags.Contains(flagStringerType) {
		b.data.Set(i, compress.UnsafeGetBytes(val.Interface().(fmt.Stringer).String()))
	} else {
		b.data.Set(i, compress.UnsafeGetBytes(val.String()))
	}
	return nil
}

func (b *BlockString) SetFieldAt(i int, val reflect.Value) error {
	// explicit check if type implements Marshaler (v != struct type)
	if val.CanInterface() && val.Type().Implements(textMarshalerType) {
		buf, err := val.Interface().(encoding.TextMarshaler).MarshalText()
		if err != nil {
			return err
		}
		b.data.Set(i, buf)
	} else if val.CanInterface() && val.Type().Implements(stringerType) {
		b.data.Set(i, compress.UnsafeGetBytes(val.Interface().(fmt.Stringer).String()))
	} else {
		b.data.Set(i, compress.UnsafeGetBytes(val.String()))
	}
	return nil
}

func (b *BlockDec256) SetWithCast(i int, val reflect.Value) error {
	switch {
	case b.flags.Contains(flagUintType):
		b.data.Set(i, vec.Int256{0, 0, 0, val.Uint()})
	case b.flags.Contains(flagIntType):
		b.data.Set(i, vec.Int256{0, 0, 0, uint64(val.Int())})
	case b.flags.Contains(flagFloatType):
		dec := decimal.Decimal256{}
		dec.SetFloat64(val.Float(), b.scale)
		b.data.Set(i, dec.Int256())
	default:
		b.data.Set(i, val.Interface().(decimal.Decimal256).Quantize(b.scale).Int256())
	}
	return nil
}

func (b *BlockDec256) SetFieldAt(i int, val reflect.Value) error {
	b.data.Set(i, val.Interface().(decimal.Decimal256).Quantize(b.scale).Int256())
	return nil
}

func (b *BlockDec128) SetWithCast(i int, val reflect.Value) error {
	switch {
	case b.flags.Contains(flagUintType):
		b.data.Set(i, vec.Int128{0, val.Uint()})
	case b.flags.Contains(flagIntType):
		b.data.Set(i, vec.Int128{0, uint64(val.Int())})
	case b.flags.Contains(flagFloatType):
		dec := decimal.Decimal128{}
		dec.SetFloat64(val.Float(), b.scale)
		b.data.Set(i, dec.Int128())
	default:
		b.data.Set(i, val.Interface().(decimal.Decimal128).Quantize(b.scale).Int128())
	}
	return nil
}

func (b *BlockDec128) SetFieldAt(i int, val reflect.Value) error {
	b.data.Set(i, val.Interface().(decimal.Decimal128).Quantize(b.scale).Int128())
	return nil
}

func (b *BlockDec64) SetWithCast(i int, val reflect.Value) error {
	switch {
	case b.flags.Contains(flagUintType):
		b.data.Set(i, int64(val.Uint()))
	case b.flags.Contains(flagIntType):
		b.data.Set(i, val.Int())
	case b.flags.Contains(flagFloatType):
		dec := decimal.Decimal64{}
		dec.SetFloat64(val.Float(), b.scale)
		b.data.Set(i, dec.Int64())
	default:
		b.data.Set(i, val.Interface().(decimal.Decimal64).Quantize(b.scale).Int64())
	}
	return nil
}

func (b *BlockDec64) SetFieldAt(i int, val reflect.Value) error {
	b.data.Set(i, val.Interface().(decimal.Decimal64).Quantize(b.scale).Int64())
	return nil
}

func (b *BlockDec32) SetWithCast(i int, val reflect.Value) error {
	switch {
	case b.flags.Contains(flagUintType):
		b.data.Set(i, int32(val.Uint()))
	case b.flags.Contains(flagIntType):
		b.data.Set(i, int32(val.Int()))
	case b.flags.Contains(flagFloatType):
		dec := decimal.Decimal32{}
		dec.SetFloat64(val.Float(), b.scale)
		b.data.Set(i, dec.Int32())
	default:
		b.data.Set(i, val.Interface().(decimal.Decimal32).Quantize(b.scale).Int32())
	}
	return nil
}

func (b *BlockDec32) SetFieldAt(i int, val reflect.Value) error {
	b.data.Set(i, val.Interface().(decimal.Decimal32).Quantize(b.scale).Int32())
	return nil
}

func (b *BlockNum[T]) Grow(n int) {
	b.data.Grow(n)
}

func (b *BlockInt256) Grow(n int) {
	b.data.AppendFrom(vec.MakeInt256LLSlice(n))
}

func (b *BlockInt128) Grow(n int) {
	b.data.AppendFrom(vec.MakeInt128LLSlice(n))
}

func (b *BlockBytes) Grow(n int) {
	b.data.Append(make([][]byte, n)...)
}

func (b *BlockBool) Grow(n int) {
	b.data.Grow(b.data.Len() + n)
}

func (b *BlockNum[T]) Append(val reflect.Value) error {
	switch {
	case val.CanInt():
		b.data.Append(T(val.Int()))
	case val.CanUint():
		b.data.Append(T(val.Uint()))
	case val.CanFloat():
		b.data.Append(T(val.Float()))
	}
	return nil
}

func (b *BlockTime) Append(val reflect.Value) error {
	b.data.Append(val.Interface().(time.Time).UnixNano())
	return nil
}

func (b *BlockInt128) Append(val reflect.Value) error {
	b.data.Append(val.Interface().(vec.Int128))
	return nil
}

func (b *BlockInt256) Append(val reflect.Value) error {
	b.data.Append(val.Interface().(vec.Int256))
	return nil
}

func (b *BlockBytes) Append(val reflect.Value) error {
	if b.flags.Contains(flagBinaryMarshalerType) {
		buf, err := val.Interface().(encoding.BinaryMarshaler).MarshalBinary()
		if err != nil {
			return err
		}
		b.data.Append(buf)
	} else {
		b.data.Append(val.Bytes())
	}
	return nil
}

func (b *BlockString) Append(val reflect.Value) error {
	if b.flags.Contains(flagTextMarshalerType) {
		buf, err := val.Interface().(encoding.TextMarshaler).MarshalText()
		if err != nil {
			return err
		}
		b.data.Append(buf)
	} else if b.flags.Contains(flagStringerType) {
		b.data.Append(compress.UnsafeGetBytes(val.Interface().(fmt.Stringer).String()))
	} else {
		b.data.Append(compress.UnsafeGetBytes(val.String()))
	}
	return nil
}

func (b *BlockBool) Append(val reflect.Value) error {
	l := b.data.Len()
	b.data.Grow(l + 1)
	if val.Bool() {
		b.data.Set(l)
	}
	return nil
}

func (b *BlockDec256) Append(val reflect.Value) error {
	switch {
	case b.flags.Contains(flagUintType):
		b.data.Append(vec.Int256{0, 0, 0, val.Uint()})
	case b.flags.Contains(flagIntType):
		b.data.Append(vec.Int256{0, 0, 0, uint64(val.Int())})
	case b.flags.Contains(flagFloatType):
		dec := decimal.Decimal256{}
		dec.SetFloat64(val.Float(), b.scale)
		b.data.Append(dec.Int256())
	default:
		b.data.Append(val.Interface().(decimal.Decimal256).Quantize(b.scale).Int256())
	}
	return nil
}

func (b *BlockDec128) Append(val reflect.Value) error {
	switch {
	case b.flags.Contains(flagUintType):
		b.data.Append(vec.Int128{0, val.Uint()})
	case b.flags.Contains(flagIntType):
		b.data.Append(vec.Int128{0, uint64(val.Int())})
	case b.flags.Contains(flagFloatType):
		dec := decimal.Decimal128{}
		dec.SetFloat64(val.Float(), b.scale)
		b.data.Append(dec.Int128())
	default:
		b.data.Append(val.Interface().(decimal.Decimal128).Quantize(b.scale).Int128())
	}
	return nil
}

func (b *BlockDec64) Append(val reflect.Value) error {
	switch {
	case b.flags.Contains(flagUintType):
		b.data.Append(int64(val.Uint()))
	case b.flags.Contains(flagIntType):
		b.data.Append(val.Int())
	case b.flags.Contains(flagFloatType):
		dec := decimal.Decimal64{}
		dec.SetFloat64(val.Float(), b.scale)
		b.data.Append(dec.Int64())
	default:
		b.data.Append(val.Interface().(decimal.Decimal64).Quantize(b.scale).Int64())
	}
	return nil
}

func (b *BlockDec32) Append(val reflect.Value) error {
	switch {
	case b.flags.Contains(flagUintType):
		b.data.Append(int32(val.Uint()))
	case b.flags.Contains(flagIntType):
		b.data.Append(int32(val.Int()))
	case b.flags.Contains(flagFloatType):
		dec := decimal.Decimal32{}
		dec.SetFloat64(val.Float(), b.scale)
		b.data.Append(dec.Int32())
	default:
		b.data.Append(val.Interface().(decimal.Decimal32).Quantize(b.scale).Int32())
	}
	return nil
}

func (b *BlockNum[T]) Delete(pos, n int) {
	b.data.Delete(pos, n)
}

func (b *BlockBytes) Delete(pos, n int) {
	b.data.Delete(pos, n)
}

func (b *BlockBool) Delete(pos, n int) {
	b.data.Delete(pos, n)
}

func (b *BlockInt256) Delete(pos, n int) {
	b.data = b.data.Delete(pos, n)
}

func (b *BlockInt128) Delete(pos, n int) {
	b.data = b.data.Delete(pos, n)
}

func NewBlock(typ BlockType, comp Compression, sz int, scale int, flags FieldFlags) Block {
	var bl Block
	switch typ {
	case BlockTypeTime:
		b := new(BlockTime)
		b.data = *num.NewNumArrayFromSlice(arena.Alloc(typ, sz).([]int64))
		bl = b
	case BlockTypeInt64:
		b := new(BlockInt64)
		b.data = *num.NewNumArrayFromSlice(arena.Alloc(typ, sz).([]int64))
		bl = b
	case BlockTypeFloat64:
		b := new(BlockFloat64)
		b.data = *num.NewNumArrayFromSlice(arena.Alloc(typ, sz).([]float64))
		bl = b
	case BlockTypeFloat32:
		b := new(BlockFloat32)
		b.data = *num.NewNumArrayFromSlice(arena.Alloc(typ, sz).([]float32))
		bl = b
	case BlockTypeInt32:
		b := new(BlockInt32)
		b.data = *num.NewNumArrayFromSlice(arena.Alloc(typ, sz).([]int32))
		bl = b
	case BlockTypeInt16:
		b := new(BlockInt16)
		b.data = *num.NewNumArrayFromSlice(arena.Alloc(typ, sz).([]int16))
		bl = b
	case BlockTypeInt8:
		b := new(BlockInt8)
		b.data = *num.NewNumArrayFromSlice(arena.Alloc(typ, sz).([]int8))
		bl = b
	case BlockTypeUint64:
		b := new(BlockUint64)
		b.data = *num.NewNumArrayFromSlice(arena.Alloc(typ, sz).([]uint64))
		bl = b
	case BlockTypeUint32:
		b := new(BlockUint32)
		b.data = *num.NewNumArrayFromSlice(arena.Alloc(typ, sz).([]uint32))
		bl = b
	case BlockTypeUint16:
		b := new(BlockUint16)
		b.data = *num.NewNumArrayFromSlice(arena.Alloc(typ, sz).([]uint16))
		bl = b
	case BlockTypeUint8:
		b := new(BlockUint8)
		b.data = *num.NewNumArrayFromSlice(arena.Alloc(typ, sz).([]uint8))
		bl = b
	case BlockTypeBool:
		b := new(BlockBool)
		b.data = vec.NewBitset(sz).Reset()
		bl = b
	case BlockTypeString:
		b := new(BlockString)
		b.data = dedup.NewByteArray(sz)
		bl = b
	case BlockTypeBytes:
		b := new(BlockBytes)
		b.data = dedup.NewByteArray(sz)
		bl = b
	case BlockTypeInt128:
		b := new(BlockInt128)
		b.data.X0 = arena.Alloc(BlockTypeInt64, sz).([]int64)
		b.data.X1 = arena.Alloc(BlockTypeUint64, sz).([]uint64)
		bl = b
	case BlockTypeInt256:
		b := new(BlockInt256)
		b.data.X0 = arena.Alloc(BlockTypeInt64, sz).([]int64)
		b.data.X1 = arena.Alloc(BlockTypeUint64, sz).([]uint64)
		b.data.X2 = arena.Alloc(BlockTypeUint64, sz).([]uint64)
		b.data.X3 = arena.Alloc(BlockTypeUint64, sz).([]uint64)
		bl = b
	case BlockTypeDecimal32:
		b := new(BlockDec32)
		b.data = *num.NewNumArrayFromSlice(arena.Alloc(typ, sz).([]int32))
		bl = b
	case BlockTypeDecimal64:
		b := new(BlockDec64)
		b.data = *num.NewNumArrayFromSlice(arena.Alloc(typ, sz).([]int64))
		bl = b
	case BlockTypeDecimal128:
		b := new(BlockDec128)
		b.data.X0 = arena.Alloc(BlockTypeInt64, sz).([]int64)
		b.data.X1 = arena.Alloc(BlockTypeUint64, sz).([]uint64)
		bl = b
	case BlockTypeDecimal256:
		b := new(BlockDec256)
		b.data.X0 = arena.Alloc(BlockTypeInt64, sz).([]int64)
		b.data.X1 = arena.Alloc(BlockTypeUint64, sz).([]uint64)
		b.data.X2 = arena.Alloc(BlockTypeUint64, sz).([]uint64)
		b.data.X3 = arena.Alloc(BlockTypeUint64, sz).([]uint64)
		bl = b
	}
	bl.setType(typ)
	bl.SetCompression(comp)
	bl.SetDirty()
	bl.SetScale(scale)
	bl.SetFlags(flags)
	return bl
}

func NewBlockFromSlice(typ BlockType, comp Compression, slice interface{}) Block {
	var bl Block
	switch typ {
	case BlockTypeTime:
		b := new(BlockTime)
		b.data = *num.NewNumArrayFromSlice(slice.([]int64))
		bl = b
	case BlockTypeInt64:
		b := new(BlockInt64)
		b.data = *num.NewNumArrayFromSlice(slice.([]int64))
		bl = b
	case BlockTypeFloat64:
		b := new(BlockFloat64)
		b.data = *num.NewNumArrayFromSlice(slice.([]float64))
		bl = b
	case BlockTypeFloat32:
		b := new(BlockFloat32)
		b.data = *num.NewNumArrayFromSlice(slice.([]float32))
		bl = b
	case BlockTypeInt32:
		b := new(BlockInt32)
		b.data = *num.NewNumArrayFromSlice(slice.([]int32))
		bl = b
	case BlockTypeInt16:
		b := new(BlockInt16)
		b.data = *num.NewNumArrayFromSlice(slice.([]int16))
		bl = b
	case BlockTypeInt8:
		b := new(BlockInt8)
		b.data = *num.NewNumArrayFromSlice(slice.([]int8))
		bl = b
	case BlockTypeUint64:
		b := new(BlockUint64)
		b.data = *num.NewNumArrayFromSlice(slice.([]uint64))
		bl = b
	case BlockTypeUint32:
		b := new(BlockUint32)
		b.data = *num.NewNumArrayFromSlice(slice.([]uint32))
		bl = b
	case BlockTypeUint16:
		b := new(BlockUint16)
		b.data = *num.NewNumArrayFromSlice(slice.([]uint16))
		bl = b
	case BlockTypeUint8:
		b := new(BlockUint8)
		b.data = *num.NewNumArrayFromSlice(slice.([]uint8))
		bl = b
		/*	case BlockTypeBool:
				b := new(BlockBool)
				b.data = vec.NewBitset(sz).Reset()
				bl = b
			case BlockTypeString:
				b := new(BlockString)
				b.data = dedup.NewByteArray(sz)
				bl = b
			case BlockTypeBytes:
				b := new(BlockBytes)
				b.data = dedup.NewByteArray(sz)
				bl = b*/
	case BlockTypeInt128:
		b := new(BlockInt128)
		b.data = slice.(vec.Int128LLSlice)
		bl = b
	case BlockTypeInt256:
		b := new(BlockInt256)
		b.data = slice.(vec.Int256LLSlice)
		bl = b
	default:
		errorString := fmt.Sprintf("NewBlockFromSlice not yet implemented for Type %s", typ.String())
		panic(errorString)
	}
	bl.setType(typ)
	bl.SetCompression(comp)
	bl.SetDirty()
	return bl
}

func (b *BlockTime) Copy(src Block) {
	if src == nil {
		return
	}
	sb := src.(*BlockTime)
	b.size = sb.size
	b.dirty = true
	b.scale = sb.scale
	b.flags = sb.flags
	b.data.Copy(sb.data.Slice())
}

func (b *BlockInt64) Copy(src Block) {
	if src == nil {
		return
	}
	sb := src.(*BlockInt64)
	b.size = sb.size
	b.dirty = true
	b.scale = sb.scale
	b.flags = sb.flags
	b.data.Copy(sb.data.Slice())
}

func (b *BlockInt32) Copy(src Block) {
	if src == nil {
		return
	}
	sb := src.(*BlockInt32)
	b.size = sb.size
	b.dirty = true
	b.scale = sb.scale
	b.flags = sb.flags
	b.data.Copy(sb.data.Slice())
}

func (b *BlockInt16) Copy(src Block) {
	if src == nil {
		return
	}
	sb := src.(*BlockInt16)
	b.size = sb.size
	b.dirty = true
	b.scale = sb.scale
	b.flags = sb.flags
	b.data.Copy(sb.data.Slice())
}

func (b *BlockInt8) Copy(src Block) {
	if src == nil {
		return
	}
	sb := src.(*BlockInt8)
	b.size = sb.size
	b.dirty = true
	b.scale = sb.scale
	b.flags = sb.flags
	b.data.Copy(sb.data.Slice())
}

func (b *BlockUint64) Copy(src Block) {
	if src == nil {
		return
	}
	sb := src.(*BlockUint64)
	b.size = sb.size
	b.dirty = true
	b.scale = sb.scale
	b.flags = sb.flags
	b.data.Copy(sb.data.Slice())
}

func (b *BlockUint32) Copy(src Block) {
	if src == nil {
		return
	}
	sb := src.(*BlockUint32)
	b.size = sb.size
	b.dirty = true
	b.scale = sb.scale
	b.flags = sb.flags
	b.data.Copy(sb.data.Slice())
}

func (b *BlockUint16) Copy(src Block) {
	if src == nil {
		return
	}
	sb := src.(*BlockUint16)
	b.size = sb.size
	b.dirty = true
	b.scale = sb.scale
	b.flags = sb.flags
	b.data.Copy(sb.data.Slice())
}

func (b *BlockUint8) Copy(src Block) {
	if src == nil {
		return
	}
	sb := src.(*BlockUint8)
	b.size = sb.size
	b.dirty = true
	b.scale = sb.scale
	b.flags = sb.flags
	b.data.Copy(sb.data.Slice())
}

func (b *BlockFloat64) Copy(src Block) {
	if src == nil {
		return
	}
	sb := src.(*BlockFloat64)
	b.size = sb.size
	b.dirty = true
	b.scale = sb.scale
	b.flags = sb.flags
	b.data.Copy(sb.data.Slice())
}

func (b *BlockFloat32) Copy(src Block) {
	if src == nil {
		return
	}
	sb := src.(*BlockFloat32)
	b.size = sb.size
	b.dirty = true
	b.scale = sb.scale
	b.flags = sb.flags
	b.data.Copy(sb.data.Slice())
}

func (b *BlockBool) Copy(src Block) {
	if src == nil {
		return
	}
	sb := src.(*BlockBool)
	b.size = sb.size
	b.dirty = true
	b.scale = sb.scale
	b.flags = sb.flags
	b.data = vec.NewBitsetFromBytes(sb.data.Bytes(), sb.size)
}

func (b *BlockBytes) Copy(src Block) {
	if src == nil {
		return
	}
	sb := src.(*BlockBytes)
	b.size = sb.size
	b.dirty = true
	b.scale = sb.scale
	b.flags = sb.flags
	b.data = dedup.NewByteArray(b.size)
	b.data.AppendFrom(sb.data)
}

func (b *BlockString) Copy(src Block) {
	if src == nil {
		return
	}
	sb := src.(*BlockString)
	b.size = sb.size
	b.dirty = true
	b.scale = sb.scale
	b.flags = sb.flags
	b.data = dedup.NewByteArray(b.size)
	b.data.AppendFrom(sb.data)
}

func (b *BlockInt128) Copy(src Block) {
	if src == nil {
		return
	}
	sb := src.(*BlockInt128)
	b.size = sb.size
	b.dirty = true
	b.scale = sb.scale
	b.flags = sb.flags
	sz := len(b.data.X0)
	b.data.X0 = b.data.X0[:sz]
	copy(b.data.X0, sb.data.X0)
	b.data.X1 = b.data.X1[:sz]
	copy(b.data.X1, sb.data.X1)
}

func (b *BlockInt256) Copy(src Block) {
	if src == nil || b.Type() != src.Type() {
		return
	}
	sb := src.(*BlockInt256)
	b.size = sb.size
	b.dirty = true
	b.scale = sb.scale
	b.flags = sb.flags
	sz := len(b.data.X0)
	b.data.X0 = b.data.X0[:sz]
	copy(b.data.X0, sb.data.X0)
	b.data.X1 = b.data.X1[:sz]
	copy(b.data.X1, sb.data.X1)
	b.data.X2 = b.data.X2[:sz]
	copy(b.data.X2, sb.data.X2)
	b.data.X3 = b.data.X3[:sz]
	copy(b.data.X3, sb.data.X3)
}

func (b *BlockDec32) Copy(src Block) {
	if src == nil {
		return
	}
	sb := src.(*BlockDec32)
	b.size = sb.size
	b.dirty = true
	b.scale = sb.scale
	b.flags = sb.flags
	b.data.Copy(sb.data.Slice())
}

func (b *BlockDec64) Copy(src Block) {
	if src == nil {
		return
	}
	sb := src.(*BlockDec64)
	b.size = sb.size
	b.dirty = true
	b.scale = sb.scale
	b.flags = sb.flags
	b.data.Copy(sb.data.Slice())
}

func (b *BlockDec128) Copy(src Block) {
	if src == nil {
		return
	}
	sb := src.(*BlockDec128)
	b.size = sb.size
	b.dirty = true
	b.scale = sb.scale
	b.flags = sb.flags
	sz := len(b.data.X0)
	b.data.X0 = b.data.X0[:sz]
	copy(b.data.X0, sb.data.X0)
	b.data.X1 = b.data.X1[:sz]
	copy(b.data.X1, sb.data.X1)
}

func (b *BlockDec256) Copy(src Block) {
	if src == nil || b.Type() != src.Type() {
		return
	}
	sb := src.(*BlockDec256)
	b.size = sb.size
	b.dirty = true
	b.scale = sb.scale
	b.flags = sb.flags
	sz := len(b.data.X0)
	b.data.X0 = b.data.X0[:sz]
	copy(b.data.X0, sb.data.X0)
	b.data.X1 = b.data.X1[:sz]
	copy(b.data.X1, sb.data.X1)
	b.data.X2 = b.data.X2[:sz]
	copy(b.data.X2, sb.data.X2)
	b.data.X3 = b.data.X3[:sz]
	copy(b.data.X3, sb.data.X3)
}

func (b *BlockTime) AppendFrom(src Block, pos, len int) {
	sb := src.(*BlockTime)
	b.data.AppendFrom(sb.data.Slice(), pos, len)
}

func (b *BlockInt64) AppendFrom(src Block, pos, len int) {
	sb := src.(*BlockInt64)
	b.data.AppendFrom(sb.data.Slice(), pos, len)
}

func (b *BlockInt32) AppendFrom(src Block, pos, len int) {
	sb := src.(*BlockInt32)
	b.data.AppendFrom(sb.data.Slice(), pos, len)
}

func (b *BlockInt16) AppendFrom(src Block, pos, len int) {
	sb := src.(*BlockInt16)
	b.data.AppendFrom(sb.data.Slice(), pos, len)
}

func (b *BlockInt8) AppendFrom(src Block, pos, len int) {
	sb := src.(*BlockInt8)
	b.data.AppendFrom(sb.data.Slice(), pos, len)
}

func (b *BlockUint64) AppendFrom(src Block, pos, len int) {
	sb := src.(*BlockUint64)
	b.data.AppendFrom(sb.data.Slice(), pos, len)
}

func (b *BlockUint32) AppendFrom(src Block, pos, len int) {
	sb := src.(*BlockUint32)
	b.data.AppendFrom(sb.data.Slice(), pos, len)
}

func (b *BlockUint16) AppendFrom(src Block, pos, len int) {
	sb := src.(*BlockUint16)
	b.data.AppendFrom(sb.data.Slice(), pos, len)
}

func (b *BlockUint8) AppendFrom(src Block, pos, len int) {
	sb := src.(*BlockUint8)
	b.data.AppendFrom(sb.data.Slice(), pos, len)
}

func (b *BlockFloat64) AppendFrom(src Block, pos, len int) {
	sb := src.(*BlockFloat64)
	b.data.AppendFrom(sb.data.Slice(), pos, len)
}

func (b *BlockFloat32) AppendFrom(src Block, pos, len int) {
	sb := src.(*BlockFloat32)
	b.data.AppendFrom(sb.data.Slice(), pos, len)
}

func (b *BlockInt256) AppendFrom(src Block, pos, len int) {
	sb := src.(*BlockInt256)
	b.data.AppendFrom(sb.data.Subslice(pos, pos+len))
}

func (b *BlockInt128) AppendFrom(src Block, pos, len int) {
	sb := src.(*BlockInt128)
	b.data.AppendFrom(sb.data.Subslice(pos, pos+len))
}

func (b *BlockDec32) AppendFrom(src Block, pos, len int) {
	sb := src.(*BlockDec32)

	// FIXME: are different scales possible here?
	sc, dc := sb.scale, b.scale
	if sc == dc {
		b.data.AppendFrom(sb.data.Slice(), pos, len)
	} else {
		for _, v := range sb.data.RangeSlice(pos, pos+len) {
			b.data.Append(decimal.NewDecimal32(v, sc).Quantize(dc).Int32())
		}
	}
}

func (b *BlockDec64) AppendFrom(src Block, pos, len int) {
	sb := src.(*BlockDec64)

	// FIXME: are different scales possible here?
	sc, dc := sb.scale, b.scale
	if sc == dc {
		b.data.AppendFrom(sb.data.Slice(), pos, len)
	} else {
		for _, v := range sb.data.RangeSlice(pos, pos+len) {
			b.data.Append(decimal.NewDecimal64(v, sc).Quantize(dc).Int64())
		}
	}
}

func (b *BlockDec128) AppendFrom(src Block, pos, len int) {
	sb := src.(*BlockDec128)

	// FIXME: are different scales possible here?
	sc, dc := sb.scale, b.scale
	if sc == dc {
		b.data.AppendFrom(sb.data.Subslice(pos, pos+len))
	} else {
		for j := 0; j < len; j++ {
			b.data.Append(decimal.NewDecimal128(sb.data.Elem(pos+j), sc).Quantize(dc).Int128())
		}
	}
}

func (b *BlockDec256) AppendFrom(src Block, pos, len int) {
	sb := src.(*BlockDec256)

	// FIXME: are different scales possible here?
	sc, dc := sb.scale, b.scale
	if sc == dc {
		b.data.AppendFrom(sb.data.Subslice(pos, pos+len))
	} else {
		for j := 0; j < len; j++ {
			b.data.Append(decimal.NewDecimal256(sb.data.Elem(pos+j), sc).Quantize(dc).Int256())
		}
	}
}

func (b *BlockBytes) AppendFrom(src Block, pos, len int) {
	sb := src.(*BlockBytes)
	if len == 1 {
		b.data.Append(sb.data.Elem(pos))
	} else {
		b.data.Append(sb.data.Subslice(pos, pos+len)...)
	}
}

func (b *BlockString) AppendFrom(src Block, pos, len int) {
	sb := src.(*BlockString)
	if len == 1 {
		b.data.Append(sb.data.Elem(pos))
	} else {
		b.data.Append(sb.data.Subslice(pos, pos+len)...)
	}
}

func (b *BlockBool) AppendFrom(src Block, pos, len int) {
	sb := src.(*BlockBool)
	b.data.Append(sb.data, pos, len)
}

func (b *BlockTime) ReplaceFrom(src Block, spos, dpos, len int) {
	sb := src.(*BlockTime)
	b.data.ReplaceFrom(sb.data.Slice(), spos, dpos, len)
}

func (b *BlockInt64) ReplaceFrom(src Block, spos, dpos, len int) {
	sb := src.(*BlockInt64)
	b.data.ReplaceFrom(sb.data.Slice(), spos, dpos, len)
}

func (b *BlockInt32) ReplaceFrom(src Block, spos, dpos, len int) {
	sb := src.(*BlockInt32)
	b.data.ReplaceFrom(sb.data.Slice(), spos, dpos, len)
}

func (b *BlockInt16) ReplaceFrom(src Block, spos, dpos, len int) {
	sb := src.(*BlockInt16)
	b.data.ReplaceFrom(sb.data.Slice(), spos, dpos, len)
}

func (b *BlockInt8) ReplaceFrom(src Block, spos, dpos, len int) {
	sb := src.(*BlockInt8)
	b.data.ReplaceFrom(sb.data.Slice(), spos, dpos, len)
}

func (b *BlockUint64) ReplaceFrom(src Block, spos, dpos, len int) {
	sb := src.(*BlockUint64)
	b.data.ReplaceFrom(sb.data.Slice(), spos, dpos, len)
}

func (b *BlockUint32) ReplaceFrom(src Block, spos, dpos, len int) {
	sb := src.(*BlockUint32)
	b.data.ReplaceFrom(sb.data.Slice(), spos, dpos, len)
}

func (b *BlockUint16) ReplaceFrom(src Block, spos, dpos, len int) {
	sb := src.(*BlockUint16)
	b.data.ReplaceFrom(sb.data.Slice(), spos, dpos, len)
}

func (b *BlockUint8) ReplaceFrom(src Block, spos, dpos, len int) {
	sb := src.(*BlockUint8)
	b.data.ReplaceFrom(sb.data.Slice(), spos, dpos, len)
}

func (b *BlockFloat64) ReplaceFrom(src Block, spos, dpos, len int) {
	sb := src.(*BlockFloat64)
	b.data.ReplaceFrom(sb.data.Slice(), spos, dpos, len)
}

func (b *BlockFloat32) ReplaceFrom(src Block, spos, dpos, len int) {
	sb := src.(*BlockFloat32)
	b.data.ReplaceFrom(sb.data.Slice(), spos, dpos, len)
}

func (b *BlockInt256) ReplaceFrom(src Block, spos, dpos, len int) {
	sb := src.(*BlockInt256)
	b.data.Copy(sb.data, dpos, spos, len)
}

func (b *BlockInt128) ReplaceFrom(src Block, spos, dpos, len int) {
	sb := src.(*BlockInt128)
	b.data.Copy(sb.data, dpos, spos, len)
}

func (b *BlockBytes) ReplaceFrom(src Block, spos, dpos, len int) {
	sb := src.(*BlockBytes)
	b.data.Copy(sb.data, dpos, spos, len)
}

func (b *BlockString) ReplaceFrom(src Block, spos, dpos, len int) {
	sb := src.(*BlockBytes)
	b.data.Copy(sb.data, dpos, spos, len)
}

func (b *BlockBool) ReplaceFrom(src Block, spos, dpos, len int) {
	sb := src.(*BlockBool)
	b.data.Replace(sb.data, spos, len, dpos)
}

func (b *BlockDec256) ReplaceFrom(src Block, spos, dpos, len int) {
	sb := src.(*BlockDec256)

	// FIXME: are different scales possible here?
	sc, dc := sb.scale, b.scale
	if sc == dc {
		b.data.Copy(sb.data, dpos, spos, len)
	} else {
		for j := 0; j < len; j++ {
			b.data.Set(dpos+j, decimal.NewDecimal256(sb.data.Elem(spos+j), sc).Quantize(dc).Int256())
		}
	}
}

func (b *BlockDec128) ReplaceFrom(src Block, spos, dpos, len int) {
	sb := src.(*BlockDec128)

	// FIXME: are different scales possible here?
	sc, dc := sb.scale, b.scale
	if sc == dc {
		b.data.Copy(sb.data, dpos, spos, len)
	} else {
		for j := 0; j < len; j++ {
			b.data.Set(dpos+j, decimal.NewDecimal128(sb.data.Elem(spos+j), sc).Quantize(dc).Int128())
		}
	}
}

func (b *BlockDec64) ReplaceFrom(src Block, spos, dpos, len int) {
	sb := src.(*BlockDec64)

	// FIXME: are different scales possible here?
	sc, dc := sb.scale, b.scale
	if sc == dc {
		b.data.ReplaceFrom(sb.data.Slice(), dpos, spos, len)
	} else {
		for j := 0; j < len; j++ {
			b.data.Set(dpos+j, decimal.NewDecimal64(sb.data.Elem(spos+j), sc).Quantize(dc).Int64())
		}
	}
}

func (b *BlockDec32) ReplaceFrom(src Block, spos, dpos, len int) {
	sb := src.(*BlockDec32)

	// FIXME: are different scales possible here?
	sc, dc := sb.scale, b.scale
	if sc == dc {
		b.data.ReplaceFrom(sb.data.Slice(), dpos, spos, len)
	} else {
		for j := 0; j < len; j++ {
			b.data.Set(dpos+j, decimal.NewDecimal32(sb.data.Elem(spos+j), sc).Quantize(dc).Int32())
		}
	}
}

func (b *BlockTime) InsertFrom(src Block, spos, dpos, len int) {
	sb := src.(*BlockTime)
	b.data.InsertFrom(sb.data.Slice(), spos, dpos, len)
}

func (b *BlockInt64) InsertFrom(src Block, spos, dpos, len int) {
	sb := src.(*BlockInt64)
	b.data.InsertFrom(sb.data.Slice(), spos, dpos, len)
}

func (b *BlockInt32) InsertFrom(src Block, spos, dpos, len int) {
	sb := src.(*BlockInt32)
	b.data.InsertFrom(sb.data.Slice(), spos, dpos, len)
}

func (b *BlockInt16) InsertFrom(src Block, spos, dpos, len int) {
	sb := src.(*BlockInt16)
	b.data.InsertFrom(sb.data.Slice(), spos, dpos, len)
}

func (b *BlockInt8) InsertFrom(src Block, spos, dpos, len int) {
	sb := src.(*BlockInt8)
	b.data.InsertFrom(sb.data.Slice(), spos, dpos, len)
}

func (b *BlockUint64) InsertFrom(src Block, spos, dpos, len int) {
	sb := src.(*BlockUint64)
	b.data.InsertFrom(sb.data.Slice(), spos, dpos, len)
}

func (b *BlockUint32) InsertFrom(src Block, spos, dpos, len int) {
	sb := src.(*BlockUint32)
	b.data.InsertFrom(sb.data.Slice(), spos, dpos, len)
}

func (b *BlockUint16) InsertFrom(src Block, spos, dpos, len int) {
	sb := src.(*BlockUint16)
	b.data.InsertFrom(sb.data.Slice(), spos, dpos, len)
}

func (b *BlockUint8) InsertFrom(src Block, spos, dpos, len int) {
	sb := src.(*BlockUint8)
	b.data.InsertFrom(sb.data.Slice(), spos, dpos, len)
}

func (b *BlockFloat64) InsertFrom(src Block, spos, dpos, len int) {
	sb := src.(*BlockFloat64)
	b.data.InsertFrom(sb.data.Slice(), spos, dpos, len)
}

func (b *BlockFloat32) InsertFrom(src Block, spos, dpos, len int) {
	sb := src.(*BlockFloat32)
	b.data.InsertFrom(sb.data.Slice(), spos, dpos, len)
}

func (b *BlockBytes) InsertFrom(src Block, spos, dpos, len int) {
	sb := src.(*BlockBytes)
	b.data.Insert(dpos, sb.data.Subslice(spos, spos+len)...)
}

func (b *BlockString) InsertFrom(src Block, spos, dpos, len int) {
	sb := src.(*BlockString)
	b.data.Insert(dpos, sb.data.Subslice(spos, spos+len)...)
}

func (b *BlockBool) InsertFrom(src Block, spos, dpos, len int) {
	sb := src.(*BlockBool)
	b.data.Insert(sb.data, spos, len, dpos)
}

func (b *BlockInt256) InsertFrom(src Block, spos, dpos, len int) {
	sb := src.(*BlockInt256)
	b.data.Insert(dpos, sb.data.Subslice(spos, spos+len))
}

func (b *BlockInt128) InsertFrom(src Block, spos, dpos, len int) {
	sb := src.(*BlockInt128)
	b.data.Insert(dpos, sb.data.Subslice(spos, spos+len))
}

func (b *BlockDec32) InsertFrom(src Block, spos, dpos, len int) {
	sb := src.(*BlockDec32)

	sc, dc := sb.scale, b.scale
	if sc == dc {
		b.data.InsertFrom(sb.data.Slice(), spos, dpos, len)
	} else {
		cp := make([]int32, len)
		for i, v := range sb.data.RangeSlice(spos, spos+len) {
			cp[i] = decimal.NewDecimal32(v, sc).Quantize(dc).Int32()
		}
		b.data.InsertFrom(cp, 0, dpos, len)
	}
}

func (b *BlockDec64) InsertFrom(src Block, spos, dpos, len int) {
	sb := src.(*BlockDec64)

	sc, dc := sb.scale, b.scale
	if sc == dc {
		b.data.InsertFrom(sb.data.Slice(), spos, dpos, len)
	} else {
		cp := make([]int64, len)
		for i, v := range sb.data.RangeSlice(spos, spos+len) {
			cp[i] = decimal.NewDecimal64(v, sc).Quantize(dc).Int64()
		}
		b.data.InsertFrom(cp, 0, dpos, len)
	}
}

func (b *BlockDec128) InsertFrom(src Block, spos, dpos, len int) {
	sb := src.(*BlockDec128)

	sc, dc := sb.scale, b.scale
	if sc == dc {
		b.data.Insert(dpos, sb.data.Subslice(spos, spos+len))
	} else {
		cp := vec.MakeInt128LLSlice(len)
		for i := 0; i < len; i++ {
			cp.Set(i, decimal.NewDecimal128(sb.data.Elem(spos+len), sc).Quantize(dc).Int128())
		}
		b.data.Insert(dpos, cp)
	}
}

func (b *BlockDec256) InsertFrom(src Block, spos, dpos, len int) {
	sb := src.(*BlockDec256)

	sc, dc := sb.scale, b.scale
	if sc == dc {
		b.data.Insert(dpos, sb.data.Subslice(spos, spos+len))
	} else {
		cp := vec.MakeInt256LLSlice(len)
		for i := 0; i < len; i++ {
			cp.Set(i, decimal.NewDecimal256(sb.data.Elem(spos+len), sc).Quantize(dc).Int256())
		}
		b.data.Insert(dpos, cp)
	}
}

func (b *BlockNum[T]) Len() int {
	return b.data.Len()
}

func (b *BlockBool) Len() int {
	return b.data.Len()
}

func (b *BlockBytes) Len() int {
	return b.data.Len()
}

func (b *BlockInt128) Len() int {
	return b.data.Len()
}

func (b *BlockInt256) Len() int {
	return b.data.Len()
}

func (b *BlockNum[T]) Cap() int {
	return b.data.Cap()
}

func (b *BlockBool) Cap() int {
	return b.data.Cap()
}

func (b *BlockBytes) Cap() int {
	return b.data.Cap()
}

func (b *BlockInt128) Cap() int {
	return b.data.Cap()
}

func (b *BlockInt256) Cap() int {
	return b.data.Cap()
}

// Estimate the upper bound of the space required to store a serialization
// of this block. The true size may be smaller due to efficient type-based
// compression and generic subsequent block compression.
//
// This size hint is used to properly dimension the encoer/decoder buffers
// as is required by LZ4 and to avoid memcopy during write.
func (b *BlockInt64) MaxStoredSize() int {
	sz := compress.Int64ArrayEncodedSize(b.data.Slice())
	return sz + storedBlockHeaderSize + b.comp.HeaderSize(sz)
}

func (b *BlockInt32) MaxStoredSize() int {
	sz := compress.Int32ArrayEncodedSize(b.data.Slice())
	return sz + storedBlockHeaderSize + b.comp.HeaderSize(sz)
}

func (b *BlockInt16) MaxStoredSize() int {
	sz := compress.Int16ArrayEncodedSize(b.data.Slice())
	return sz + storedBlockHeaderSize + b.comp.HeaderSize(sz)
}
func (b *BlockInt8) MaxStoredSize() int {
	sz := compress.Int8ArrayEncodedSize(b.data.Slice())
	return sz + storedBlockHeaderSize + b.comp.HeaderSize(sz)
}

func (b *BlockUint64) MaxStoredSize() int {
	sz := compress.Uint64ArrayEncodedSize(b.data.Slice())
	return sz + storedBlockHeaderSize + b.comp.HeaderSize(sz)
}

func (b *BlockUint32) MaxStoredSize() int {
	sz := compress.Uint32ArrayEncodedSize(b.data.Slice())
	return sz + storedBlockHeaderSize + b.comp.HeaderSize(sz)
}

func (b *BlockUint16) MaxStoredSize() int {
	sz := compress.Uint16ArrayEncodedSize(b.data.Slice())
	return sz + storedBlockHeaderSize + b.comp.HeaderSize(sz)
}

func (b *BlockUint8) MaxStoredSize() int {
	sz := compress.Uint8ArrayEncodedSize(b.data.Slice())
	return sz + storedBlockHeaderSize + b.comp.HeaderSize(sz)
}

func (b *BlockFloat64) MaxStoredSize() int {
	sz := compress.Float64ArrayEncodedSize(b.data.Slice())
	return sz + storedBlockHeaderSize + b.comp.HeaderSize(sz)
}

func (b *BlockFloat32) MaxStoredSize() int {
	sz := compress.Float32ArrayEncodedSize(b.data.Slice())
	return sz + storedBlockHeaderSize + b.comp.HeaderSize(sz)
}

func (b *BlockBool) MaxStoredSize() int {
	var sz int
	sz = compress.BitsetEncodedSize(b.data)
	return sz + storedBlockHeaderSize + b.comp.HeaderSize(sz)
}

func (b *BlockBytes) MaxStoredSize() int {
	var sz int
	sz = b.data.MaxEncodedSize()
	return sz + storedBlockHeaderSize + b.comp.HeaderSize(sz)
}

func (b *BlockInt128) MaxStoredSize() int {
	var sz int
	sz = compress.Int128ArrayEncodedSize(b.data)
	return sz + storedBlockHeaderSize + b.comp.HeaderSize(sz)
}

func (b *BlockInt256) MaxStoredSize() int {
	var sz int
	sz = compress.Int256ArrayEncodedSize(b.data)
	return sz + storedBlockHeaderSize + b.comp.HeaderSize(sz)
}

func (b *BlockNum[T]) HeapSize() int {
	sz := BlockSz
	sz += b.data.HeapSize()
	return sz
}

func (b *BlockBool) HeapSize() int {
	sz := BlockSz
	sz += b.data.HeapSize()
	return sz
}

func (b *BlockBytes) HeapSize() int {
	sz := BlockSz
	sz += b.data.HeapSize()
	return sz
}

func (b *BlockInt128) HeapSize() int {
	sz := BlockSz
	// FIXME: care about slice header size
	sz += b.data.Len() * 16
	return sz
}

func (b *BlockInt256) HeapSize() int {
	sz := BlockSz
	// FIXME: care about slice header size
	sz += b.data.Len() * 32
	return sz
}

func (b *BlockNum[T]) Clear() {
	b.data.Clear()
	b.dirty = true
	b.size = 0
}

func (b *BlockBytes) Clear() {
	b.data.Clear()
	if !b.data.IsMaterialized() {
		mat := b.data.Materialize()
		b.data.Release()
		b.data = mat
	}
	b.dirty = true
	b.size = 0
}

func (b *BlockBool) Clear() {
	b.data.Reset()
	b.dirty = true
	b.size = 0
}

func (b *BlockInt128) Clear() {
	b.data.X0 = b.data.X0[:0]
	b.data.X1 = b.data.X1[:0]
	b.dirty = true
	b.size = 0
}

func (b *BlockInt256) Clear() {
	b.data.X0 = b.data.X0[:0]
	b.data.X1 = b.data.X1[:0]
	b.data.X2 = b.data.X2[:0]
	b.data.X3 = b.data.X3[:0]
	b.dirty = true
	b.size = 0
}

func (b *BlockNum[T]) Release() {
	arena.Free(b.Type(), b.data.Slice()[:0])
	b.data.Release()
}

func (b *BlockBool) Release() {
	b.data.Close()
	b.data = nil
}

func (b *BlockBytes) Release() {
	b.data.Release()
	b.data = nil
}

func (b *BlockInt128) Release() {
	arena.Free(BlockTypeInt64, b.data.X0[:0])
	arena.Free(BlockTypeUint64, b.data.X1[:0])
	b.data.X0 = nil
	b.data.X1 = nil
}

func (b *BlockInt256) Release() {
	arena.Free(BlockTypeInt64, b.data.X0[:0])
	arena.Free(BlockTypeUint64, b.data.X1[:0])
	arena.Free(BlockTypeUint64, b.data.X2[:0])
	arena.Free(BlockTypeUint64, b.data.X3[:0])
	b.data.X0 = nil
	b.data.X1 = nil
	b.data.X2 = nil
	b.data.X3 = nil
}

func (b *BlockInt64) Encode(buf *bytes.Buffer) (int, error) {
	if buf == nil {
		return 0, fmt.Errorf("block: nil buffer while encoding")
	}
	n, err := encodeInt64Block(buf, b.data.Slice(), b.Compression())
	if err == nil {
		b.dirty = false
		b.size = n
	}
	return n, err
}

func (b *BlockInt32) Encode(buf *bytes.Buffer) (int, error) {
	if buf == nil {
		return 0, fmt.Errorf("block: nil buffer while encoding")
	}
	n, err := encodeInt32Block(buf, b.data.Slice(), b.Compression())
	if err == nil {
		b.dirty = false
		b.size = n
	}
	return n, err
}

func (b *BlockInt16) Encode(buf *bytes.Buffer) (int, error) {
	if buf == nil {
		return 0, fmt.Errorf("block: nil buffer while encoding")
	}
	n, err := encodeInt16Block(buf, b.data.Slice(), b.Compression())
	if err == nil {
		b.dirty = false
		b.size = n
	}
	return n, err
}

func (b *BlockInt8) Encode(buf *bytes.Buffer) (int, error) {
	if buf == nil {
		return 0, fmt.Errorf("block: nil buffer while encoding")
	}
	n, err := encodeInt8Block(buf, b.data.Slice(), b.Compression())
	if err == nil {
		b.dirty = false
		b.size = n
	}
	return n, err
}

func (b *BlockUint64) Encode(buf *bytes.Buffer) (int, error) {
	if buf == nil {
		return 0, fmt.Errorf("block: nil buffer while encoding")
	}
	n, err := encodeUint64Block(buf, b.data.Slice(), b.Compression())
	if err == nil {
		b.dirty = false
		b.size = n
	}
	return n, err
}

func (b *BlockUint32) Encode(buf *bytes.Buffer) (int, error) {
	if buf == nil {
		return 0, fmt.Errorf("block: nil buffer while encoding")
	}
	n, err := encodeUint32Block(buf, b.data.Slice(), b.Compression())
	if err == nil {
		b.dirty = false
		b.size = n
	}
	return n, err
}

func (b *BlockUint16) Encode(buf *bytes.Buffer) (int, error) {
	if buf == nil {
		return 0, fmt.Errorf("block: nil buffer while encoding")
	}
	n, err := encodeUint16Block(buf, b.data.Slice(), b.Compression())
	if err == nil {
		b.dirty = false
		b.size = n
	}
	return n, err
}

func (b *BlockUint8) Encode(buf *bytes.Buffer) (int, error) {
	if buf == nil {
		return 0, fmt.Errorf("block: nil buffer while encoding")
	}
	n, err := encodeUint8Block(buf, b.data.Slice(), b.Compression())
	if err == nil {
		b.dirty = false
		b.size = n
	}
	return n, err
}

func (b *BlockFloat64) Encode(buf *bytes.Buffer) (int, error) {
	if buf == nil {
		return 0, fmt.Errorf("block: nil buffer while encoding")
	}
	n, err := encodeFloat64Block(buf, b.data.Slice(), b.Compression())
	if err == nil {
		b.dirty = false
		b.size = n
	}
	return n, err
}

func (b *BlockFloat32) Encode(buf *bytes.Buffer) (int, error) {
	if buf == nil {
		return 0, fmt.Errorf("block: nil buffer while encoding")
	}
	n, err := encodeFloat32Block(buf, b.data.Slice(), b.Compression())
	if err == nil {
		b.dirty = false
		b.size = n
	}
	return n, err
}

func (b *BlockTime) Encode(buf *bytes.Buffer) (int, error) {
	if buf == nil {
		return 0, fmt.Errorf("block: nil buffer while encoding")
	}

	n, err := encodeTimeBlock(buf, b.data.Slice(), b.Compression())
	if err != nil {
		return n, err
	}

	b.dirty = false
	b.size = n
	return n, nil
}

func (b *BlockBytes) Encode(buf *bytes.Buffer) (int, error) {
	if buf == nil {
		return 0, fmt.Errorf("block: nil buffer while encoding")
	}

	n, err := encodeBytesBlock(buf, b.data, b.Compression())
	if err != nil {
		return n, err
	}

	b.dirty = false
	b.size = n
	return n, nil
}

func (b *BlockString) Encode(buf *bytes.Buffer) (int, error) {
	if buf == nil {
		return 0, fmt.Errorf("block: nil buffer while encoding")
	}

	n, err := encodeStringBlock(buf, b.data, b.Compression())
	if err != nil {
		return n, err
	}

	b.dirty = false
	b.size = n
	return n, nil
}

func (b *BlockBool) Encode(buf *bytes.Buffer) (int, error) {
	if buf == nil {
		return 0, fmt.Errorf("block: nil buffer while encoding")
	}

	n, err := encodeBoolBlock(buf, b.data, b.Compression())
	if err != nil {
		return n, err
	}

	b.dirty = false
	b.size = n
	return n, nil
}

func (b *BlockInt128) Encode(buf *bytes.Buffer) (int, error) {
	if buf == nil {
		return 0, fmt.Errorf("block: nil buffer while encoding")
	}

	n, err := encodeInt128Block(buf, b.data, b.Compression())
	if err != nil {
		return n, err
	}

	b.dirty = false
	b.size = n
	return n, nil
}

func (b *BlockInt256) Encode(buf *bytes.Buffer) (int, error) {
	if buf == nil {
		return 0, fmt.Errorf("block: nil buffer while encoding")
	}

	n, err := encodeInt256Block(buf, b.data, b.Compression())
	if err != nil {
		return n, err
	}

	b.dirty = false
	b.size = n
	return n, nil
}

func (b *BlockTime) Decode(buf []byte, sz, stored int) error {
	typ, err := readBlockType(buf)
	if err != nil {
		return err
	}
	if typ != b.Type() {
		return fmt.Errorf("Decode: unexpected block type %d(%s), expected %d(%s)",
			typ, typ.String(), b.Type(), b.Type().String())
	}
	b.dirty = false
	b.size = stored

	if b.data.Slice() == nil || cap(b.data.Slice()) < sz {
		arena.Free(typ, b.data.Slice())
		b.data = *num.NewNumArrayFromSlice(arena.Alloc(typ, sz).([]int64))
	}
	//var tmp []int64
	tmp, err := decodeTimeBlock(buf, b.data.Slice()[:0])
	b.data.SetSlice(tmp)

	// FIXME: add a chec here if slice was reallocated
	return err
}

func (b *BlockString) Decode(buf []byte, sz, stored int) error {
	typ, err := readBlockType(buf)
	if err != nil {
		return err
	}
	if typ != b.Type() {
		return fmt.Errorf("Decode: unexpected block type %d(%s), expected %d(%s)",
			typ, typ.String(), b.Type(), b.Type().String())
	}
	b.dirty = false
	b.size = stored

	b.data, err = decodeStringBlock(buf, b.data, sz)
	return err
}

func (b *BlockBytes) Decode(buf []byte, sz, stored int) error {
	typ, err := readBlockType(buf)
	if err != nil {
		return err
	}
	if typ != b.Type() {
		return fmt.Errorf("Decode: unexpected block type %d(%s), expected %d(%s)",
			typ, typ.String(), b.Type(), b.Type().String())
	}
	b.dirty = false
	b.size = stored

	b.data, err = decodeBytesBlock(buf, b.data, sz)
	return err
}

func (b *BlockBool) Decode(buf []byte, sz, stored int) error {
	typ, err := readBlockType(buf)
	if err != nil {
		return err
	}
	if typ != b.Type() {
		return fmt.Errorf("Decode: unexpected block type %d(%s), expected %d(%s)",
			typ, typ.String(), b.Type(), b.Type().String())
	}
	b.dirty = false
	b.size = stored

	if b.data == nil || b.data.Cap() < sz {
		b.data = vec.NewBitset(sz)
		b.data.Reset()
	} else {
		b.data.Grow(sz).Reset()
	}
	b.data, err = decodeBoolBlock(buf, b.data)

	return err
}

func (b *BlockInt128) Decode(buf []byte, sz, stored int) error {
	typ, err := readBlockType(buf)
	if err != nil {
		return err
	}
	if typ != b.Type() {
		//return fmt.Errorf("Decode: unexpected block type %d(%s), expected %d(%s)",
		//	typ, typ.String(), b.Type(), b.Type().String())
	}
	b.dirty = false
	b.size = stored

	if b.data.X0 == nil || cap(b.data.X0) < sz {
		// FIXME: should we not free here int64 slice
		arena.Free(typ, b.data.X0)
		b.data.X0 = arena.Alloc(BlockTypeInt64, sz).([]int64)
	}
	if b.data.X1 == nil || cap(b.data.X1) < sz {
		// FIXME: should we not free here uint64 slice
		arena.Free(typ, b.data.X1)
		b.data.X1 = arena.Alloc(BlockTypeUint64, sz).([]uint64)
	}
	b.data.X0 = b.data.X0[:0]
	b.data.X1 = b.data.X1[:0]
	b.data, err = decodeInt128Block(buf, b.data)

	return err
}

func (b *BlockInt256) Decode(buf []byte, sz, stored int) error {
	typ, err := readBlockType(buf)
	if err != nil {
		return err
	}
	if typ != b.Type() {
		//return fmt.Errorf("Decode: unexpected block type %d(%s), expected %d(%s)",
		//	typ, typ.String(), b.Type(), b.Type().String())
	}
	b.dirty = false
	b.size = stored

	if b.data.X0 == nil || cap(b.data.X0) < sz {
		// FIXME: should we not free here uint64 slice
		arena.Free(typ, b.data.X0)
		b.data.X0 = arena.Alloc(BlockTypeInt64, sz).([]int64)
	}
	if b.data.X1 == nil || cap(b.data.X1) < sz {
		// FIXME: should we not free here uint64 slice
		arena.Free(typ, b.data.X1)
		b.data.X1 = arena.Alloc(BlockTypeUint64, sz).([]uint64)
	}
	if b.data.X2 == nil || cap(b.data.X2) < sz {
		// FIXME: should we not free here uint64 slice
		arena.Free(typ, b.data.X2)
		b.data.X2 = arena.Alloc(BlockTypeUint64, sz).([]uint64)
	}
	if b.data.X3 == nil || cap(b.data.X3) < sz {
		// FIXME: should we not free here uint64 slice
		arena.Free(typ, b.data.X3)
		b.data.X3 = arena.Alloc(BlockTypeUint64, sz).([]uint64)
	}
	b.data.X0 = b.data.X0[:0]
	b.data.X1 = b.data.X1[:0]
	b.data.X2 = b.data.X2[:0]
	b.data.X3 = b.data.X3[:0]
	b.data, err = decodeInt256Block(buf, b.data)

	return err
}

func (b *BlockNum[T]) Decode(buf []byte, sz, stored int) error {
	typ, err := readBlockType(buf)
	if err != nil {
		return err
	}
	if typ != b.Type() {
		//return fmt.Errorf("Decode: unexpected block type %d(%s), expected %d(%s)",
		//	typ, typ.String(), b.Type(), b.Type().String())
	}
	b.dirty = false
	b.size = stored

	var tmp interface{}

	switch typ {
	case BlockTypeFloat64:
		if b.data.Slice() == nil || cap(b.data.Slice()) < sz {
			arena.Free(typ, b.data.Slice())
			b.data = *num.NewNumArrayFromSlice(arena.Alloc(typ, sz).([]T))
		}
		tmp, err = decodeFloat64Block(buf, interface{}(b.data.Slice()).([]float64)[:0])
		b.data.SetSlice(tmp.([]T))

	case BlockTypeFloat32:
		if b.data.Slice() == nil || cap(b.data.Slice()) < sz {
			arena.Free(typ, b.data.Slice())
			b.data = *num.NewNumArrayFromSlice(arena.Alloc(typ, sz).([]T))
		}
		tmp, err = decodeFloat32Block(buf, interface{}(b.data.Slice()).([]float32)[:0])
		b.data.SetSlice(tmp.([]T))

	case BlockTypeInt64:
		if b.data.Slice() == nil || cap(b.data.Slice()) < sz {
			arena.Free(typ, b.data.Slice())
			b.data = *num.NewNumArrayFromSlice(arena.Alloc(typ, sz).([]T))
		}
		tmp, err = decodeInt64Block(buf, interface{}(b.data.Slice()).([]int64)[:0])
		b.data.SetSlice(tmp.([]T))

	case BlockTypeInt32:
		if b.data.Slice() == nil || cap(b.data.Slice()) < sz {
			arena.Free(typ, b.data.Slice())
			b.data = *num.NewNumArrayFromSlice(arena.Alloc(typ, sz).([]T))
		}
		tmp, err = decodeInt32Block(buf, interface{}(b.data.Slice()).([]int32)[:0])
		b.data.SetSlice(tmp.([]T))

	case BlockTypeInt16:
		if b.data.Slice() == nil || cap(b.data.Slice()) < sz {
			arena.Free(typ, b.data.Slice())
			b.data = *num.NewNumArrayFromSlice(arena.Alloc(typ, sz).([]T))
		}
		tmp, err = decodeInt16Block(buf, interface{}(b.data.Slice()).([]int16)[:0])
		b.data.SetSlice(tmp.([]T))

	case BlockTypeInt8:
		if b.data.Slice() == nil || cap(b.data.Slice()) < sz {
			arena.Free(typ, b.data.Slice())
			b.data = *num.NewNumArrayFromSlice(arena.Alloc(typ, sz).([]T))
		}
		tmp, err = decodeInt8Block(buf, interface{}(b.data.Slice()).([]int8)[:0])
		b.data.SetSlice(tmp.([]T))

	case BlockTypeUint64:
		if b.data.Slice() == nil || cap(b.data.Slice()) < sz {
			arena.Free(typ, b.data.Slice())
			b.data = *num.NewNumArrayFromSlice(arena.Alloc(typ, sz).([]T))
		}
		tmp, err = decodeUint64Block(buf, interface{}(b.data.Slice()).([]uint64)[:0])
		b.data.SetSlice(tmp.([]T))

	case BlockTypeUint32:
		if b.data.Slice() == nil || cap(b.data.Slice()) < sz {
			arena.Free(typ, b.data.Slice())
			b.data = *num.NewNumArrayFromSlice(arena.Alloc(typ, sz).([]T))
		}
		tmp, err = decodeUint32Block(buf, interface{}(b.data.Slice()).([]uint32)[:0])
		b.data.SetSlice(tmp.([]T))

	case BlockTypeUint16:
		if b.data.Slice() == nil || cap(b.data.Slice()) < sz {
			arena.Free(typ, b.data.Slice())
			b.data = *num.NewNumArrayFromSlice(arena.Alloc(typ, sz).([]T))
		}
		tmp, err = decodeUint16Block(buf, interface{}(b.data.Slice()).([]uint16)[:0])
		b.data.SetSlice(tmp.([]T))

	case BlockTypeUint8:
		if b.data.Slice() == nil || cap(b.data.Slice()) < sz {
			arena.Free(typ, b.data.Slice())
			b.data = *num.NewNumArrayFromSlice(arena.Alloc(typ, sz).([]T))
		}
		tmp, err = decodeUint8Block(buf, interface{}(b.data.Slice()).([]uint8)[:0])
		b.data.SetSlice(tmp.([]T))

	default:
		err = fmt.Errorf("block: invalid data type %s (%[1]d)", typ)
	}
	return err
}

func (b *BlockTime) MinMax() (interface{}, interface{}) {
	min, max := vec.Int64.MinMax(b.data.Slice())
	return time.Unix(0, min).UTC(), time.Unix(0, max).UTC()
}

func (b *BlockNum[T]) MinMax() (interface{}, interface{}) {
	return b.data.MinMax()
}

func (b *BlockBool) MinMax() (interface{}, interface{}) {
	if b.data.Len() > 0 && b.data.Count() > 0 {
		return true, false
	}
	return false, false
}

func (b *BlockString) MinMax() (interface{}, interface{}) {
	min, max := b.data.MinMax()
	return compress.UnsafeGetString(min), compress.UnsafeGetString(max)
}

func (b *BlockBytes) MinMax() (interface{}, interface{}) {
	return b.data.MinMax()
}

func (b *BlockInt128) MinMax() (interface{}, interface{}) {
	return b.data.MinMax()
}

func (b *BlockInt256) MinMax() (interface{}, interface{}) {
	return b.data.MinMax()
}

func (b *BlockNum[T]) Less(i, j int) bool {
	return b.data.Less(i, j)
}

func (b *BlockInt256) Less(i, j int) bool {
	return b.data.Elem(i).Lt(b.data.Elem(j))
}

func (b *BlockInt128) Less(i, j int) bool {
	return b.data.Elem(i).Lt(b.data.Elem(j))
}

func (b *BlockBool) Less(i, j int) bool {
	return !b.data.IsSet(i) && b.data.IsSet(j)
}

func (b *BlockBytes) Less(i, j int) bool {
	return bytes.Compare(b.data.Elem(i), b.data.Elem(j)) < 0
}

func (b *BlockNum[T]) Swap(i, j int) {
	b.data.Swap(i, j)
}

func (b *BlockBytes) Swap(i, j int) {
	b.data.Swap(i, j)
}

func (b *BlockBool) Swap(i, j int) {
	b.data.Swap(i, j)
}

func (b *BlockInt128) Swap(i, j int) {
	b.data.Swap(i, j)
}

func (b *BlockInt256) Swap(i, j int) {
	b.data.Swap(i, j)
}

func (b *BlockNum[T]) Hashes(res []uint64) []uint64 {
	sz := b.Len()
	if res == nil || cap(res) < sz {
		res = arena.Alloc(BlockTypeUint64, sz).([]uint64)
	}
	res = res[:sz]
	b.data.Hashes(res)
	return res
}

func (b *BlockBool) Hashes(res []uint64) []uint64 {
	sz := b.Len()
	if res == nil || cap(res) < sz {
		res = arena.Alloc(BlockTypeUint64, sz).([]uint64)
	}
	res = res[:sz]
	zero, one := xxhash.Sum64([]byte{0}), xxhash.Sum64([]byte{1})
	for i := 0; i < b.data.Len(); i++ {
		if b.data.IsSet(i) {
			res[i] = one
		} else {
			res[i] = zero
		}
	}
	return res
}

func (b *BlockBytes) Hashes(res []uint64) []uint64 {
	sz := b.Len()
	if res == nil || cap(res) < sz {
		res = arena.Alloc(BlockTypeUint64, sz).([]uint64)
	}
	res = res[:sz]
	for i := 0; i < b.data.Len(); i++ {
		res[i] = xxhash.Sum64(b.data.Elem(i))
	}
	return res
}

func (b *BlockInt128) Hashes(res []uint64) []uint64 {
	sz := b.Len()
	if res == nil || cap(res) < sz {
		res = arena.Alloc(BlockTypeUint64, sz).([]uint64)
	}
	res = res[:sz]
	for i := 0; i < b.data.Len(); i++ {
		buf := b.data.Elem(i).Bytes16()
		res[i] = xxhash.Sum64(buf[:])
	}
	return res
}

func (b *BlockInt256) Hashes(res []uint64) []uint64 {
	sz := b.Len()
	if res == nil || cap(res) < sz {
		res = arena.Alloc(BlockTypeUint64, sz).([]uint64)
	}
	res = res[:sz]
	for i := 0; i < b.data.Len(); i++ {
		buf := b.data.Elem(i).Bytes32()
		res[i] = xxhash.Sum64(buf[:])
	}
	return res
}

func (b *BlockBytes) EstimateCardinality(prec uint) uint32 {
	filter := loglogbeta.NewFilterWithPrecision(uint32(prec))
	l := b.data.Len()
	for i := 0; i < l; i++ {
		filter.Add(b.data.Elem(i))
	}
	return util.MinU32(uint32(l), uint32(filter.Cardinality()))
}

func (b *BlockBool) EstimateCardinality(prec uint) uint32 {
	// FIXME: make more efficient: no loglogbeta, process bytes
	filter := loglogbeta.NewFilterWithPrecision(uint32(prec))
	var (
		count int
		last  bool
	)
	for _, v := range b.data.Slice() {
		if count == 2 {
			break
		}
		if v {
			filter.Add([]byte{1})
			if count == 0 || !last {
				count++
			}
		} else {
			filter.Add([]byte{0})
			if count == 0 || last {
				count++
			}
		}
	}
	return util.MinU32(uint32(b.data.Len()), uint32(filter.Cardinality()))
}

func (b *BlockInt256) EstimateCardinality(prec uint) uint32 {
	filter := loglogbeta.NewFilterWithPrecision(uint32(prec))
	l := b.data.Len()
	for i := 0; i < l; i++ {
		buf := b.data.Elem(i).Bytes32()
		filter.Add(buf[:])
	}
	return util.MinU32(uint32(l), uint32(filter.Cardinality()))
}

func (b *BlockInt128) EstimateCardinality(prec uint) uint32 {
	filter := loglogbeta.NewFilterWithPrecision(uint32(prec))
	l := b.data.Len()
	for i := 0; i < l; i++ {
		buf := b.data.Elem(i).Bytes16()
		filter.Add(buf[:])
	}
	return util.MinU32(uint32(l), uint32(filter.Cardinality()))
}

func (b *BlockInt64) EstimateCardinality(prec uint) uint32 {
	filter := loglogbeta.NewFilterWithPrecision(uint32(prec))
	filter.AddManyInt64(b.data.Slice())
	return util.MinU32(uint32(b.data.Len()), uint32(filter.Cardinality()))
}

func (b *BlockInt32) EstimateCardinality(prec uint) uint32 {
	filter := loglogbeta.NewFilterWithPrecision(uint32(prec))
	filter.AddManyInt32(b.data.Slice())
	return util.MinU32(uint32(b.data.Len()), uint32(filter.Cardinality()))
}

func (b *BlockInt16) EstimateCardinality(prec uint) uint32 {
	filter := loglogbeta.NewFilterWithPrecision(uint32(prec))
	var buf [2]byte
	for _, v := range b.data.Slice() {
		bigEndian.PutUint16(buf[:], uint16(v))
		filter.Add(buf[:2])
	}
	return util.MinU32(uint32(b.data.Len()), uint32(filter.Cardinality()))
}

func (b *BlockInt8) EstimateCardinality(prec uint) uint32 {
	filter := loglogbeta.NewFilterWithPrecision(uint32(prec))
	for _, v := range b.data.Slice() {
		filter.Add([]byte{byte(v)})
	}
	return util.MinU32(uint32(b.data.Len()), uint32(filter.Cardinality()))
}

func (b *BlockUint64) EstimateCardinality(prec uint) uint32 {
	filter := loglogbeta.NewFilterWithPrecision(uint32(prec))
	filter.AddManyUint64(b.data.Slice())
	return util.MinU32(uint32(b.data.Len()), uint32(filter.Cardinality()))
}

func (b *BlockUint32) EstimateCardinality(prec uint) uint32 {
	filter := loglogbeta.NewFilterWithPrecision(uint32(prec))
	filter.AddManyUint32(b.data.Slice())
	return util.MinU32(uint32(b.data.Len()), uint32(filter.Cardinality()))
}

func (b *BlockUint16) EstimateCardinality(prec uint) uint32 {
	filter := loglogbeta.NewFilterWithPrecision(uint32(prec))
	var buf [2]byte
	for _, v := range b.data.Slice() {
		bigEndian.PutUint16(buf[:], v)
		filter.Add(buf[:2])
	}
	return util.MinU32(uint32(b.data.Len()), uint32(filter.Cardinality()))
}

func (b *BlockUint8) EstimateCardinality(prec uint) uint32 {
	filter := loglogbeta.NewFilterWithPrecision(uint32(prec))
	for _, v := range b.data.Slice() {
		filter.Add([]byte{v})
	}
	return util.MinU32(uint32(b.data.Len()), uint32(filter.Cardinality()))
}

func (b *BlockFloat64) EstimateCardinality(prec uint) uint32 {
	filter := loglogbeta.NewFilterWithPrecision(uint32(prec))
	var buf [8]byte
	for _, v := range b.data.Slice() {
		bigEndian.PutUint64(buf[:], math.Float64bits(v))
		filter.Add(buf[:])
	}
	return util.MinU32(uint32(b.data.Len()), uint32(filter.Cardinality()))
}

func (b *BlockFloat32) EstimateCardinality(prec uint) uint32 {
	filter := loglogbeta.NewFilterWithPrecision(uint32(prec))
	var buf [4]byte
	for _, v := range b.data.Slice() {
		bigEndian.PutUint32(buf[:], math.Float32bits(v))
		filter.Add(buf[:4])
	}
	return util.MinU32(uint32(b.data.Len()), uint32(filter.Cardinality()))
}

func (b *BlockBytes) BuildBloomFilter(m int) *bloom.Filter {
	flt := bloom.NewFilter(m)
	for i := 0; i < b.data.Len(); i++ {
		flt.Add(b.data.Elem(i))
	}
	return flt
}

func (b *BlockBool) BuildBloomFilter(m int) *bloom.Filter {
	// FIXME: make more efficiant
	flt := bloom.NewFilter(m)
	var (
		count int
		last  bool
	)
	for _, v := range b.data.Slice() {
		if count == 2 {
			break
		}
		if v {
			flt.Add([]byte{1})
			if count == 0 || !last {
				count++
			}
		} else {
			flt.Add([]byte{0})
			if count == 0 || last {
				count++
			}
		}
	}
	return flt
}

func (b *BlockInt256) BuildBloomFilter(m int) *bloom.Filter {
	flt := bloom.NewFilter(m)
	for i := 0; i < b.data.Len(); i++ {
		buf := b.data.Elem(i).Bytes32()
		flt.Add(buf[:])
	}
	return flt
}

func (b *BlockInt128) BuildBloomFilter(m int) *bloom.Filter {
	flt := bloom.NewFilter(m)
	for i := 0; i < b.data.Len(); i++ {
		buf := b.data.Elem(i).Bytes16()
		flt.Add(buf[:])
	}
	return flt
}

func (b *BlockInt64) BuildBloomFilter(m int) *bloom.Filter {
	flt := bloom.NewFilter(m)
	flt.AddManyInt64(b.data.Slice())
	return flt
}

func (b *BlockInt32) BuildBloomFilter(m int) *bloom.Filter {
	flt := bloom.NewFilter(m)
	flt.AddManyInt32(b.data.Slice())
	return flt
}

func (b *BlockInt16) BuildBloomFilter(m int) *bloom.Filter {
	flt := bloom.NewFilter(m)
	flt.AddManyInt16(b.data.Slice())
	return flt
}

func (b *BlockInt8) BuildBloomFilter(m int) *bloom.Filter {
	flt := bloom.NewFilter(m)
	for _, v := range b.data.Slice() {
		flt.Add([]byte{byte(v)})
	}
	return flt
}

func (b *BlockUint64) BuildBloomFilter(m int) *bloom.Filter {
	flt := bloom.NewFilter(m)
	flt.AddManyUint64(b.data.Slice())
	return flt
}

func (b *BlockUint32) BuildBloomFilter(m int) *bloom.Filter {
	flt := bloom.NewFilter(m)
	flt.AddManyUint32(b.data.Slice())
	return flt
}

func (b *BlockUint16) BuildBloomFilter(m int) *bloom.Filter {
	flt := bloom.NewFilter(m)
	flt.AddManyUint16(b.data.Slice())
	return flt
}

func (b *BlockUint8) BuildBloomFilter(m int) *bloom.Filter {
	flt := bloom.NewFilter(m)
	for _, v := range b.data.Slice() {
		flt.Add([]byte{v})
	}
	return flt
}

func (b *BlockFloat64) BuildBloomFilter(m int) *bloom.Filter {
	flt := bloom.NewFilter(m)
	flt.AddManyFloat64(b.data.Slice())
	return flt
}

func (b *BlockFloat32) BuildBloomFilter(m int) *bloom.Filter {
	flt := bloom.NewFilter(m)
	flt.AddManyFloat32(b.data.Slice())
	return flt
}

func (b *blockCommon) Optimize() {}

func (b *BlockBytes) Optimize() {
	if b.data.IsOptimized() {
		return
	}
	// log.Infof("Pack %d: optimize %T rows=%d len=%d cap=%d", p.key, b.Bytes, p.nValues, b.Bytes.Len(), b.Bytes.Cap())
	opt := b.data.Optimize()
	b.data.Release()
	b.data = opt
	// log.Infof("Pack %d: optimized to %T len=%d cap=%d", p.key, b.Bytes, b.Bytes.Len(), b.Bytes.Cap())
}

func (b *blockCommon) Materialize() {}

func (b *BlockBytes) Materialize() {
	if b.data.IsMaterialized() {
		return
	}
	// log.Infof("Pack %d: materialize %T rows=%d len=%d cap=%d", p.key, b.Bytes, p.nValues, b.Bytes.Len(), b.Bytes.Cap())
	mat := b.data.Materialize()
	b.data.Release()
	b.data = mat
	// log.Infof("Pack %d: materialized to %T len=%d cap=%d", p.key, b.Bytes, b.Bytes.Len(), b.Bytes.Cap())
}

func (b *BlockBytes) MatchEqual(val interface{}, bits, mask *vec.Bitset) *vec.Bitset {
	return b.data.MatchEqual(val.([]byte), bits, mask)
}

func (b *BlockString) MatchEqual(val interface{}, bits, mask *vec.Bitset) *vec.Bitset {
	return b.data.MatchEqual([]byte(val.(string)), bits, mask)
}

func (b *BlockTime) MatchEqual(val interface{}, bits, mask *vec.Bitset) *vec.Bitset {
	return vec.MatchInt64Equal(b.data.Slice(), val.(time.Time).UnixNano(), bits, mask)
}

func (b *BlockBool) MatchEqual(val interface{}, bits, mask *vec.Bitset) *vec.Bitset {
	if val.(bool) {
		return bits.Copy(b.data)
	} else {
		return bits.Copy(b.data).Neg()
	}
}

func (b *BlockInt256) MatchEqual(val interface{}, bits, mask *vec.Bitset) *vec.Bitset {
	return vec.MatchInt256Equal(b.data, val.(vec.Int256), bits, mask)
}

func (b *BlockInt128) MatchEqual(val interface{}, bits, mask *vec.Bitset) *vec.Bitset {
	return vec.MatchInt128Equal(b.data, val.(vec.Int128), bits, mask)
}

func (b *BlockNum[T]) MatchEqual(val interface{}, bits, mask *vec.Bitset) *vec.Bitset {
	return b.data.MatchEqual(val, bits, mask)
}

func (b *BlockBytes) MatchNotEqual(val interface{}, bits, mask *vec.Bitset) *vec.Bitset {
	return b.data.MatchNotEqual(val.([]byte), bits, mask)
}

func (b *BlockString) MatchNotEqual(val interface{}, bits, mask *vec.Bitset) *vec.Bitset {
	return b.data.MatchNotEqual([]byte(val.(string)), bits, mask)
}

func (b *BlockTime) MatchNotEqual(val interface{}, bits, mask *vec.Bitset) *vec.Bitset {
	return vec.MatchInt64NotEqual(b.data.Slice(), val.(time.Time).UnixNano(), bits, mask)
}

func (b *BlockBool) MatchNotEqual(val interface{}, bits, mask *vec.Bitset) *vec.Bitset {
	if val.(bool) {
		return bits.Copy(b.data).Neg()
	} else {
		return bits.Copy(b.data)
	}
}

func (b *BlockInt256) MatchNotEqual(val interface{}, bits, mask *vec.Bitset) *vec.Bitset {
	return vec.MatchInt256NotEqual(b.data, val.(vec.Int256), bits, mask)
}

func (b *BlockInt128) MatchNotEqual(val interface{}, bits, mask *vec.Bitset) *vec.Bitset {
	return vec.MatchInt128NotEqual(b.data, val.(vec.Int128), bits, mask)
}

func (b *BlockNum[T]) MatchNotEqual(val interface{}, bits, mask *vec.Bitset) *vec.Bitset {
	return b.data.MatchNotEqual(val, bits, mask)
}

func (b *BlockBytes) MatchGreaterThan(val interface{}, bits, mask *vec.Bitset) *vec.Bitset {
	return b.data.MatchGreaterThan(val.([]byte), bits, mask)
}

func (b *BlockString) MatchGreaterThan(val interface{}, bits, mask *vec.Bitset) *vec.Bitset {
	return b.data.MatchGreaterThan([]byte(val.(string)), bits, mask)
}

func (b *BlockTime) MatchGreaterThan(val interface{}, bits, mask *vec.Bitset) *vec.Bitset {
	return vec.MatchInt64GreaterThan(b.data.Slice(), val.(time.Time).UnixNano(), bits, mask)
}

func (b *BlockBool) MatchGreaterThan(val interface{}, bits, mask *vec.Bitset) *vec.Bitset {
	if val.(bool) {
		return bits
	} else {
		return bits.Copy(b.data)
	}
}

func (b *BlockInt256) MatchGreaterThan(val interface{}, bits, mask *vec.Bitset) *vec.Bitset {
	return vec.MatchInt256GreaterThan(b.data, val.(vec.Int256), bits, mask)
}

func (b *BlockInt128) MatchGreaterThan(val interface{}, bits, mask *vec.Bitset) *vec.Bitset {
	return vec.MatchInt128GreaterThan(b.data, val.(vec.Int128), bits, mask)
}

func (b *BlockNum[T]) MatchGreaterThan(val interface{}, bits, mask *vec.Bitset) *vec.Bitset {
	return b.data.MatchGreaterThan(val, bits, mask)
}

func (b *BlockBytes) MatchGreaterThanEqual(val interface{}, bits, mask *vec.Bitset) *vec.Bitset {
	return b.data.MatchGreaterThanEqual(val.([]byte), bits, mask)
}

func (b *BlockString) MatchGreaterThanEqual(val interface{}, bits, mask *vec.Bitset) *vec.Bitset {
	return b.data.MatchGreaterThanEqual([]byte(val.(string)), bits, mask)
}

func (b *BlockTime) MatchGreaterThanEqual(val interface{}, bits, mask *vec.Bitset) *vec.Bitset {
	return vec.MatchInt64GreaterThanEqual(b.data.Slice(), val.(time.Time).UnixNano(), bits, mask)
}

func (b *BlockBool) MatchGreaterThanEqual(val interface{}, bits, mask *vec.Bitset) *vec.Bitset {
	return bits.Copy(b.data)
}

func (b *BlockInt256) MatchGreaterThanEqual(val interface{}, bits, mask *vec.Bitset) *vec.Bitset {
	return vec.MatchInt256GreaterThanEqual(b.data, val.(vec.Int256), bits, mask)
}

func (b *BlockInt128) MatchGreaterThanEqual(val interface{}, bits, mask *vec.Bitset) *vec.Bitset {
	return vec.MatchInt128GreaterThanEqual(b.data, val.(vec.Int128), bits, mask)
}

func (b *BlockNum[T]) MatchGreaterThanEqual(val interface{}, bits, mask *vec.Bitset) *vec.Bitset {
	return b.data.MatchGreaterThanEqual(val, bits, mask)
}

func (b *BlockBytes) MatchLessThan(val interface{}, bits, mask *vec.Bitset) *vec.Bitset {
	return b.data.MatchLessThan(val.([]byte), bits, mask)
}

func (b *BlockString) MatchLessThan(val interface{}, bits, mask *vec.Bitset) *vec.Bitset {
	return b.data.MatchLessThan([]byte(val.(string)), bits, mask)
}

func (b *BlockTime) MatchLessThan(val interface{}, bits, mask *vec.Bitset) *vec.Bitset {
	return vec.MatchInt64LessThan(b.data.Slice(), val.(time.Time).UnixNano(), bits, mask)
}

func (b *BlockBool) MatchLessThan(val interface{}, bits, mask *vec.Bitset) *vec.Bitset {
	if val.(bool) {
		return bits.Copy(b.data).Neg()
	} else {
		return bits
	}
}

func (b *BlockInt256) MatchLessThan(val interface{}, bits, mask *vec.Bitset) *vec.Bitset {
	return vec.MatchInt256LessThan(b.data, val.(vec.Int256), bits, mask)
}

func (b *BlockInt128) MatchLessThan(val interface{}, bits, mask *vec.Bitset) *vec.Bitset {
	return vec.MatchInt128LessThan(b.data, val.(vec.Int128), bits, mask)
}

func (b *BlockNum[T]) MatchLessThan(val interface{}, bits, mask *vec.Bitset) *vec.Bitset {
	return b.data.MatchLessThan(val, bits, mask)
}

func (b *BlockBytes) MatchLessThanEqual(val interface{}, bits, mask *vec.Bitset) *vec.Bitset {
	return b.data.MatchLessThanEqual(val.([]byte), bits, mask)
}

func (b *BlockString) MatchLessThanEqual(val interface{}, bits, mask *vec.Bitset) *vec.Bitset {
	return b.data.MatchLessThanEqual([]byte(val.(string)), bits, mask)
}

func (b *BlockTime) MatchLessThanEqual(val interface{}, bits, mask *vec.Bitset) *vec.Bitset {
	return vec.MatchInt64LessThanEqual(b.data.Slice(), val.(time.Time).UnixNano(), bits, mask)
}

func (b *BlockBool) MatchLessThanEqual(val interface{}, bits, mask *vec.Bitset) *vec.Bitset {
	return bits.Copy(b.data)
}

func (b *BlockInt256) MatchLessThanEqual(val interface{}, bits, mask *vec.Bitset) *vec.Bitset {
	return vec.MatchInt256LessThanEqual(b.data, val.(vec.Int256), bits, mask)
}

func (b *BlockInt128) MatchLessThanEqual(val interface{}, bits, mask *vec.Bitset) *vec.Bitset {
	return vec.MatchInt128LessThanEqual(b.data, val.(vec.Int128), bits, mask)
}

func (b *BlockNum[T]) MatchLessThanEqual(val interface{}, bits, mask *vec.Bitset) *vec.Bitset {
	return b.data.MatchLessThanEqual(val, bits, mask)
}

func (b *BlockBytes) MatchBetween(from, to interface{}, bits, mask *vec.Bitset) *vec.Bitset {
	return b.data.MatchBetween(from.([]byte), to.([]byte), bits, mask)
}

func (b *BlockString) MatchBetween(from, to interface{}, bits, mask *vec.Bitset) *vec.Bitset {
	fromb := compress.UnsafeGetBytes(from.(string))
	tob := compress.UnsafeGetBytes(to.(string))
	return b.data.MatchBetween(fromb, tob, bits, mask)
}

func (b *BlockTime) MatchBetween(from, to interface{}, bits, mask *vec.Bitset) *vec.Bitset {
	return vec.MatchInt64Between(b.data.Slice(), from.(time.Time).UnixNano(), to.(time.Time).UnixNano(), bits, mask)
}

func (b *BlockBool) MatchBetween(from, to interface{}, bits, mask *vec.Bitset) *vec.Bitset {
	switch from, to := from.(bool), to.(bool); true {
	case from != to:
		return bits.Copy(b.data)
	case from:
		return bits.Copy(b.data)
	default:
		return bits.Copy(b.data).Neg()
	}
}

func (b *BlockInt256) MatchBetween(from, to interface{}, bits, mask *vec.Bitset) *vec.Bitset {
	return vec.MatchInt256Between(b.data, from.(vec.Int256), to.(vec.Int256), bits, mask)
}

func (b *BlockInt128) MatchBetween(from, to interface{}, bits, mask *vec.Bitset) *vec.Bitset {
	return vec.MatchInt128Between(b.data, from.(vec.Int128), to.(vec.Int128), bits, mask)
}

func (b *BlockNum[T]) MatchBetween(from, to interface{}, bits, mask *vec.Bitset) *vec.Bitset {
	return b.data.MatchBetween(from, to, bits, mask)
}

func (b *blockCommon) MatchRegExp(re string, bits, mask *vec.Bitset) *vec.Bitset {
	return bits
}

func (b *BlockString) MatchRegExp(re string, bits, mask *vec.Bitset) *vec.Bitset {
	rematch := strings.Replace(re, "*", ".*", -1)
	for i, v := range b.data.Slice() {
		// skip masked values
		if mask != nil && !mask.IsSet(i) {
			continue
		}
		if match, _ := regexp.Match(rematch, v); match {
			bits.Set(i)
		}
	}
	return bits
}

func (b *BlockTime) MatchRegExp(re string, bits, mask *vec.Bitset) *vec.Bitset {
	rematch := strings.Replace(re, "*", ".*", -1)
	for i, v := range b.data.Slice() {
		// skip masked values
		if mask != nil && !mask.IsSet(i) {
			continue
		}
		val := time.Unix(0, v).Format(time.RFC3339)
		if match, _ := regexp.MatchString(rematch, val); match {
			bits.Set(i)
		}
	}
	return bits
}

func (b *BlockNum[T]) EqualAt(i int, val interface{}) bool {
	return b.data.Elem(i) == val.(T)
}

func (b *BlockBytes) EqualAt(i int, val interface{}) bool {
	return bytes.Equal(b.data.Elem(i), val.([]byte))
}

func (b *BlockString) EqualAt(i int, val interface{}) bool {
	return compress.UnsafeGetString(b.data.Elem(i)) == val.(string)
}

func (b *BlockTime) EqualAt(i int, val interface{}) bool {
	ts := b.data.Elem(i)
	if ts == 0 {
		return zeroTime.Equal(val.(time.Time))
	}
	return time.Unix(0, ts).Equal(val.(time.Time))
}

func (b *BlockBool) EqualAt(i int, val interface{}) bool {
	return b.data.IsSet(i) == val.(bool)
}

func (b *BlockInt128) EqualAt(i int, val interface{}) bool {
	return b.data.Elem(i).Eq(val.(vec.Int128))
}

func (b *BlockInt256) EqualAt(i int, val interface{}) bool {
	return b.data.Elem(i).Eq(val.(vec.Int256))
}

func (b *BlockDec256) EqualAt(i int, val interface{}) bool {
	return decimal.NewDecimal256(b.data.Elem(i), b.scale).Eq(val.(decimal.Decimal256))
}

func (b *BlockDec128) EqualAt(i int, val interface{}) bool {
	return decimal.NewDecimal128(b.data.Elem(i), b.scale).Eq(val.(decimal.Decimal128))
}

func (b *BlockDec64) EqualAt(i int, val interface{}) bool {
	return decimal.NewDecimal64(b.data.Elem(i), b.scale).Eq(val.(decimal.Decimal64))
}

func (b *BlockDec32) EqualAt(i int, val interface{}) bool {
	return decimal.NewDecimal32(b.data.Elem(i), b.scale).Eq(val.(decimal.Decimal32))
}

func (b *BlockNum[T]) GtAt(i int, val interface{}) bool {
	return b.data.Elem(i) > val.(T)
}

func (b *BlockBytes) GtAt(i int, val interface{}) bool {
	return bytes.Compare(b.data.Elem(i), val.([]byte)) > 0
}

func (b *BlockString) GtAt(i int, val interface{}) bool {
	return compress.UnsafeGetString(b.data.Elem(i)) > val.(string)
}

func (b *BlockTime) GtAt(i int, val interface{}) bool {
	ts := b.data.Elem(i)
	if ts == 0 {
		return zeroTime.After(val.(time.Time))
	}
	return time.Unix(0, ts).After(val.(time.Time))
}

func (b *BlockBool) GtAt(i int, val interface{}) bool {
	// FIXME: check this
	return b.data.IsSet(i) != val.(bool)
}

func (b *BlockInt128) GtAt(i int, val interface{}) bool {
	return b.data.Elem(i).Gt(val.(vec.Int128))
}

func (b *BlockInt256) GtAt(i int, val interface{}) bool {
	return b.data.Elem(i).Gt(val.(vec.Int256))
}

func (b *BlockDec256) GtAt(i int, val interface{}) bool {
	return decimal.NewDecimal256(b.data.Elem(i), b.scale).Gt(val.(decimal.Decimal256))
}

func (b *BlockDec128) GtAt(i int, val interface{}) bool {
	return decimal.NewDecimal128(b.data.Elem(i), b.scale).Gt(val.(decimal.Decimal128))
}

func (b *BlockDec64) GtAt(i int, val interface{}) bool {
	return decimal.NewDecimal64(b.data.Elem(i), b.scale).Gt(val.(decimal.Decimal64))
}

func (b *BlockDec32) GtAt(i int, val interface{}) bool {
	return decimal.NewDecimal32(b.data.Elem(i), b.scale).Gt(val.(decimal.Decimal32))
}

func (b *BlockNum[T]) GteAt(i int, val interface{}) bool {
	return b.data.Elem(i) >= val.(T)
}

func (b *BlockBytes) GteAt(i int, val interface{}) bool {
	return bytes.Compare(b.data.Elem(i), val.([]byte)) >= 0
}

func (b *BlockString) GteAt(i int, val interface{}) bool {
	return compress.UnsafeGetString(b.data.Elem(i)) >= val.(string)
}

func (b *BlockTime) GteAt(i int, val interface{}) bool {
	ts := b.data.Elem(i)
	if ts == 0 {
		return !zeroTime.Before(val.(time.Time))
	}
	return !time.Unix(0, ts).Before(val.(time.Time))
}

func (b *BlockBool) GteAt(i int, val interface{}) bool {
	// FIXME: check this
	return true
}

func (b *BlockInt128) GteAt(i int, val interface{}) bool {
	return b.data.Elem(i).Gte(val.(vec.Int128))
}

func (b *BlockInt256) GteAt(i int, val interface{}) bool {
	return b.data.Elem(i).Gte(val.(vec.Int256))
}

func (b *BlockDec256) GteAt(i int, val interface{}) bool {
	return decimal.NewDecimal256(b.data.Elem(i), b.scale).Gte(val.(decimal.Decimal256))
}

func (b *BlockDec128) GteAt(i int, val interface{}) bool {
	return decimal.NewDecimal128(b.data.Elem(i), b.scale).Gte(val.(decimal.Decimal128))
}

func (b *BlockDec64) GteAt(i int, val interface{}) bool {
	return decimal.NewDecimal64(b.data.Elem(i), b.scale).Gte(val.(decimal.Decimal64))
}

func (b *BlockDec32) GteAt(i int, val interface{}) bool {
	return decimal.NewDecimal32(b.data.Elem(i), b.scale).Gte(val.(decimal.Decimal32))
}

func (b *BlockNum[T]) LtAt(i int, val interface{}) bool {
	return b.data.Elem(i) < val.(T)
}

func (b *BlockBytes) LtAt(i int, val interface{}) bool {
	return bytes.Compare(b.data.Elem(i), val.([]byte)) < 0
}

func (b *BlockString) LtAt(i int, val interface{}) bool {
	return compress.UnsafeGetString(b.data.Elem(i)) < val.(string)
}

func (b *BlockTime) LtAt(i int, val interface{}) bool {
	ts := b.data.Elem(i)
	if ts == 0 {
		return zeroTime.Before(val.(time.Time))
	}
	return time.Unix(0, ts).Before(val.(time.Time))
}

func (b *BlockBool) LtAt(i int, val interface{}) bool {
	// FIXME: check this
	return b.data.IsSet(i) != val.(bool)
}

func (b *BlockInt128) LtAt(i int, val interface{}) bool {
	return b.data.Elem(i).Lt(val.(vec.Int128))
}

func (b *BlockInt256) LtAt(i int, val interface{}) bool {
	return b.data.Elem(i).Lt(val.(vec.Int256))
}

func (b *BlockDec256) LtAt(i int, val interface{}) bool {
	return decimal.NewDecimal256(b.data.Elem(i), b.scale).Lt(val.(decimal.Decimal256))
}

func (b *BlockDec128) LtAt(i int, val interface{}) bool {
	return decimal.NewDecimal128(b.data.Elem(i), b.scale).Lt(val.(decimal.Decimal128))
}

func (b *BlockDec64) LtAt(i int, val interface{}) bool {
	return decimal.NewDecimal64(b.data.Elem(i), b.scale).Lt(val.(decimal.Decimal64))
}

func (b *BlockDec32) LtAt(i int, val interface{}) bool {
	return decimal.NewDecimal32(b.data.Elem(i), b.scale).Lt(val.(decimal.Decimal32))
}

func (b *BlockNum[T]) LteAt(i int, val interface{}) bool {
	return b.data.Elem(i) <= val.(T)
}

func (b *BlockBytes) LteAt(i int, val interface{}) bool {
	return bytes.Compare(b.data.Elem(i), val.([]byte)) <= 0
}

func (b *BlockString) LteAt(i int, val interface{}) bool {
	return compress.UnsafeGetString(b.data.Elem(i)) <= val.(string)
}

func (b *BlockTime) LteAt(i int, val interface{}) bool {
	ts := b.data.Elem(i)
	if ts == 0 {
		return !zeroTime.After(val.(time.Time))
	}
	return !time.Unix(0, ts).After(val.(time.Time))
}

func (b *BlockBool) LteAt(i int, val interface{}) bool {
	// FIXME: check this
	v := val.(bool)
	return v || b.data.IsSet(i) == v
}

func (b *BlockInt128) LteAt(i int, val interface{}) bool {
	return b.data.Elem(i).Lte(val.(vec.Int128))
}

func (b *BlockInt256) LteAt(i int, val interface{}) bool {
	return b.data.Elem(i).Lte(val.(vec.Int256))
}

func (b *BlockDec256) LteAt(i int, val interface{}) bool {
	return decimal.NewDecimal256(b.data.Elem(i), b.scale).Lte(val.(decimal.Decimal256))
}

func (b *BlockDec128) LteAt(i int, val interface{}) bool {
	return decimal.NewDecimal128(b.data.Elem(i), b.scale).Lte(val.(decimal.Decimal128))
}

func (b *BlockDec64) LteAt(i int, val interface{}) bool {
	return decimal.NewDecimal64(b.data.Elem(i), b.scale).Lte(val.(decimal.Decimal64))
}

func (b *BlockDec32) LteAt(i int, val interface{}) bool {
	return decimal.NewDecimal32(b.data.Elem(i), b.scale).Lte(val.(decimal.Decimal32))
}

func (b *BlockNum[T]) BetweenAt(i int, from, to interface{}) bool {
	val := b.data.Elem(i)
	return !(val < from.(T) || val > to.(T))
}

func (b *BlockBytes) BetweenAt(i int, from, to interface{}) bool {
	val := b.data.Elem(i)
	fromMatch := bytes.Compare(val, from.([]byte))
	if fromMatch == 0 || len(from.([]byte)) == 0 {
		return true
	}
	if fromMatch < 0 {
		return false
	}
	toMatch := bytes.Compare(val, to.([]byte))
	if toMatch > 0 {
		return false
	}
	return true
}

func (b *BlockString) BetweenAt(i int, from, to interface{}) bool {
	val := compress.UnsafeGetString(b.data.Elem(i))
	fromMatch := strings.Compare(val, from.(string))
	if fromMatch == 0 || len(from.(string)) == 0 {
		return true
	}
	if fromMatch < 0 {
		return false
	}
	toMatch := strings.Compare(val, to.(string))
	if toMatch > 0 {
		return false
	}
	return true
}

func (b *BlockTime) BetweenAt(i int, from, to interface{}) bool {
	var val time.Time
	if ts := b.data.Elem(i); ts == 0 {
		val = zeroTime
	} else {
		val = time.Unix(0, ts)
	}

	if val.Before(from.(time.Time)) {
		return false
	}
	if val.After(to.(time.Time)) {
		return false
	}
	return true
}

func (b *BlockBool) BetweenAt(i int, from, to interface{}) bool {
	// FIXME: check this
	val := b.data.IsSet(i)
	switch true {
	case from.(bool) != to.(bool):
		return true
	case from.(bool) == val:
		return true
	case to.(bool) == val:
		return true
	}
	return false
}

func (b *BlockInt256) BetweenAt(i int, from, to interface{}) bool {
	val := b.data.Elem(i)
	return !(val.Lt(from.(vec.Int256)) || val.Gt(to.(vec.Int256)))
}

func (b *BlockInt128) BetweenAt(i int, from, to interface{}) bool {
	val := b.data.Elem(i)
	return !(val.Lt(from.(vec.Int128)) || val.Gt(to.(vec.Int128)))
}

func (b *BlockDec256) BetweenAt(i int, from, to interface{}) bool {
	val := decimal.NewDecimal256(b.data.Elem(i), b.scale)
	return !(val.Lt(from.(decimal.Decimal256)) || val.Gt(to.(decimal.Decimal256)))
}

func (b *BlockDec128) BetweenAt(i int, from, to interface{}) bool {
	val := decimal.NewDecimal128(b.data.Elem(i), b.scale)
	return !(val.Lt(from.(decimal.Decimal128)) || val.Gt(to.(decimal.Decimal128)))
}

func (b *BlockDec64) BetweenAt(i int, from, to interface{}) bool {
	val := decimal.NewDecimal64(b.data.Elem(i), b.scale)
	return !(val.Lt(from.(decimal.Decimal64)) || val.Gt(to.(decimal.Decimal64)))
}

func (b *BlockDec32) BetweenAt(i int, from, to interface{}) bool {
	val := decimal.NewDecimal32(b.data.Elem(i), b.scale)
	return !(val.Lt(from.(decimal.Decimal32)) || val.Gt(to.(decimal.Decimal32)))
}

func (b *blockCommon) RegExpAt(i int, re string) bool {
	return false
}

func (b *BlockString) RegExpAt(i int, re string) bool {
	val := compress.UnsafeGetString(b.data.Elem(i))
	match, _ := regexp.MatchString(strings.Replace(re, "*", ".*", -1), val)
	return match
}

func (b *BlockTime) RegExpAt(i int, re string) bool {
	var val time.Time
	if ts := b.data.Elem(i); ts == 0 {
		val = zeroTime
	} else {
		val = time.Unix(0, ts)
	}

	match, _ := regexp.MatchString(
		strings.Replace(re, "*", ".*", -1),
		val.Format(time.RFC3339),
	)
	return match
}

// FIXME: Dump should not be part of Block interface
func (b *blockCommon) Dump() []byte {
	return []byte{}
}

func (b *BlockBytes) Dump() []byte {
	return []byte(b.data.Dump())
}
