// Copyright (c) 2018-2022 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package block

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"reflect"
	"sync/atomic"
	"time"
	"unsafe"

	"blockwatch.cc/knoxdb/encoding/compress"
	"blockwatch.cc/knoxdb/encoding/dedup"
	"blockwatch.cc/knoxdb/hash/xxhash"
	"blockwatch.cc/knoxdb/vec"
)

var bigEndian = binary.BigEndian

// FixMe: check if this works correctly
var BlockSz = int(reflect.TypeOf(blockCommon{}).Size()) + 8

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

// Note: uses 5 bit encoding (max 32 values)
type BlockType byte

const (
	BlockTypeTime    = BlockType(0)
	BlockTypeInt64   = BlockType(1)
	BlockTypeUint64  = BlockType(2)
	BlockTypeFloat64 = BlockType(3)
	BlockTypeBool    = BlockType(4)
	BlockTypeString  = BlockType(5)
	BlockTypeBytes   = BlockType(6)
	BlockTypeInt32   = BlockType(7)
	BlockTypeInt16   = BlockType(8)
	BlockTypeInt8    = BlockType(9)
	BlockTypeUint32  = BlockType(10)
	BlockTypeUint16  = BlockType(11)
	BlockTypeUint8   = BlockType(12)
	BlockTypeFloat32 = BlockType(13)
	BlockTypeInt128  = BlockType(14)
	BlockTypeInt256  = BlockType(15)
	BlockTypeInvalid = BlockType(16)
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
	IsDirty() bool
	Type() BlockType
	SetDirty()
	SetCompression(Compression)
	Release()
	Hashes([]uint64) []uint64
	HeapSize() int
	Compression() Compression
	Slice() interface{}
	RangeSlice(int, int) interface{}
	Clear()
	Len() int
	Cap() int
	MaxStoredSize() int
	Encode(*bytes.Buffer) (int, error)
	Decode([]byte, int, int) error
	MinMax() (interface{}, interface{})
	CompressedSize() int
	Less(int, int) bool
	Swap(int, int)
	Elem(int) interface{}
	Set(int, interface{})
	Append(interface{})
	AppendFrom(Block, int, int)
	ReplaceFrom(Block, int, int, int)
	Delete(int, int)
	Copy(Block)
	Grow(int)
	InsertFrom(Block, int, int, int)
	Optimize()
	Materialize()
	MatchEqual(interface{}, *vec.Bitset, *vec.Bitset) *vec.Bitset
	MatchNotEqual(interface{}, *vec.Bitset, *vec.Bitset) *vec.Bitset
	MatchGreaterThan(interface{}, *vec.Bitset, *vec.Bitset) *vec.Bitset
	MatchGreaterThanEqual(interface{}, *vec.Bitset, *vec.Bitset) *vec.Bitset
	MatchLessThan(interface{}, *vec.Bitset, *vec.Bitset) *vec.Bitset
	MatchLessThanEqual(interface{}, *vec.Bitset, *vec.Bitset) *vec.Bitset
	MatchBetween(interface{}, interface{}, *vec.Bitset, *vec.Bitset) *vec.Bitset
	Dump() []byte
}

type blockCommon struct {
	refCount int64
	//typ      BlockType
	comp  Compression
	dirty bool
	size  int // stored size, debug data
}

type BlockNum[T Number] struct {
	blockCommon
	//typ  BlockType
	dataNum[T]
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

type BlockTime struct {
	BlockNum[int64]
}

type dataNum[T Number] struct {
	data []T
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

func (b *BlockNum[N]) Type() BlockType {
	switch reflect.ValueOf(*new(N)).Kind() {
	case reflect.Int64:
		return BlockTypeInt64
	case reflect.Int32:
		return BlockTypeInt32
	case reflect.Int16:
		return BlockTypeInt16
	case reflect.Int8:
		return BlockTypeInt8
	case reflect.Uint64:
		return BlockTypeUint64
	case reflect.Uint32:
		return BlockTypeUint32
	case reflect.Uint16:
		return BlockTypeUint16
	case reflect.Uint8:
		return BlockTypeUint8
	case reflect.Float64:
		return BlockTypeFloat64
	case reflect.Float32:
		return BlockTypeFloat32
	}
	return BlockTypeInvalid
}

func (b *BlockTime) Type() BlockType {
	return BlockTypeTime
}

func (b *BlockBytes) Type() BlockType {
	return BlockTypeBytes
}

func (b *BlockString) Type() BlockType {
	return BlockTypeString
}

func (b *BlockBool) Type() BlockType {
	return BlockTypeBool
}

func (b *BlockInt128) Type() BlockType {
	return BlockTypeInt128
}

func (b *BlockInt256) Type() BlockType {
	return BlockTypeInt256
}

/*
	func (b Block) IsInt() bool {
		switch b.Type() {
		case BlockTypeInt64, BlockTypeInt32, BlockTypeInt16, BlockTypeInt8,
			BlockTypeUint64, BlockTypeUint32, BlockTypeUint16, BlockTypeUint8:
			return true
		default:
			return false
		}
	}

	func (b Block) IsSint() bool {
		switch b.Type() {
		case BlockTypeInt64, BlockTypeInt32, BlockTypeInt16, BlockTypeInt8:
			return true
		default:
			return false
		}
	}

	func (b Block) IsUint() bool {
		switch b.Type() {
		case BlockTypeUint64, BlockTypeUint32, BlockTypeUint16, BlockTypeUint8:
			return true
		default:
			return false
		}
	}

	func (b Block) IsFloat() bool {
		switch b.Type() {
		case BlockTypeFloat64, BlockTypeFloat32:
			return true
		default:
			return false
		}
	}
*/
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

func (b *blockCommon) SetCompression(c Compression) {
	b.comp = c
}

func (b *BlockNum[N]) Slice() interface{} {
	return b.data
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

func (b *BlockNum[T]) RangeSlice(start, end int) interface{} {
	return b.data[start:end]
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

func (b *BlockNum[T]) Elem(idx int) interface{} {
	if idx >= b.Len() {
		return nil
	}
	return b.data[idx]
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

func (b *BlockNum[T]) Set(i int, val interface{}) {
	b.data[i] = val.(T)
}

func (b *BlockInt128) Set(i int, val interface{}) {
	b.data.Set(i, val.(vec.Int128))
}

func (b *BlockInt256) Set(i int, val interface{}) {
	b.data.Set(i, val.(vec.Int256))
}

func (b *BlockBool) Set(i int, val interface{}) {
	if val.(bool) {
		b.data.Set(i)
	} else {
		b.data.Clear(i)
	}
}

func (b *BlockBytes) Set(i int, val interface{}) {
	b.data.Set(i, val.([]byte))
}

func (b *BlockNum[T]) Grow(n int) {
	b.data = append(b.data, make([]T, n)...)
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

func (b *BlockNum[T]) Append(val interface{}) {
	b.data = append(b.data, val.(T))
}

func (b *BlockInt128) Append(val interface{}) {
	b.data.Append(val.(vec.Int128))
}

func (b *BlockInt256) Append(val interface{}) {
	b.data.Append(val.(vec.Int256))
}

func (b *BlockBytes) Append(val interface{}) {
	b.data.Append(val.([]byte))
}

func (b *BlockBool) Append(val interface{}) {
	l := b.data.Len()
	b.data.Grow(l + 1)
	if val.(bool) {
		b.data.Set(l)
	}
}

func (b *BlockNum[T]) Delete(pos, n int) {
	b.data = append(b.data[:pos], b.data[pos+n:]...)
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

/*func AllocBlock() *Block {
	return BlockPool.Get().(*Block)
}*/

func NewBlock(typ BlockType, comp Compression, sz int) Block {
	var bl Block
	switch typ {
	case BlockTypeTime:
		b := new(BlockTime)
		b.data = arena.Alloc(typ, sz).([]int64)
		bl = b
	case BlockTypeInt64:
		b := new(BlockNum[int64])
		b.data = arena.Alloc(typ, sz).([]int64)
		bl = b
	case BlockTypeFloat64:
		b := new(BlockNum[float64])
		b.data = arena.Alloc(typ, sz).([]float64)
		bl = b
	case BlockTypeFloat32:
		b := new(BlockNum[float32])
		b.data = arena.Alloc(typ, sz).([]float32)
		bl = b
	case BlockTypeInt32:
		b := new(BlockNum[int32])
		b.data = arena.Alloc(typ, sz).([]int32)
		bl = b
	case BlockTypeInt16:
		b := new(BlockNum[int16])
		b.data = arena.Alloc(typ, sz).([]int16)
		bl = b
	case BlockTypeInt8:
		b := new(BlockNum[int8])
		b.data = arena.Alloc(typ, sz).([]int8)
		bl = b
	case BlockTypeUint64:
		b := new(BlockNum[uint64])
		b.data = arena.Alloc(typ, sz).([]uint64)
		bl = b
	case BlockTypeUint32:
		b := new(BlockNum[uint32])
		b.data = arena.Alloc(typ, sz).([]uint32)
		bl = b
	case BlockTypeUint16:
		b := new(BlockNum[uint16])
		b.data = arena.Alloc(typ, sz).([]uint16)
		bl = b
	case BlockTypeUint8:
		b := new(BlockNum[uint8])
		b.data = arena.Alloc(typ, sz).([]uint8)
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
	}
	//b.typ = typ
	bl.SetCompression(comp)
	bl.SetDirty()
	return bl
}

func NewBlockFromSlice(typ BlockType, comp Compression, slice interface{}) Block {
	var bl Block
	switch typ {
	case BlockTypeTime:
		b := new(BlockTime)
		b.data = slice.([]int64)
		bl = b
	case BlockTypeInt64:
		b := new(BlockNum[int64])
		b.data = slice.([]int64)
		bl = b
	case BlockTypeFloat64:
		b := new(BlockNum[float64])
		b.data = slice.([]float64)
		bl = b
	case BlockTypeFloat32:
		b := new(BlockNum[float32])
		b.data = slice.([]float32)
		bl = b
	case BlockTypeInt32:
		b := new(BlockNum[int32])
		b.data = slice.([]int32)
		bl = b
	case BlockTypeInt16:
		b := new(BlockNum[int16])
		b.data = slice.([]int16)
		bl = b
	case BlockTypeInt8:
		b := new(BlockNum[int8])
		b.data = slice.([]int8)
		bl = b
	case BlockTypeUint64:
		b := new(BlockNum[uint64])
		b.data = slice.([]uint64)
		bl = b
	case BlockTypeUint32:
		b := new(BlockNum[uint32])
		b.data = slice.([]uint32)
		bl = b
	case BlockTypeUint16:
		b := new(BlockNum[uint16])
		b.data = slice.([]uint16)
		bl = b
	case BlockTypeUint8:
		b := new(BlockNum[uint8])
		b.data = slice.([]uint8)
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
	//b.typ = typ
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
	b.data = b.data[:sb.size]
	copy(b.data, sb.data)
}

func (b *BlockNum[T]) Copy(src Block) {
	if src == nil {
		return
	}
	sb := src.(*BlockNum[T])
	b.size = sb.size
	b.dirty = true
	b.data = b.data[:sb.size]
	copy(b.data, sb.data)
}

func (b *BlockBool) Copy(src Block) {
	if src == nil {
		return
	}
	sb := src.(*BlockBool)
	b.size = sb.size
	b.dirty = true
	b.data = vec.NewBitsetFromBytes(sb.data.Bytes(), sb.size)
}

func (b *BlockBytes) Copy(src Block) {
	if src == nil {
		return
	}
	sb := src.(*BlockBytes)
	b.size = sb.size
	b.dirty = true
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
	b.data = append(b.data, sb.data[pos:pos+len]...)
}

func (b *BlockNum[T]) AppendFrom(src Block, pos, len int) {
	sb := src.(*BlockNum[T])
	b.data = append(b.data, sb.data[pos:pos+len]...)
}

func (b *BlockInt256) AppendFrom(src Block, pos, len int) {
	sb := src.(*BlockInt256)
	b.data.AppendFrom(sb.data.Subslice(pos, pos+len))
}

func (b *BlockInt128) AppendFrom(src Block, pos, len int) {
	sb := src.(*BlockInt128)
	b.data.AppendFrom(sb.data.Subslice(pos, pos+len))
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
	copy(b.data[dpos:], sb.data[spos:spos+len])
}

func (b *BlockNum[T]) ReplaceFrom(src Block, spos, dpos, len int) {
	sb := src.(*BlockNum[T])
	copy(b.data[dpos:], sb.data[spos:spos+len])
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

func (b *BlockTime) InsertFrom(src Block, spos, dpos, len int) {
	sb := src.(*BlockTime)
	b.data = vec.Int64.Insert(b.data, dpos, sb.data[spos:spos+len]...)
}

func (b *BlockNum[T]) InsertFrom(src Block, spos, dpos, len int) {
	sb := src.(*BlockNum[T])
	switch b.Type() {
	case BlockTypeFloat64:
		b.data = interface{}(vec.Float64.Insert(interface{}(b.data).([]float64), dpos, interface{}(sb.data).([]float64)[spos:spos+len]...)).([]T)
	case BlockTypeFloat32:
		b.data = interface{}(vec.Float32.Insert(interface{}(b.data).([]float32), dpos, interface{}(sb.data).([]float32)[spos:spos+len]...)).([]T)
	case BlockTypeInt64:
		b.data = interface{}(vec.Int64.Insert(interface{}(b.data).([]int64), dpos, interface{}(sb.data).([]int64)[spos:spos+len]...)).([]T)
	case BlockTypeInt32:
		b.data = interface{}(vec.Int32.Insert(interface{}(b.data).([]int32), dpos, interface{}(sb.data).([]int32)[spos:spos+len]...)).([]T)
	case BlockTypeInt16:
		b.data = interface{}(vec.Int16.Insert(interface{}(b.data).([]int16), dpos, interface{}(sb.data).([]int16)[spos:spos+len]...)).([]T)
	case BlockTypeInt8:
		b.data = interface{}(vec.Int8.Insert(interface{}(b.data).([]int8), dpos, interface{}(sb.data).([]int8)[spos:spos+len]...)).([]T)
	case BlockTypeUint64:
		b.data = interface{}(vec.Uint64.Insert(interface{}(b.data).([]uint64), dpos, interface{}(sb.data).([]uint64)[spos:spos+len]...)).([]T)
	case BlockTypeUint32:
		b.data = interface{}(vec.Uint32.Insert(interface{}(b.data).([]uint32), dpos, interface{}(sb.data).([]uint32)[spos:spos+len]...)).([]T)
	case BlockTypeUint16:
		b.data = interface{}(vec.Uint16.Insert(interface{}(b.data).([]uint16), dpos, interface{}(sb.data).([]uint16)[spos:spos+len]...)).([]T)
	case BlockTypeUint8:
		b.data = interface{}(vec.Uint8.Insert(interface{}(b.data).([]uint8), dpos, interface{}(sb.data).([]uint8)[spos:spos+len]...)).([]T)
	}
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

func (b *BlockNum[T]) Len() int {
	return len(b.data)
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
	return cap(b.data)
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
func (b *BlockNum[T]) MaxStoredSize() int {
	var sz int
	switch b.Type() {
	case BlockTypeFloat64:
		sz = compress.Float64ArrayEncodedSize(interface{}(b.data).([]float64))
	case BlockTypeFloat32:
		sz = compress.Float32ArrayEncodedSize(interface{}(b.data).([]float32))
	case BlockTypeInt64:
		sz = compress.Int64ArrayEncodedSize(interface{}(b.data).([]int64))
	case BlockTypeInt32:
		sz = compress.Int32ArrayEncodedSize(interface{}(b.data).([]int32))
	case BlockTypeInt16:
		sz = compress.Int16ArrayEncodedSize(interface{}(b.data).([]int16))
	case BlockTypeInt8:
		sz = compress.Int8ArrayEncodedSize(interface{}(b.data).([]int8))
	case BlockTypeUint64:
		sz = compress.Uint64ArrayEncodedSize(interface{}(b.data).([]uint64))
	case BlockTypeUint32:
		sz = compress.Uint32ArrayEncodedSize(interface{}(b.data).([]uint32))
	case BlockTypeUint16:
		sz = compress.Uint16ArrayEncodedSize(interface{}(b.data).([]uint16))
	case BlockTypeUint8:
		sz = compress.Uint8ArrayEncodedSize(interface{}(b.data).([]uint8))
	}
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
	sz += len(b.data) * int(unsafe.Sizeof(new(T)))
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
	sz += b.data.Len() * 16
	return sz
}

func (b *BlockInt256) HeapSize() int {
	sz := BlockSz
	sz += b.data.Len() * 32
	return sz
}

func (b *BlockNum[T]) Clear() {
	b.data = b.data[:0]
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
	arena.Free(b.Type(), b.data[:0])
	b.data = nil
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

func (b *BlockNum[T]) Encode(buf *bytes.Buffer) (int, error) {
	if buf == nil {
		return 0, fmt.Errorf("block: nil buffer while encoding")
	}
	var (
		err error
		n   int
	)

	switch b.Type() {
	case BlockTypeFloat64:
		n, err = encodeFloat64Block(buf, interface{}(b.data).([]float64), b.Compression())
	case BlockTypeFloat32:
		n, err = encodeFloat32Block(buf, interface{}(b.data).([]float32), b.Compression())
	case BlockTypeInt64:
		n, err = encodeInt64Block(buf, interface{}(b.data).([]int64), b.Compression())
	case BlockTypeInt32:
		n, err = encodeInt32Block(buf, interface{}(b.data).([]int32), b.Compression())
	case BlockTypeInt16:
		n, err = encodeInt16Block(buf, interface{}(b.data).([]int16), b.Compression())
	case BlockTypeInt8:
		n, err = encodeInt8Block(buf, interface{}(b.data).([]int8), b.Compression())
	case BlockTypeUint64:
		n, err = encodeUint64Block(buf, interface{}(b.data).([]uint64), b.Compression())
	case BlockTypeUint32:
		n, err = encodeUint32Block(buf, interface{}(b.data).([]uint32), b.Compression())
	case BlockTypeUint16:
		n, err = encodeUint16Block(buf, interface{}(b.data).([]uint16), b.Compression())
	case BlockTypeUint8:
		n, err = encodeUint8Block(buf, interface{}(b.data).([]uint8), b.Compression())
	}
	if err != nil {
		return n, err
	}

	b.dirty = false
	b.size = n
	return n, nil
}

func (b *BlockTime) Encode(buf *bytes.Buffer) (int, error) {
	if buf == nil {
		return 0, fmt.Errorf("block: nil buffer while encoding")
	}

	n, err := encodeTimeBlock(buf, b.data, b.Compression())
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

	if b.data == nil || cap(b.data) < sz {
		arena.Free(typ, b.data)
		b.data = arena.Alloc(typ, sz).([]int64)
	}
	b.data, err = decodeTimeBlock(buf, b.data[:0])
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
		return fmt.Errorf("Decode: unexpected block type %d(%s), expected %d(%s)",
			typ, typ.String(), b.Type(), b.Type().String())
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
		return fmt.Errorf("Decode: unexpected block type %d(%s), expected %d(%s)",
			typ, typ.String(), b.Type(), b.Type().String())
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
		return fmt.Errorf("Decode: unexpected block type %d(%s), expected %d(%s)",
			typ, typ.String(), b.Type(), b.Type().String())
	}
	b.dirty = false
	b.size = stored

	var tmp interface{}

	switch typ {
	case BlockTypeFloat64:
		if b.data == nil || cap(b.data) < sz {
			arena.Free(typ, b.data)
			b.data = arena.Alloc(typ, sz).([]T)
		}
		tmp, err = decodeFloat64Block(buf, interface{}(b.data).([]float64)[:0])
		b.data = tmp.([]T)

	case BlockTypeFloat32:
		if b.data == nil || cap(b.data) < sz {
			arena.Free(typ, b.data)
			b.data = arena.Alloc(typ, sz).([]T)
		}
		tmp, err = decodeFloat32Block(buf, interface{}(b.data).([]float32)[:0])
		b.data = tmp.([]T)

	case BlockTypeInt64:
		if b.data == nil || cap(b.data) < sz {
			arena.Free(typ, b.data)
			b.data = arena.Alloc(typ, sz).([]T)
		}
		tmp, err = decodeInt64Block(buf, interface{}(b.data).([]int64)[:0])
		b.data = tmp.([]T)

	case BlockTypeInt32:
		if b.data == nil || cap(b.data) < sz {
			arena.Free(typ, b.data)
			b.data = arena.Alloc(typ, sz).([]T)
		}
		tmp, err = decodeInt32Block(buf, interface{}(b.data).([]int32)[:0])
		b.data = tmp.([]T)

	case BlockTypeInt16:
		if b.data == nil || cap(b.data) < sz {
			arena.Free(typ, b.data)
			b.data = arena.Alloc(typ, sz).([]T)
		}
		tmp, err = decodeInt16Block(buf, interface{}(b.data).([]int16)[:0])
		b.data = tmp.([]T)

	case BlockTypeInt8:
		if b.data == nil || cap(b.data) < sz {
			arena.Free(typ, b.data)
			b.data = arena.Alloc(typ, sz).([]T)
		}
		tmp, err = decodeInt8Block(buf, interface{}(b.data).([]int8)[:0])
		b.data = tmp.([]T)

	case BlockTypeUint64:
		if b.data == nil || cap(b.data) < sz {
			arena.Free(typ, b.data)
			b.data = arena.Alloc(typ, sz).([]T)
		}
		tmp, err = decodeUint64Block(buf, interface{}(b.data).([]uint64)[:0])
		b.data = tmp.([]T)

	case BlockTypeUint32:
		if b.data == nil || cap(b.data) < sz {
			arena.Free(typ, b.data)
			b.data = arena.Alloc(typ, sz).([]T)
		}
		tmp, err = decodeUint32Block(buf, interface{}(b.data).([]uint32)[:0])
		b.data = tmp.([]T)

	case BlockTypeUint16:
		if b.data == nil || cap(b.data) < sz {
			arena.Free(typ, b.data)
			b.data = arena.Alloc(typ, sz).([]T)
		}
		tmp, err = decodeUint16Block(buf, interface{}(b.data).([]uint16)[:0])
		b.data = tmp.([]T)

	case BlockTypeUint8:
		if b.data == nil || cap(b.data) < sz {
			arena.Free(typ, b.data)
			b.data = arena.Alloc(typ, sz).([]T)
		}
		tmp, err = decodeUint8Block(buf, interface{}(b.data).([]uint8)[:0])
		b.data = tmp.([]T)

	default:
		err = fmt.Errorf("block: invalid data type %s (%[1]d)", typ)
	}
	return err
}

func (b *BlockTime) MinMax() (interface{}, interface{}) {
	min, max := vec.Int64.MinMax(b.data)
	return time.Unix(0, min).UTC(), time.Unix(0, max).UTC()
}

func (b *BlockNum[T]) MinMax() (interface{}, interface{}) {
	switch b.Type() {
	case BlockTypeFloat64:
		return vec.Float64.MinMax(interface{}(b.data).([]float64))
	case BlockTypeFloat32:
		return vec.Float32.MinMax(interface{}(b.data).([]float32))
	case BlockTypeInt64:
		return vec.Int64.MinMax(interface{}(b.data).([]int64))
	case BlockTypeInt32:
		return vec.Int32.MinMax(interface{}(b.data).([]int32))
	case BlockTypeInt16:
		return vec.Int16.MinMax(interface{}(b.data).([]int16))
	case BlockTypeInt8:
		return vec.Int8.MinMax(interface{}(b.data).([]int8))
	case BlockTypeUint64:
		return vec.Uint64.MinMax(interface{}(b.data).([]uint64))
	case BlockTypeUint32:
		return vec.Uint32.MinMax(interface{}(b.data).([]uint32))
	case BlockTypeUint16:
		return vec.Uint16.MinMax(interface{}(b.data).([]uint16))
	case BlockTypeUint8:
		return vec.Uint8.MinMax(interface{}(b.data).([]uint8))
	}
	return nil, nil
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
	return b.data[i] < b.data[j]
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
	b.data[i], b.data[j] = b.data[j], b.data[i]
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
	var buf [8]byte
	switch b.Type() {
	case BlockTypeFloat64:
		for i, v := range b.data {
			bigEndian.PutUint64(buf[:], math.Float64bits(float64(v)))
			res[i] = xxhash.Sum64(buf[:])
		}
	case BlockTypeFloat32:
		for i, v := range b.data {
			bigEndian.PutUint32(buf[:], math.Float32bits(float32(v)))
			res[i] = xxhash.Sum64(buf[:4])
		}
	case BlockTypeInt64:
		for i, v := range b.data {
			bigEndian.PutUint64(buf[:], uint64(v))
			res[i] = xxhash.Sum64(buf[:])
		}
	case BlockTypeInt32:
		for i, v := range b.data {
			bigEndian.PutUint32(buf[:], uint32(v))
			res[i] = xxhash.Sum64(buf[:4])
		}
	case BlockTypeInt16:
		for i, v := range b.data {
			bigEndian.PutUint16(buf[:], uint16(v))
			res[i] = xxhash.Sum64(buf[:2])
		}
	case BlockTypeInt8:
		for i, v := range b.data {
			res[i] = xxhash.Sum64([]byte{uint8(v)})
		}
	case BlockTypeUint64:
		for i, v := range b.data {
			bigEndian.PutUint64(buf[:], uint64(v))
			res[i] = xxhash.Sum64(buf[:])
		}
	case BlockTypeUint32:
		for i, v := range b.data {
			bigEndian.PutUint32(buf[:], uint32(v))
			res[i] = xxhash.Sum64(buf[:4])
		}
	case BlockTypeUint16:
		for i, v := range b.data {
			bigEndian.PutUint16(buf[:], uint16(v))
			res[i] = xxhash.Sum64(buf[:2])
		}
	case BlockTypeUint8:
		for i, v := range b.data {
			res[i] = xxhash.Sum64([]byte{byte(v)})
		}
	}
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
	return vec.MatchInt64Equal(b.data, val.(time.Time).UnixNano(), bits, mask)
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
	switch b.Type() {
	case BlockTypeFloat64:
		return vec.MatchFloat64Equal(interface{}(b.data).([]float64), val.(float64), bits, mask)
	case BlockTypeFloat32:
		return vec.MatchFloat32Equal(interface{}(b.data).([]float32), val.(float32), bits, mask)
	case BlockTypeInt64:
		return vec.MatchInt64Equal(interface{}(b.data).([]int64), val.(int64), bits, mask)
	case BlockTypeInt32:
		return vec.MatchInt32Equal(interface{}(b.data).([]int32), val.(int32), bits, mask)
	case BlockTypeInt16:
		return vec.MatchInt16Equal(interface{}(b.data).([]int16), val.(int16), bits, mask)
	case BlockTypeInt8:
		return vec.MatchInt8Equal(interface{}(b.data).([]int8), val.(int8), bits, mask)
	case BlockTypeUint64:
		return vec.MatchUint64Equal(interface{}(b.data).([]uint64), val.(uint64), bits, mask)
	case BlockTypeUint32:
		return vec.MatchUint32Equal(interface{}(b.data).([]uint32), val.(uint32), bits, mask)
	case BlockTypeUint16:
		return vec.MatchUint16Equal(interface{}(b.data).([]uint16), val.(uint16), bits, mask)
	case BlockTypeUint8:
		return vec.MatchUint8Equal(interface{}(b.data).([]uint8), val.(uint8), bits, mask)
	}
	return nil
}

func (b *BlockBytes) MatchNotEqual(val interface{}, bits, mask *vec.Bitset) *vec.Bitset {
	return b.data.MatchNotEqual(val.([]byte), bits, mask)
}

func (b *BlockString) MatchNotEqual(val interface{}, bits, mask *vec.Bitset) *vec.Bitset {
	return b.data.MatchNotEqual([]byte(val.(string)), bits, mask)
}

func (b *BlockTime) MatchNotEqual(val interface{}, bits, mask *vec.Bitset) *vec.Bitset {
	return vec.MatchInt64NotEqual(b.data, val.(time.Time).UnixNano(), bits, mask)
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
	switch b.Type() {
	case BlockTypeFloat64:
		return vec.MatchFloat64NotEqual(interface{}(b.data).([]float64), val.(float64), bits, mask)
	case BlockTypeFloat32:
		return vec.MatchFloat32NotEqual(interface{}(b.data).([]float32), val.(float32), bits, mask)
	case BlockTypeInt64:
		return vec.MatchInt64NotEqual(interface{}(b.data).([]int64), val.(int64), bits, mask)
	case BlockTypeInt32:
		return vec.MatchInt32NotEqual(interface{}(b.data).([]int32), val.(int32), bits, mask)
	case BlockTypeInt16:
		return vec.MatchInt16NotEqual(interface{}(b.data).([]int16), val.(int16), bits, mask)
	case BlockTypeInt8:
		return vec.MatchInt8NotEqual(interface{}(b.data).([]int8), val.(int8), bits, mask)
	case BlockTypeUint64:
		return vec.MatchUint64NotEqual(interface{}(b.data).([]uint64), val.(uint64), bits, mask)
	case BlockTypeUint32:
		return vec.MatchUint32NotEqual(interface{}(b.data).([]uint32), val.(uint32), bits, mask)
	case BlockTypeUint16:
		return vec.MatchUint16NotEqual(interface{}(b.data).([]uint16), val.(uint16), bits, mask)
	case BlockTypeUint8:
		return vec.MatchUint8NotEqual(interface{}(b.data).([]uint8), val.(uint8), bits, mask)
	}
	return nil
}

func (b *BlockBytes) MatchGreaterThan(val interface{}, bits, mask *vec.Bitset) *vec.Bitset {
	return b.data.MatchGreaterThan(val.([]byte), bits, mask)
}

func (b *BlockString) MatchGreaterThan(val interface{}, bits, mask *vec.Bitset) *vec.Bitset {
	return b.data.MatchGreaterThan([]byte(val.(string)), bits, mask)
}

func (b *BlockTime) MatchGreaterThan(val interface{}, bits, mask *vec.Bitset) *vec.Bitset {
	return vec.MatchInt64GreaterThan(b.data, val.(time.Time).UnixNano(), bits, mask)
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
	switch b.Type() {
	case BlockTypeFloat64:
		return vec.MatchFloat64GreaterThan(interface{}(b.data).([]float64), val.(float64), bits, mask)
	case BlockTypeFloat32:
		return vec.MatchFloat32GreaterThan(interface{}(b.data).([]float32), val.(float32), bits, mask)
	case BlockTypeInt64:
		return vec.MatchInt64GreaterThan(interface{}(b.data).([]int64), val.(int64), bits, mask)
	case BlockTypeInt32:
		return vec.MatchInt32GreaterThan(interface{}(b.data).([]int32), val.(int32), bits, mask)
	case BlockTypeInt16:
		return vec.MatchInt16GreaterThan(interface{}(b.data).([]int16), val.(int16), bits, mask)
	case BlockTypeInt8:
		return vec.MatchInt8GreaterThan(interface{}(b.data).([]int8), val.(int8), bits, mask)
	case BlockTypeUint64:
		return vec.MatchUint64GreaterThan(interface{}(b.data).([]uint64), val.(uint64), bits, mask)
	case BlockTypeUint32:
		return vec.MatchUint32GreaterThan(interface{}(b.data).([]uint32), val.(uint32), bits, mask)
	case BlockTypeUint16:
		return vec.MatchUint16GreaterThan(interface{}(b.data).([]uint16), val.(uint16), bits, mask)
	case BlockTypeUint8:
		return vec.MatchUint8GreaterThan(interface{}(b.data).([]uint8), val.(uint8), bits, mask)
	}
	return nil
}

func (b *BlockBytes) MatchGreaterThanEqual(val interface{}, bits, mask *vec.Bitset) *vec.Bitset {
	return b.data.MatchGreaterThanEqual(val.([]byte), bits, mask)
}

func (b *BlockString) MatchGreaterThanEqual(val interface{}, bits, mask *vec.Bitset) *vec.Bitset {
	return b.data.MatchGreaterThanEqual([]byte(val.(string)), bits, mask)
}

func (b *BlockTime) MatchGreaterThanEqual(val interface{}, bits, mask *vec.Bitset) *vec.Bitset {
	return vec.MatchInt64GreaterThanEqual(b.data, val.(time.Time).UnixNano(), bits, mask)
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
	switch b.Type() {
	case BlockTypeFloat64:
		return vec.MatchFloat64GreaterThanEqual(interface{}(b.data).([]float64), val.(float64), bits, mask)
	case BlockTypeFloat32:
		return vec.MatchFloat32GreaterThanEqual(interface{}(b.data).([]float32), val.(float32), bits, mask)
	case BlockTypeInt64:
		return vec.MatchInt64GreaterThanEqual(interface{}(b.data).([]int64), val.(int64), bits, mask)
	case BlockTypeInt32:
		return vec.MatchInt32GreaterThanEqual(interface{}(b.data).([]int32), val.(int32), bits, mask)
	case BlockTypeInt16:
		return vec.MatchInt16GreaterThanEqual(interface{}(b.data).([]int16), val.(int16), bits, mask)
	case BlockTypeInt8:
		return vec.MatchInt8GreaterThanEqual(interface{}(b.data).([]int8), val.(int8), bits, mask)
	case BlockTypeUint64:
		return vec.MatchUint64GreaterThanEqual(interface{}(b.data).([]uint64), val.(uint64), bits, mask)
	case BlockTypeUint32:
		return vec.MatchUint32GreaterThanEqual(interface{}(b.data).([]uint32), val.(uint32), bits, mask)
	case BlockTypeUint16:
		return vec.MatchUint16GreaterThanEqual(interface{}(b.data).([]uint16), val.(uint16), bits, mask)
	case BlockTypeUint8:
		return vec.MatchUint8GreaterThanEqual(interface{}(b.data).([]uint8), val.(uint8), bits, mask)
	}
	return nil
}

func (b *BlockBytes) MatchLessThan(val interface{}, bits, mask *vec.Bitset) *vec.Bitset {
	return b.data.MatchLessThan(val.([]byte), bits, mask)
}

func (b *BlockString) MatchLessThan(val interface{}, bits, mask *vec.Bitset) *vec.Bitset {
	return b.data.MatchLessThan([]byte(val.(string)), bits, mask)
}

func (b *BlockTime) MatchLessThan(val interface{}, bits, mask *vec.Bitset) *vec.Bitset {
	return vec.MatchInt64LessThan(b.data, val.(time.Time).UnixNano(), bits, mask)
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
	switch b.Type() {
	case BlockTypeFloat64:
		return vec.MatchFloat64LessThan(interface{}(b.data).([]float64), val.(float64), bits, mask)
	case BlockTypeFloat32:
		return vec.MatchFloat32LessThan(interface{}(b.data).([]float32), val.(float32), bits, mask)
	case BlockTypeInt64:
		return vec.MatchInt64LessThan(interface{}(b.data).([]int64), val.(int64), bits, mask)
	case BlockTypeInt32:
		return vec.MatchInt32LessThan(interface{}(b.data).([]int32), val.(int32), bits, mask)
	case BlockTypeInt16:
		return vec.MatchInt16LessThan(interface{}(b.data).([]int16), val.(int16), bits, mask)
	case BlockTypeInt8:
		return vec.MatchInt8LessThan(interface{}(b.data).([]int8), val.(int8), bits, mask)
	case BlockTypeUint64:
		return vec.MatchUint64LessThan(interface{}(b.data).([]uint64), val.(uint64), bits, mask)
	case BlockTypeUint32:
		return vec.MatchUint32LessThan(interface{}(b.data).([]uint32), val.(uint32), bits, mask)
	case BlockTypeUint16:
		return vec.MatchUint16LessThan(interface{}(b.data).([]uint16), val.(uint16), bits, mask)
	case BlockTypeUint8:
		return vec.MatchUint8LessThan(interface{}(b.data).([]uint8), val.(uint8), bits, mask)
	}
	return nil
}

func (b *BlockBytes) MatchLessThanEqual(val interface{}, bits, mask *vec.Bitset) *vec.Bitset {
	return b.data.MatchLessThanEqual(val.([]byte), bits, mask)
}

func (b *BlockString) MatchLessThanEqual(val interface{}, bits, mask *vec.Bitset) *vec.Bitset {
	return b.data.MatchLessThanEqual([]byte(val.(string)), bits, mask)
}

func (b *BlockTime) MatchLessThanEqual(val interface{}, bits, mask *vec.Bitset) *vec.Bitset {
	return vec.MatchInt64LessThanEqual(b.data, val.(time.Time).UnixNano(), bits, mask)
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
	switch b.Type() {
	case BlockTypeFloat64:
		return vec.MatchFloat64LessThanEqual(interface{}(b.data).([]float64), val.(float64), bits, mask)
	case BlockTypeFloat32:
		return vec.MatchFloat32LessThanEqual(interface{}(b.data).([]float32), val.(float32), bits, mask)
	case BlockTypeInt64:
		return vec.MatchInt64LessThanEqual(interface{}(b.data).([]int64), val.(int64), bits, mask)
	case BlockTypeInt32:
		return vec.MatchInt32LessThanEqual(interface{}(b.data).([]int32), val.(int32), bits, mask)
	case BlockTypeInt16:
		return vec.MatchInt16LessThanEqual(interface{}(b.data).([]int16), val.(int16), bits, mask)
	case BlockTypeInt8:
		return vec.MatchInt8LessThanEqual(interface{}(b.data).([]int8), val.(int8), bits, mask)
	case BlockTypeUint64:
		return vec.MatchUint64LessThanEqual(interface{}(b.data).([]uint64), val.(uint64), bits, mask)
	case BlockTypeUint32:
		return vec.MatchUint32LessThanEqual(interface{}(b.data).([]uint32), val.(uint32), bits, mask)
	case BlockTypeUint16:
		return vec.MatchUint16LessThanEqual(interface{}(b.data).([]uint16), val.(uint16), bits, mask)
	case BlockTypeUint8:
		return vec.MatchUint8LessThanEqual(interface{}(b.data).([]uint8), val.(uint8), bits, mask)
	}
	return nil
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
	return vec.MatchInt64Between(b.data, from.(time.Time).UnixNano(), to.(time.Time).UnixNano(), bits, mask)
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
	switch b.Type() {
	case BlockTypeFloat64:
		return vec.MatchFloat64Between(interface{}(b.data).([]float64), from.(float64), to.(float64), bits, mask)
	case BlockTypeFloat32:
		return vec.MatchFloat32Between(interface{}(b.data).([]float32), from.(float32), to.(float32), bits, mask)
	case BlockTypeInt64:
		return vec.MatchInt64Between(interface{}(b.data).([]int64), from.(int64), to.(int64), bits, mask)
	case BlockTypeInt32:
		return vec.MatchInt32Between(interface{}(b.data).([]int32), from.(int32), to.(int32), bits, mask)
	case BlockTypeInt16:
		return vec.MatchInt16Between(interface{}(b.data).([]int16), from.(int16), to.(int16), bits, mask)
	case BlockTypeInt8:
		return vec.MatchInt8Between(interface{}(b.data).([]int8), from.(int8), to.(int8), bits, mask)
	case BlockTypeUint64:
		return vec.MatchUint64Between(interface{}(b.data).([]uint64), from.(uint64), to.(uint64), bits, mask)
	case BlockTypeUint32:
		return vec.MatchUint32Between(interface{}(b.data).([]uint32), from.(uint32), to.(uint32), bits, mask)
	case BlockTypeUint16:
		return vec.MatchUint16Between(interface{}(b.data).([]uint16), from.(uint16), to.(uint16), bits, mask)
	case BlockTypeUint8:
		return vec.MatchUint8Between(interface{}(b.data).([]uint8), from.(uint8), to.(uint8), bits, mask)
	}
	return nil
}

// FIXME: Dump should not be part of Block interface
func (b *blockCommon) Dump() []byte {
	return []byte{}
}

func (b *BlockBytes) Dump() []byte {
	return []byte(b.data.Dump())
}
