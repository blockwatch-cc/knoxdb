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
	IsIgnore() bool
	IsDirty() bool
	Type() BlockType
	SetDirty()
	SetCompression(Compression)
	Release()
	Hashes([]uint64) []uint64
	HeapSize() int
	Copy()
	Compression() Compression
	RawSlice() interface{}
	Clear()
	Len() int
	Cap() int
	MaxStoredSize() int
	Encode(*bytes.Buffer) (int, error)
	MinMax() (interface{}, interface{})
	CompressedSize() int
	Less(int, int) bool
	Swap(int, int)
}

type blockCommon struct {
	refCount int64
	//typ      BlockType
	comp  Compression
	dirty bool
	size  int // stored size, debug data
}

type BlockNum[N Number] struct {
	blockCommon
	//typ  BlockType
	data []N
}

type BlockBytes struct {
	blockCommon
	Data dedup.ByteArray
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

func (b *blockCommon) IsIgnore() bool {
	return b == nil
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

func (b *BlockNum[N]) RawSlice() interface{} {
	return b.data
}

func (b *BlockBool) RawSlice() interface{} {
	return b.data.Slice()
}

func (b *BlockInt128) RawSlice() interface{} {
	return b.data.Int128Slice()
}

func (b *BlockInt256) RawSlice() interface{} {
	return b.data.Int256Slice()
}

func (b *BlockBytes) RawSlice() interface{} {
	return b.data.Slice()
}

func (b *BlockString) RawSlice() interface{} {
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

func (b *BlockString) Elem(idx int) interface{} {
	if idx >= b.Len() {
		return nil
	}
	return compress.UnsafeGetString(b.data.Elem(idx))
}

func (b *BlockBytes) Elem(idx int) interface{} {
	if idx >= b.Len() {
		return nil
	}
	return b.data.Elem(idx)
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

func (b *BlockNum[T]) Copy(src *BlockNum[T]) {
	if src == nil || b.Type() != src.Type() {
		return
	}
	b.size = src.size
	b.dirty = true
	b.data = b.data[:len(src.data)]
	copy(b.data, src.data)
}

func (b *BlockBool) Copy(src *BlockBool) {
	if src == nil || b.Type() != src.Type() {
		return
	}
	b.size = src.size
	b.dirty = true
	b.data = vec.NewBitsetFromBytes(src.data.Bytes(), src.data.Len())
}

func (b *BlockBytes) Copy(src *BlockBytes) {
	if src == nil || b.Type() != src.Type() {
		return
	}
	b.size = src.size
	b.dirty = true
	b.data = dedup.NewByteArray(src.data.Len())
	b.data.AppendFrom(src.data)
}

func (b *BlockInt128) Copy(src *BlockInt128) {
	if src == nil || b.Type() != src.Type() {
		return
	}
	b.size = src.size
	b.dirty = true
	sz := len(b.data.X0)
	b.data.X0 = b.data.X0[:sz]
	copy(b.data.X0, src.data.X0)
	b.data.X1 = b.data.X1[:sz]
	copy(b.data.X1, src.data.X1)
}

func (b *BlockInt256) Copy(src *BlockInt256) {
	if src == nil || b.Type() != src.Type() {
		return
	}
	b.size = src.size
	b.dirty = true
	sz := len(b.data.X0)
	b.data.X0 = b.data.X0[:sz]
	copy(b.data.X0, src.data.X0)
	b.data.X1 = b.data.X1[:sz]
	copy(b.data.X1, src.data.X1)
	b.data.X2 = b.data.X2[:sz]
	copy(b.data.X2, src.data.X2)
	b.data.X3 = b.data.X3[:sz]
	copy(b.data.X3, src.data.X3)
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
	if b.IsIgnore() {
		return 0
	}
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
	if b.IsIgnore() {
		return 0
	}
	var sz int
	sz = compress.BitsetEncodedSize(b.data)
	return sz + storedBlockHeaderSize + b.comp.HeaderSize(sz)
}

func (b *BlockBytes) MaxStoredSize() int {
	if b.IsIgnore() {
		return 0
	}
	var sz int
	sz = b.data.MaxEncodedSize()
	return sz + storedBlockHeaderSize + b.comp.HeaderSize(sz)
}

func (b *BlockInt128) MaxStoredSize() int {
	if b.IsIgnore() {
		return 0
	}
	var sz int
	sz = compress.Int128ArrayEncodedSize(b.data)
	return sz + storedBlockHeaderSize + b.comp.HeaderSize(sz)
}

func (b *BlockInt256) MaxStoredSize() int {
	if b.IsIgnore() {
		return 0
	}
	var sz int
	sz = compress.Int256ArrayEncodedSize(b.data)
	return sz + storedBlockHeaderSize + b.comp.HeaderSize(sz)
}

func (b *BlockNum[T]) HeapSize() int {
	if b.IsIgnore() {
		return 0
	}
	sz := BlockSz
	sz += len(b.data) * int(unsafe.Sizeof(new(T)))
	return sz
}

func (b *BlockBool) HeapSize() int {
	if b.IsIgnore() {
		return 0
	}
	sz := BlockSz
	sz += b.data.HeapSize()
	return sz
}

func (b *BlockBytes) HeapSize() int {
	if b.IsIgnore() {
		return 0
	}
	sz := BlockSz
	sz += b.data.HeapSize()
	return sz
}

func (b *BlockInt128) HeapSize() int {
	if b.IsIgnore() {
		return 0
	}
	sz := BlockSz
	sz += b.data.Len() * 16
	return sz
}

func (b *BlockInt256) HeapSize() int {
	if b.IsIgnore() {
		return 0
	}
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
	if b == nil {
		return
	}
	arena.Free(b.Type(), b.data[:0])
	b.data = nil
}

func (b *BlockBool) Release() {
	if b == nil {
		return
	}
	b.data.Close()
	b.data = nil
}

func (b *BlockBytes) Release() {
	if b == nil {
		return
	}
	b.data.Release()
	b.data = nil
}

func (b *BlockInt128) Release() {
	if b == nil {
		return
	}
	arena.Free(BlockTypeInt64, b.data.X0[:0])
	arena.Free(BlockTypeUint64, b.data.X1[:0])
	b.data.X0 = nil
	b.data.X1 = nil
}

func (b *BlockInt256) Release() {
	if b == nil {
		return
	}
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

func (b *Block) Decode(buf []byte, sz, stored int) error {
	var err error
	b.typ, err = readBlockType(buf)
	if err != nil {
		return err
	}
	b.dirty = false
	b.size = stored

	switch b.typ {
	case BlockTime:
		if b.Int64 == nil || cap(b.Int64) < sz {
			arena.Free(b.typ, b.Int64)
			b.Int64 = arena.Alloc(b.typ, sz).([]int64)
		}
		b.Int64, err = decodeTimeBlock(buf, b.Int64[:0])

	case BlockFloat64:
		if b.Float64 == nil || cap(b.Float64) < sz {
			arena.Free(b.typ, b.Float64)
			b.Float64 = arena.Alloc(b.typ, sz).([]float64)
		}
		b.Float64, err = decodeFloat64Block(buf, b.Float64[:0])

	case BlockFloat32:
		if b.Float32 == nil || cap(b.Float32) < sz {
			arena.Free(b.typ, b.Float32)
			b.Float32 = arena.Alloc(b.typ, sz).([]float32)
		}
		b.Float32, err = decodeFloat32Block(buf, b.Float32[:0])

	case BlockInt64:
		if b.Int64 == nil || cap(b.Int64) < sz {
			arena.Free(b.typ, b.Int64)
			b.Int64 = arena.Alloc(b.typ, sz).([]int64)
		}
		b.Int64, err = decodeInt64Block(buf, b.Int64[:0])

	case BlockInt32:
		if b.Int32 == nil || cap(b.Int32) < sz {
			arena.Free(b.typ, b.Int32)
			b.Int32 = arena.Alloc(b.typ, sz).([]int32)
		}
		b.Int32, err = decodeInt32Block(buf, b.Int32[:0])

	case BlockInt16:
		if b.Int16 == nil || cap(b.Int16) < sz {
			arena.Free(b.typ, b.Int16)
			b.Int16 = arena.Alloc(b.typ, sz).([]int16)
		}
		b.Int16, err = decodeInt16Block(buf, b.Int16[:0])

	case BlockInt8:
		if b.Int8 == nil || cap(b.Int8) < sz {
			arena.Free(b.typ, b.Int8)
			b.Int8 = arena.Alloc(b.typ, sz).([]int8)
		}
		b.Int8, err = decodeInt8Block(buf, b.Int8[:0])

	case BlockUint64:
		if b.Uint64 == nil || cap(b.Uint64) < sz {
			arena.Free(b.typ, b.Uint64)
			b.Uint64 = arena.Alloc(b.typ, sz).([]uint64)
		}
		b.Uint64, err = decodeUint64Block(buf, b.Uint64[:0])

	case BlockUint32:
		if b.Uint32 == nil || cap(b.Uint32) < sz {
			arena.Free(b.typ, b.Uint32)
			b.Uint32 = arena.Alloc(b.typ, sz).([]uint32)
		}
		b.Uint32, err = decodeUint32Block(buf, b.Uint32[:0])

	case BlockUint16:
		if b.Uint16 == nil || cap(b.Uint16) < sz {
			arena.Free(b.typ, b.Uint16)
			b.Uint16 = arena.Alloc(b.typ, sz).([]uint16)
		}
		b.Uint16, err = decodeUint16Block(buf, b.Uint16[:0])

	case BlockUint8:
		if b.Uint8 == nil || cap(b.Uint8) < sz {
			arena.Free(b.typ, b.Uint8)
			b.Uint8 = arena.Alloc(b.typ, sz).([]uint8)
		}
		b.Uint8, err = decodeUint8Block(buf, b.Uint8[:0])

	case BlockBool:
		if b.Bits == nil || b.Bits.Cap() < sz {
			b.Bits = vec.NewBitset(sz)
			b.Bits.Reset()
		} else {
			b.Bits.Grow(sz).Reset()
		}
		b.Bits, err = decodeBoolBlock(buf, b.Bits)

	case BlockString:
		b.Bytes, err = decodeStringBlock(buf, b.Bytes, sz)

	case BlockBytes:
		b.Bytes, err = decodeBytesBlock(buf, b.Bytes, sz)

	case BlockInt128:
		if b.Int128.X0 == nil || cap(b.Int128.X0) < sz {
			arena.Free(b.typ, b.Int128.X0)
			b.Int128.X0 = arena.Alloc(BlockInt64, sz).([]int64)
		}
		if b.Int128.X1 == nil || cap(b.Int128.X1) < sz {
			arena.Free(b.typ, b.Int128.X1)
			b.Int128.X1 = arena.Alloc(BlockUint64, sz).([]uint64)
		}
		b.Int128.X0 = b.Int128.X0[:0]
		b.Int128.X1 = b.Int128.X1[:0]
		b.Int128, err = decodeInt128Block(buf, b.Int128)

	case BlockInt256:
		if b.Int256.X0 == nil || cap(b.Int256.X0) < sz {
			arena.Free(b.typ, b.Int256.X0)
			b.Int256.X0 = arena.Alloc(BlockInt64, sz).([]int64)
		}
		if b.Int256.X1 == nil || cap(b.Int256.X1) < sz {
			arena.Free(b.typ, b.Int256.X1)
			b.Int256.X1 = arena.Alloc(BlockUint64, sz).([]uint64)
		}
		if b.Int256.X2 == nil || cap(b.Int256.X2) < sz {
			arena.Free(b.typ, b.Int256.X2)
			b.Int256.X2 = arena.Alloc(BlockUint64, sz).([]uint64)
		}
		if b.Int256.X3 == nil || cap(b.Int256.X3) < sz {
			arena.Free(b.typ, b.Int256.X3)
			b.Int256.X3 = arena.Alloc(BlockUint64, sz).([]uint64)
		}
		b.Int256.X0 = b.Int256.X0[:0]
		b.Int256.X1 = b.Int256.X1[:0]
		b.Int256.X2 = b.Int256.X2[:0]
		b.Int256.X3 = b.Int256.X3[:0]
		b.Int256, err = decodeInt256Block(buf, b.Int256)

	default:
		err = fmt.Errorf("block: invalid data type %s (%[1]d)", b.typ)
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

func (b *BlockBytes) Bytes() (interface{}, interface{}) {
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
