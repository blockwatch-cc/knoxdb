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

	"blockwatch.cc/knoxdb/encoding/compress"
	"blockwatch.cc/knoxdb/encoding/dedup"
	"blockwatch.cc/knoxdb/hash/xxhash"
	"blockwatch.cc/knoxdb/vec"
)

var bigEndian = binary.BigEndian

var BlockSz = int(reflect.TypeOf(Block{}).Size())

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
	BlockTime    = BlockType(0)
	BlockInt64   = BlockType(1)
	BlockUint64  = BlockType(2)
	BlockFloat64 = BlockType(3)
	BlockBool    = BlockType(4)
	BlockString  = BlockType(5)
	BlockBytes   = BlockType(6)
	BlockInt32   = BlockType(7)
	BlockInt16   = BlockType(8)
	BlockInt8    = BlockType(9)
	BlockUint32  = BlockType(10)
	BlockUint16  = BlockType(11)
	BlockUint8   = BlockType(12)
	BlockFloat32 = BlockType(13)
	BlockInt128  = BlockType(14)
	BlockInt256  = BlockType(15)
)

func (t BlockType) IsValid() bool {
	return t <= BlockInt256
}

func (t BlockType) String() string {
	switch t {
	case BlockTime:
		return "time"
	case BlockInt64:
		return "int64"
	case BlockInt32:
		return "int32"
	case BlockInt16:
		return "int16"
	case BlockInt8:
		return "int8"
	case BlockUint64:
		return "uint64"
	case BlockUint32:
		return "uint32"
	case BlockUint16:
		return "uint16"
	case BlockUint8:
		return "uint8"
	case BlockFloat64:
		return "float64"
	case BlockFloat32:
		return "float32"
	case BlockBool:
		return "bool"
	case BlockString:
		return "string"
	case BlockBytes:
		return "bytes"
	case BlockInt128:
		return "int128"
	case BlockInt256:
		return "int256"
	default:
		return "invalid block type"
	}
}

type Block struct {
	refCount int64
	typ      BlockType
	comp     Compression
	ignore   bool
	dirty    bool
	size     int // stored size, debug data

	// TODO: measure performance impact of using an interface instead of direct slices
	//       this can save up to 15x storage for slice headers / pointers
	//       but adds another level of indirection on each data access
	// data interface{}
	Bytes   dedup.ByteArray // re-used for bytes and strings
	Bits    *vec.Bitset     // -> Bitset
	Int64   []int64         // re-used by Decimal64, Timestamps
	Int32   []int32         // re-used by Decimal32
	Int16   []int16
	Int8    []int8
	Uint64  []uint64
	Uint32  []uint32
	Uint16  []uint16
	Uint8   []uint8
	Float64 []float64
	Float32 []float32
	Int128  vec.Int128LLSlice // re-used by Decimal128, Int128
	Int256  vec.Int256LLSlice // re-used by Decimal256, Int256
}

func (b *Block) IncRef() int64 {
	return atomic.AddInt64(&b.refCount, 1)
}

func (b *Block) DecRef() int64 {
	val := atomic.AddInt64(&b.refCount, -1)
	if val == 0 {
		b.Release()
	}
	return val
}

func (b Block) Type() BlockType {
	return b.typ
}

func (b *Block) IsInt() bool {
	switch b.Type() {
	case BlockInt64, BlockInt32, BlockInt16, BlockInt8,
		BlockUint64, BlockUint32, BlockUint16, BlockUint8:
		return true
	default:
		return false
	}
}

func (b *Block) IsSint() bool {
	switch b.Type() {
	case BlockInt64, BlockInt32, BlockInt16, BlockInt8:
		return true
	default:
		return false
	}
}

func (b *Block) IsUint() bool {
	switch b.Type() {
	case BlockUint64, BlockUint32, BlockUint16, BlockUint8:
		return true
	default:
		return false
	}
}

func (b *Block) IsFloat() bool {
	switch b.Type() {
	case BlockFloat64, BlockFloat32:
		return true
	default:
		return false
	}
}

func (b Block) Compression() Compression {
	return b.comp
}

func (b Block) CompressedSize() int {
	return b.size
}

func (b *Block) IsIgnore() bool {
	return b.ignore
}

func (b *Block) IsDirty() bool {
	return b.dirty
}

func (b *Block) SetDirty() {
	b.dirty = true
}

func (b *Block) SetIgnore() {
	b.Clear()
	b.ignore = true
}

func (b *Block) SetCompression(c Compression) {
	b.comp = c
}

func (b Block) RawSlice() interface{} {
	switch b.typ {
	case BlockInt64, BlockTime:
		return b.Int64
	case BlockFloat64:
		return b.Float64
	case BlockFloat32:
		return b.Float32
	case BlockInt32:
		return b.Int32
	case BlockInt16:
		return b.Int16
	case BlockInt8:
		return b.Int8
	case BlockUint64:
		return b.Uint64
	case BlockUint32:
		return b.Uint32
	case BlockUint16:
		return b.Uint16
	case BlockUint8:
		return b.Uint8
	case BlockBool:
		return b.Bits.Slice()
	case BlockString:
		s := make([]string, b.Bytes.Len())
		for i, v := range b.Bytes.Slice() {
			s[i] = compress.UnsafeGetString(v)
		}
		return s
	case BlockBytes:
		return b.Bytes.Slice()
	case BlockInt128:
		return b.Int128.Int128Slice()
	case BlockInt256:
		return b.Int256.Int256Slice()
	default:
		return nil
	}
}

func (b Block) RangeSlice(start, end int) interface{} {
	switch b.typ {
	case BlockInt64, BlockTime:
		return b.Int64[start:end]
	case BlockFloat64:
		return b.Float64[start:end]
	case BlockFloat32:
		return b.Float32[start:end]
	case BlockInt32:
		return b.Int32[start:end]
	case BlockInt16:
		return b.Int16[start:end]
	case BlockInt8:
		return b.Int8[start:end]
	case BlockUint64:
		return b.Uint64[start:end]
	case BlockUint32:
		return b.Uint32[start:end]
	case BlockUint16:
		return b.Uint16[start:end]
	case BlockUint8:
		return b.Uint8[start:end]
	case BlockBool:
		return b.Bits.SubSlice(start, end-start+1)
	case BlockString:
		s := make([]string, end-start+1)
		for i, v := range b.Bytes.Subslice(start, end) {
			s[i] = compress.UnsafeGetString(v)
		}
		return s
	case BlockBytes:
		return b.Bytes.Subslice(start, end)
	case BlockInt128:
		return b.Int128.Subslice(start, end).Int128Slice()
	case BlockInt256:
		return b.Int256.Subslice(start, end).Int256Slice()
	default:
		return nil
	}
}

func (b Block) Elem(idx int) interface{} {
	if idx >= b.Len() {
		return nil
	}
	switch b.typ {
	case BlockInt64, BlockTime:
		return b.Int64[idx]
	case BlockFloat64:
		return b.Float64[idx]
	case BlockFloat32:
		return b.Float32[idx]
	case BlockInt32:
		return b.Int32[idx]
	case BlockInt16:
		return b.Int16[idx]
	case BlockInt8:
		return b.Int8[idx]
	case BlockUint64:
		return b.Uint64[idx]
	case BlockUint32:
		return b.Uint32[idx]
	case BlockUint16:
		return b.Uint16[idx]
	case BlockUint8:
		return b.Uint8[idx]
	case BlockBool:
		return b.Bits.IsSet(idx)
	case BlockString:
		return compress.UnsafeGetString(b.Bytes.Elem(idx))
	case BlockBytes:
		return b.Bytes.Elem(idx)
	case BlockInt128:
		return b.Int128.Elem(idx)
	case BlockInt256:
		return b.Int256.Elem(idx)
	default:
		return nil
	}
}

func AllocBlock() *Block {
	return BlockPool.Get().(*Block)
}

func NewBlock(typ BlockType, comp Compression, sz int) *Block {
	b := BlockPool.Get().(*Block)
	b.typ = typ
	b.comp = comp
	b.dirty = true
	switch typ {
	case BlockTime, BlockInt64:
		b.Int64 = arena.Alloc(typ, sz).([]int64)
	case BlockFloat64:
		b.Float64 = arena.Alloc(typ, sz).([]float64)
	case BlockFloat32:
		b.Float32 = arena.Alloc(typ, sz).([]float32)
	case BlockInt32:
		b.Int32 = arena.Alloc(typ, sz).([]int32)
	case BlockInt16:
		b.Int16 = arena.Alloc(typ, sz).([]int16)
	case BlockInt8:
		b.Int8 = arena.Alloc(typ, sz).([]int8)
	case BlockUint64:
		b.Uint64 = arena.Alloc(typ, sz).([]uint64)
	case BlockUint32:
		b.Uint32 = arena.Alloc(typ, sz).([]uint32)
	case BlockUint16:
		b.Uint16 = arena.Alloc(typ, sz).([]uint16)
	case BlockUint8:
		b.Uint8 = arena.Alloc(typ, sz).([]uint8)
	case BlockBool:
		// b.Bits = arena.Alloc(typ, sz).(*vec.Bitset)
		b.Bits = vec.NewBitset(sz).Reset()
	case BlockString, BlockBytes:
		// b.Bytes = arena.Alloc(typ, sz).(dedup.ByteArray)
		b.Bytes = dedup.NewByteArray(sz)
	case BlockInt128:
		b.Int128.X0 = arena.Alloc(BlockInt64, sz).([]int64)
		b.Int128.X1 = arena.Alloc(BlockUint64, sz).([]uint64)
	case BlockInt256:
		b.Int256.X0 = arena.Alloc(BlockInt64, sz).([]int64)
		b.Int256.X1 = arena.Alloc(BlockUint64, sz).([]uint64)
		b.Int256.X2 = arena.Alloc(BlockUint64, sz).([]uint64)
		b.Int256.X3 = arena.Alloc(BlockUint64, sz).([]uint64)
	}
	return b
}

func (b *Block) Copy(src *Block) {
	if src == nil || b.typ != src.typ {
		return
	}
	b.size = src.size
	b.dirty = true
	switch b.typ {
	case BlockInt64, BlockTime:
		b.Int64 = b.Int64[:len(src.Int64)]
		copy(b.Int64, src.Int64)
	case BlockFloat64:
		b.Float64 = b.Float64[:len(src.Float64)]
		copy(b.Float64, src.Float64)
	case BlockFloat32:
		b.Float32 = b.Float32[:len(src.Float32)]
		copy(b.Float32, src.Float32)
	case BlockInt32:
		b.Int32 = b.Int32[:len(src.Int32)]
		copy(b.Int32, src.Int32)
	case BlockInt16:
		b.Int16 = b.Int16[:len(src.Int16)]
		copy(b.Int16, src.Int16)
	case BlockInt8:
		b.Int8 = b.Int8[:len(src.Int8)]
		copy(b.Int8, src.Int8)
	case BlockUint64:
		b.Uint64 = b.Uint64[:len(src.Uint64)]
		copy(b.Uint64, src.Uint64)
	case BlockUint32:
		b.Uint32 = b.Uint32[:len(src.Uint32)]
		copy(b.Uint32, src.Uint32)
	case BlockUint16:
		b.Uint16 = b.Uint16[:len(src.Uint16)]
		copy(b.Uint16, src.Uint16)
	case BlockUint8:
		b.Uint8 = b.Uint8[:len(src.Uint8)]
		copy(b.Uint8, src.Uint8)
	case BlockBool:
		b.Bits = vec.NewBitsetFromBytes(src.Bits.Bytes(), src.Bits.Len())
	case BlockString, BlockBytes:
		b.Bytes = dedup.NewByteArray(src.Bytes.Len())
		b.Bytes.AppendFrom(src.Bytes)
	case BlockInt128:
		sz := len(b.Int128.X0)
		b.Int128.X0 = b.Int128.X0[:sz]
		copy(b.Int128.X0, src.Int128.X0)
		b.Int128.X1 = b.Int128.X1[:sz]
		copy(b.Int128.X1, src.Int128.X1)
	case BlockInt256:
		sz := len(b.Int256.X0)
		b.Int256.X0 = b.Int256.X0[:sz]
		copy(b.Int256.X0, src.Int256.X0)
		b.Int256.X1 = b.Int256.X1[:sz]
		copy(b.Int256.X1, src.Int256.X1)
		b.Int256.X2 = b.Int256.X2[:sz]
		copy(b.Int256.X2, src.Int256.X2)
		b.Int256.X3 = b.Int256.X3[:sz]
		copy(b.Int256.X3, src.Int256.X3)
	}
}

func (b *Block) Len() int {
	switch b.typ {
	case BlockFloat64:
		return len(b.Float64)
	case BlockFloat32:
		return len(b.Float32)
	case BlockInt64, BlockTime:
		return len(b.Int64)
	case BlockInt32:
		return len(b.Int32)
	case BlockInt16:
		return len(b.Int16)
	case BlockInt8:
		return len(b.Int8)
	case BlockUint64:
		return len(b.Uint64)
	case BlockUint32:
		return len(b.Uint32)
	case BlockUint16:
		return len(b.Uint16)
	case BlockUint8:
		return len(b.Uint8)
	case BlockBool:
		return b.Bits.Len()
	case BlockString, BlockBytes:
		return b.Bytes.Len()
	case BlockInt128:
		return b.Int128.Len()
	case BlockInt256:
		return b.Int256.Len()
	default:
		return 0
	}
}

func (b *Block) Cap() int {
	switch b.typ {
	case BlockFloat64:
		return cap(b.Float64)
	case BlockFloat32:
		return cap(b.Float32)
	case BlockInt64, BlockTime:
		return cap(b.Int64)
	case BlockInt32:
		return cap(b.Int32)
	case BlockInt16:
		return cap(b.Int16)
	case BlockInt8:
		return cap(b.Int8)
	case BlockUint64:
		return cap(b.Uint64)
	case BlockUint32:
		return cap(b.Uint32)
	case BlockUint16:
		return cap(b.Uint16)
	case BlockUint8:
		return cap(b.Uint8)
	case BlockBool:
		return b.Bits.Cap()
	case BlockString, BlockBytes:
		return b.Bytes.Cap()
	case BlockInt128:
		return b.Int128.Cap()
	case BlockInt256:
		return b.Int256.Cap()
	default:
		return 0
	}
}

// Estimate the upper bound of the space required to store a serialization
// of this block. The true size may be smaller due to efficient type-based
// compression and generic subsequent block compression.
//
// This size hint is used to properly dimension the encoer/decoder buffers
// as is required by LZ4 and to avoid memcopy during write.
func (b *Block) MaxStoredSize() int {
	if b.ignore {
		return 0
	}
	var sz int
	switch b.typ {
	case BlockFloat64:
		sz = compress.Float64ArrayEncodedSize(b.Float64)
	case BlockFloat32:
		sz = compress.Float32ArrayEncodedSize(b.Float32)
	case BlockInt64, BlockTime:
		sz = compress.Int64ArrayEncodedSize(b.Int64)
	case BlockInt32:
		sz = compress.Int32ArrayEncodedSize(b.Int32)
	case BlockInt16:
		sz = compress.Int16ArrayEncodedSize(b.Int16)
	case BlockInt8:
		sz = compress.Int8ArrayEncodedSize(b.Int8)
	case BlockUint64:
		sz = compress.Uint64ArrayEncodedSize(b.Uint64)
	case BlockUint32:
		sz = compress.Uint32ArrayEncodedSize(b.Uint32)
	case BlockUint16:
		sz = compress.Uint16ArrayEncodedSize(b.Uint16)
	case BlockUint8:
		sz = compress.Uint8ArrayEncodedSize(b.Uint8)
	case BlockBool:
		sz = compress.BitsetEncodedSize(b.Bits)
	case BlockString, BlockBytes:
		sz = b.Bytes.MaxEncodedSize()
	case BlockInt128:
		sz = compress.Int128ArrayEncodedSize(b.Int128)
	case BlockInt256:
		sz = compress.Int256ArrayEncodedSize(b.Int256)
	}
	return sz + storedBlockHeaderSize + b.comp.HeaderSize(sz)
}

func (b *Block) HeapSize() int {
	if b.ignore {
		return 0
	}
	sz := BlockSz
	switch b.typ {
	case BlockFloat64:
		sz += len(b.Float64) * 8
	case BlockFloat32:
		sz += len(b.Float32) * 4
	case BlockInt64, BlockTime:
		sz += len(b.Int64) * 8
	case BlockInt32:
		sz += len(b.Int32) * 4
	case BlockInt16:
		sz += len(b.Int16) * 2
	case BlockInt8:
		sz += len(b.Int8)
	case BlockUint64:
		sz += len(b.Uint64) * 8
	case BlockUint32:
		sz += len(b.Uint32) * 4
	case BlockUint16:
		sz += len(b.Uint16) * 2
	case BlockUint8:
		sz += len(b.Uint8)
	case BlockBool:
		sz += b.Bits.HeapSize()
	case BlockString, BlockBytes:
		sz += b.Bytes.HeapSize()
	case BlockInt128:
		sz += b.Int128.Len() * 16
	case BlockInt256:
		sz += b.Int256.Len() * 32
	}
	return sz
}

func (b *Block) Clear() {
	switch b.typ {
	case BlockInt64, BlockTime:
		b.Int64 = b.Int64[:0]
	case BlockInt32:
		b.Int32 = b.Int32[:0]
	case BlockInt16:
		b.Int16 = b.Int16[:0]
	case BlockInt8:
		b.Int8 = b.Int8[:0]
	case BlockUint64:
		b.Uint64 = b.Uint64[:0]
	case BlockUint32:
		b.Uint32 = b.Uint32[:0]
	case BlockUint16:
		b.Uint16 = b.Uint16[:0]
	case BlockUint8:
		b.Uint8 = b.Uint8[:0]
	case BlockFloat64:
		b.Float64 = b.Float64[:0]
	case BlockFloat32:
		b.Float32 = b.Float32[:0]
	case BlockString, BlockBytes:
		b.Bytes.Clear()
		if !b.Bytes.IsMaterialized() {
			mat := b.Bytes.Materialize()
			b.Bytes.Release()
			b.Bytes = mat
		}
	case BlockBool:
		b.Bits.Reset()
	case BlockInt128:
		b.Int128.X0 = b.Int128.X0[:0]
		b.Int128.X1 = b.Int128.X1[:0]
	case BlockInt256:
		b.Int256.X0 = b.Int256.X0[:0]
		b.Int256.X1 = b.Int256.X1[:0]
		b.Int256.X2 = b.Int256.X2[:0]
		b.Int256.X3 = b.Int256.X3[:0]
	}
	b.dirty = true
	b.size = 0
}

func (b *Block) Release() {
	b.ignore = false
	b.dirty = false
	b.size = 0

	switch b.typ {
	case BlockFloat64:
		arena.Free(b.typ, b.Float64[:0])
		b.Float64 = nil
	case BlockFloat32:
		arena.Free(b.typ, b.Float32[:0])
		b.Float32 = nil
	case BlockInt64, BlockTime:
		arena.Free(b.typ, b.Int64[:0])
		b.Int64 = nil
	case BlockInt32:
		arena.Free(b.typ, b.Int32[:0])
		b.Int32 = nil
	case BlockInt16:
		arena.Free(b.typ, b.Int16[:0])
		b.Int16 = nil
	case BlockInt8:
		arena.Free(b.typ, b.Int8[:0])
		b.Int8 = nil
	case BlockUint64:
		arena.Free(b.typ, b.Uint64[:0])
		b.Uint64 = nil
	case BlockUint32:
		arena.Free(b.typ, b.Uint32[:0])
		b.Uint32 = nil
	case BlockUint16:
		arena.Free(b.typ, b.Uint16[:0])
		b.Uint16 = nil
	case BlockUint8:
		arena.Free(b.typ, b.Uint8[:0])
		b.Uint8 = nil
	case BlockBool:
		b.Bits.Close()
		b.Bits = nil
	case BlockString, BlockBytes:
		b.Bytes.Release()
		b.Bytes = nil
	case BlockInt128:
		arena.Free(BlockInt64, b.Int128.X0[:0])
		arena.Free(BlockUint64, b.Int128.X1[:0])
		b.Int128.X0 = nil
		b.Int128.X1 = nil
	case BlockInt256:
		arena.Free(BlockInt64, b.Int256.X0[:0])
		arena.Free(BlockUint64, b.Int256.X1[:0])
		arena.Free(BlockUint64, b.Int256.X2[:0])
		arena.Free(BlockUint64, b.Int256.X3[:0])
		b.Int256.X0 = nil
		b.Int256.X1 = nil
		b.Int256.X2 = nil
		b.Int256.X3 = nil
	}
	BlockPool.Put(b)
}

func (b *Block) Encode(buf *bytes.Buffer) (int, error) {
	if buf == nil {
		return 0, fmt.Errorf("block: nil buffer while encoding")
	}
	var (
		err error
		n   int
	)

	switch b.typ {
	case BlockTime:
		n, err = encodeTimeBlock(buf, b.Int64, b.Compression())
	case BlockFloat64:
		n, err = encodeFloat64Block(buf, b.Float64, b.Compression())
	case BlockFloat32:
		n, err = encodeFloat32Block(buf, b.Float32, b.Compression())
	case BlockInt64:
		n, err = encodeInt64Block(buf, b.Int64, b.Compression())
	case BlockInt32:
		n, err = encodeInt32Block(buf, b.Int32, b.Compression())
	case BlockInt16:
		n, err = encodeInt16Block(buf, b.Int16, b.Compression())
	case BlockInt8:
		n, err = encodeInt8Block(buf, b.Int8, b.Compression())
	case BlockUint64:
		n, err = encodeUint64Block(buf, b.Uint64, b.Compression())
	case BlockUint32:
		n, err = encodeUint32Block(buf, b.Uint32, b.Compression())
	case BlockUint16:
		n, err = encodeUint16Block(buf, b.Uint16, b.Compression())
	case BlockUint8:
		n, err = encodeUint8Block(buf, b.Uint8, b.Compression())
	case BlockBool:
		n, err = encodeBoolBlock(buf, b.Bits, b.Compression())
	case BlockString:
		n, err = encodeStringBlock(buf, b.Bytes, b.Compression())
	case BlockBytes:
		n, err = encodeBytesBlock(buf, b.Bytes, b.Compression())
	case BlockInt128:
		n, err = encodeInt128Block(buf, b.Int128, b.Compression())
	case BlockInt256:
		n, err = encodeInt256Block(buf, b.Int256, b.Compression())
	default:
		n, err = 0, fmt.Errorf("block: invalid data type %d (%[1]d)", b.typ)
	}
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

func (b *Block) MinMax() (interface{}, interface{}) {
	switch b.typ {
	case BlockTime:
		min, max := vec.Int64.MinMax(b.Int64)
		return time.Unix(0, min).UTC(), time.Unix(0, max).UTC()
	case BlockFloat64:
		return vec.Float64.MinMax(b.Float64)
	case BlockFloat32:
		return vec.Float32.MinMax(b.Float32)
	case BlockInt64:
		return vec.Int64.MinMax(b.Int64)
	case BlockInt32:
		return vec.Int32.MinMax(b.Int32)
	case BlockInt16:
		return vec.Int16.MinMax(b.Int16)
	case BlockInt8:
		return vec.Int8.MinMax(b.Int8)
	case BlockUint64:
		return vec.Uint64.MinMax(b.Uint64)
	case BlockUint32:
		return vec.Uint32.MinMax(b.Uint32)
	case BlockUint16:
		return vec.Uint16.MinMax(b.Uint16)
	case BlockUint8:
		return vec.Uint8.MinMax(b.Uint8)
	case BlockBool:
		if b.Bits.Len() > 0 && b.Bits.Count() > 0 {
			return true, false
		}
		return false, false
	case BlockString:
		min, max := b.Bytes.MinMax()
		return compress.UnsafeGetString(min), compress.UnsafeGetString(max)
	case BlockBytes:
		return b.Bytes.MinMax()
	case BlockInt128:
		return b.Int128.MinMax()
	case BlockInt256:
		return b.Int256.MinMax()
	default:
		return nil, nil
	}
}

func (b *Block) Less(i, j int) bool {
	switch b.typ {
	case BlockInt256:
		return b.Int256.Elem(i).Lt(b.Int256.Elem(j))
	case BlockInt128:
		return b.Int128.Elem(i).Lt(b.Int128.Elem(j))
	case BlockTime, BlockInt64:
		return b.Int64[i] < b.Int64[j]
	case BlockInt32:
		return b.Int32[i] < b.Int32[j]
	case BlockInt16:
		return b.Int16[i] < b.Int16[j]
	case BlockInt8:
		return b.Int8[i] < b.Int8[j]
	case BlockUint64:
		return b.Uint64[i] < b.Uint64[j]
	case BlockUint32:
		return b.Uint32[i] < b.Uint32[j]
	case BlockUint16:
		return b.Uint16[i] < b.Uint16[j]
	case BlockUint8:
		return b.Uint8[i] < b.Uint8[j]
	case BlockFloat64:
		return b.Float64[i] < b.Float64[j]
	case BlockFloat32:
		return b.Float32[i] < b.Float32[j]
	case BlockBool:
		return !b.Bits.IsSet(i) && b.Bits.IsSet(j)
	case BlockString, BlockBytes:
		return bytes.Compare(b.Bytes.Elem(i), b.Bytes.Elem(j)) < 0
	default:
		return false
	}
}

func (b *Block) Swap(i, j int) {
	switch b.typ {
	case BlockBytes, BlockString:
		b.Bytes.Swap(i, j)

	case BlockBool:
		b.Bits.Swap(i, j)

	case BlockFloat64:
		b.Float64[i], b.Float64[j] = b.Float64[j], b.Float64[i]

	case BlockFloat32:
		b.Float32[i], b.Float32[j] = b.Float32[j], b.Float32[i]

	case BlockInt256:
		b.Int256.Swap(i, j)

	case BlockInt128:
		b.Int128.Swap(i, j)

	case BlockInt64, BlockTime:
		b.Int64[i], b.Int64[j] = b.Int64[j], b.Int64[i]

	case BlockInt32:
		b.Int32[i], b.Int32[j] = b.Int32[j], b.Int32[i]

	case BlockInt16:
		b.Int16[i], b.Int16[j] = b.Int16[j], b.Int16[i]

	case BlockInt8:
		b.Int8[i], b.Int8[j] = b.Int8[j], b.Int8[i]

	case BlockUint64:
		b.Uint64[i], b.Uint64[j] = b.Uint64[j], b.Uint64[i]

	case BlockUint32:
		b.Uint32[i], b.Uint32[j] = b.Uint32[j], b.Uint32[i]

	case BlockUint16:
		b.Uint16[i], b.Uint16[j] = b.Uint16[j], b.Uint16[i]

	case BlockUint8:
		b.Uint8[i], b.Uint8[j] = b.Uint8[j], b.Uint8[i]
	}
}

func (b *Block) Hashes(res []uint64) []uint64 {
	sz := b.Len()
	if res == nil || cap(res) < sz {
		res = arena.Alloc(BlockUint64, sz).([]uint64)
	}
	res = res[:sz]
	var buf [8]byte
	switch b.typ {
	case BlockTime:
		for i, v := range b.Int64 {
			bigEndian.PutUint64(buf[:], uint64(v))
			res[i] = xxhash.Sum64(buf[:])
		}
	case BlockFloat64:
		for i, v := range b.Float64 {
			bigEndian.PutUint64(buf[:], math.Float64bits(v))
			res[i] = xxhash.Sum64(buf[:])
		}
	case BlockFloat32:
		for i, v := range b.Float32 {
			bigEndian.PutUint32(buf[:], math.Float32bits(v))
			res[i] = xxhash.Sum64(buf[:4])
		}
	case BlockInt64:
		for i, v := range b.Int64 {
			bigEndian.PutUint64(buf[:], uint64(v))
			res[i] = xxhash.Sum64(buf[:])
		}
	case BlockInt32:
		for i, v := range b.Int32 {
			bigEndian.PutUint32(buf[:], uint32(v))
			res[i] = xxhash.Sum64(buf[:4])
		}
	case BlockInt16:
		for i, v := range b.Int16 {
			bigEndian.PutUint16(buf[:], uint16(v))
			res[i] = xxhash.Sum64(buf[:2])
		}
	case BlockInt8:
		for i, v := range b.Int8 {
			res[i] = xxhash.Sum64([]byte{uint8(v)})
		}
	case BlockUint64:
		for i, v := range b.Uint64 {
			bigEndian.PutUint64(buf[:], v)
			res[i] = xxhash.Sum64(buf[:])
		}
	case BlockUint32:
		for i, v := range b.Uint32 {
			bigEndian.PutUint32(buf[:], v)
			res[i] = xxhash.Sum64(buf[:4])
		}
	case BlockUint16:
		for i, v := range b.Uint16 {
			bigEndian.PutUint16(buf[:], v)
			res[i] = xxhash.Sum64(buf[:2])
		}
	case BlockUint8:
		for i, v := range b.Uint8 {
			res[i] = xxhash.Sum64([]byte{v})
		}
	case BlockBool:
		zero, one := xxhash.Sum64([]byte{0}), xxhash.Sum64([]byte{1})
		for i := 0; i < b.Bits.Len(); i++ {
			if b.Bits.IsSet(i) {
				res[i] = one
			} else {
				res[i] = zero
			}
		}
	case BlockString, BlockBytes:
		for i := 0; i < b.Bytes.Len(); i++ {
			res[i] = xxhash.Sum64(b.Bytes.Elem(i))
		}
	case BlockInt128:
		for i := 0; i < b.Int128.Len(); i++ {
			buf := b.Int128.Elem(i).Bytes16()
			res[i] = xxhash.Sum64(buf[:])
		}
	case BlockInt256:
		for i := 0; i < b.Int256.Len(); i++ {
			buf := b.Int256.Elem(i).Bytes32()
			res[i] = xxhash.Sum64(buf[:])
		}
	}
	return res
}
