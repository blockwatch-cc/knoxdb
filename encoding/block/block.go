// Copyright (c) 2018-2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package block

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"time"

	"blockwatch.cc/knoxdb/encoding/compress"
	"blockwatch.cc/knoxdb/vec"
)

var bigEndian = binary.BigEndian

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
	BlockIgnore  = BlockType(255)
)

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
	case BlockIgnore:
		return "ignore"
	default:
		return "invalid block type"
	}
}

type Block struct {
	typ    BlockType
	comp   Compression
	ignore bool
	dirty  bool
	size   int // stored size, debug data

	// TODO: measure performance impact of using an interface instead of direct slices
	//       this can save up to 15x storage for slice headers / pointers
	//       but adds another level of indirection on each data access
	// data interface{}
	Strings []string
	Bytes   [][]byte
	Bits    *vec.Bitset // -> Bitset
	Int64   []int64     // re-used by Decimal64, Timestamps
	Int32   []int32     // re-used by Decimal32
	Int16   []int16
	Int8    []int8
	Uint64  []uint64
	Uint32  []uint32
	Uint16  []uint16
	Uint8   []uint8
	Float64 []float64
	Float32 []float32
	Int128  vec.Int128Slice // re-used by Decimal128, Int128
	Int256  vec.Int256Slice // re-used by Decimal256, Int256
}

func (b Block) Type() BlockType {
	return b.typ
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
	b.ignore = true
	b.Release()
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
		return b.Strings
	case BlockBytes:
		return b.Bytes
	case BlockInt128:
		return b.Int128
	case BlockInt256:
		return b.Int256
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
		return b.Bits.SubSlice(start, end-start)
	case BlockString:
		return b.Strings[start:end]
	case BlockBytes:
		return b.Bytes[start:end]
	case BlockInt128:
		return b.Int128[start:end]
	case BlockInt256:
		return b.Int256[start:end]
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
	case BlockInt64, BlockTime:
		if sz <= DefaultMaxPointsPerBlock {
			b.Int64 = int64Pool.Get().([]int64)
		} else {
			b.Int64 = make([]int64, 0, sz)
		}
	case BlockFloat64:
		if sz <= DefaultMaxPointsPerBlock {
			b.Float64 = float64Pool.Get().([]float64)
		} else {
			b.Float64 = make([]float64, 0, sz)
		}
	case BlockFloat32:
		if sz <= DefaultMaxPointsPerBlock {
			b.Float32 = float32Pool.Get().([]float32)
		} else {
			b.Float32 = make([]float32, 0, sz)
		}
	case BlockInt32:
		if sz <= DefaultMaxPointsPerBlock {
			b.Int32 = int32Pool.Get().([]int32)
		} else {
			b.Int32 = make([]int32, 0, sz)
		}
	case BlockInt16:
		if sz <= DefaultMaxPointsPerBlock {
			b.Int16 = int16Pool.Get().([]int16)
		} else {
			b.Int16 = make([]int16, 0, sz)
		}
	case BlockInt8:
		if sz <= DefaultMaxPointsPerBlock {
			b.Int8 = int8Pool.Get().([]int8)
		} else {
			b.Int8 = make([]int8, 0, sz)
		}
	case BlockUint64:
		if sz <= DefaultMaxPointsPerBlock {
			b.Uint64 = uint64Pool.Get().([]uint64)
		} else {
			b.Uint64 = make([]uint64, 0, sz)
		}
	case BlockUint32:
		if sz <= DefaultMaxPointsPerBlock {
			b.Uint32 = uint32Pool.Get().([]uint32)
		} else {
			b.Uint32 = make([]uint32, 0, sz)
		}
	case BlockUint16:
		if sz <= DefaultMaxPointsPerBlock {
			b.Uint16 = uint16Pool.Get().([]uint16)
		} else {
			b.Uint16 = make([]uint16, 0, sz)
		}
	case BlockUint8:
		if sz <= DefaultMaxPointsPerBlock {
			b.Uint8 = uint8Pool.Get().([]uint8)
		} else {
			b.Uint8 = make([]uint8, 0, sz)
		}
	case BlockBool:
		b.Bits = vec.NewBitset(sz)
		b.Bits.Reset()
	case BlockString:
		if sz <= DefaultMaxPointsPerBlock {
			b.Strings = stringPool.Get().([]string)
		} else {
			b.Strings = make([]string, 0, sz)
		}
	case BlockBytes:
		if sz <= DefaultMaxPointsPerBlock {
			b.Bytes = bytesPool.Get().([][]byte)
		} else {
			b.Bytes = make([][]byte, 0, sz)
		}
	case BlockInt128:
		if sz <= DefaultMaxPointsPerBlock {
			b.Int128 = int128Pool.Get().([]vec.Int128)
		} else {
			b.Int128 = make([]vec.Int128, 0, sz)
		}
	case BlockInt256:
		if sz <= DefaultMaxPointsPerBlock {
			b.Int256 = int256Pool.Get().([]vec.Int256)
		} else {
			b.Int256 = make([]vec.Int256, 0, sz)
		}
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
	case BlockString:
		b.Strings = b.Strings[:len(src.Strings)]
		copy(b.Strings, src.Strings)
	case BlockBytes:
		b.Bytes = b.Bytes[:len(src.Bytes)]
		for i, v := range src.Bytes {
			b.Bytes[i] = make([]byte, len(v))
			copy(b.Bytes[i], v)
		}
	case BlockInt128:
		b.Int128 = b.Int128[:len(src.Int128)]
		copy(b.Int128, src.Int128)
	case BlockInt256:
		b.Int256 = b.Int256[:len(src.Int256)]
		copy(b.Int256, src.Int256)
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
	case BlockString:
		return len(b.Strings)
	case BlockBytes:
		return len(b.Bytes)
	case BlockInt128:
		return len(b.Int128)
	case BlockInt256:
		return len(b.Int256)
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
	case BlockString:
		return cap(b.Strings)
	case BlockBytes:
		return cap(b.Bytes)
	case BlockInt128:
		return cap(b.Int128)
	case BlockInt256:
		return cap(b.Int256)
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
	case BlockString:
		sz = compress.StringArrayEncodedSize(b.Strings)
	case BlockBytes:
		sz = compress.BytesArrayEncodedSize(b.Bytes)
	case BlockInt128:
		sz = compress.Int128ArrayEncodedSize(b.Int128)
	case BlockInt256:
		sz = compress.Int256ArrayEncodedSize(b.Int256)
	}
	return sz + storedBlockHeaderSize + b.comp.HeaderSize(sz)
}

func (b *Block) HeapSize() int {
	const (
		sliceSize  = 24 // reflect.SliceHeader incl. padding
		stringSize = 16 // reflect.StringHeader incl. padding
	)
	sz := 3 + 15*sliceSize
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
	case BlockString:
		for _, v := range b.Strings {
			sz += len(v) + stringSize
		}
	case BlockBytes:
		for _, v := range b.Bytes {
			sz += len(v) + sliceSize
		}
	case BlockInt128:
		sz += len(b.Int128) * 16
	case BlockInt256:
		sz += len(b.Int256) * 32
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
	case BlockString:
		for j := range b.Strings {
			b.Strings[j] = ""
		}
		b.Strings = b.Strings[:0]
	case BlockBytes:
		for j := range b.Bytes {
			b.Bytes[j] = nil
		}
		b.Bytes = b.Bytes[:0]
	case BlockBool:
		b.Bits.Reset()
	case BlockInt128:
		b.Int128 = b.Int128[:0]
	case BlockInt256:
		b.Int256 = b.Int256[:0]
	}
	b.dirty = true
	b.size = 0
}

func (b *Block) Release() {
	if b.ignore {
		b.ignore = false
		return
	}
	b.ignore = false
	b.dirty = false
	b.size = 0

	switch b.typ {
	case BlockFloat64:
		if cap(b.Float64) == DefaultMaxPointsPerBlock {
			float64Pool.Put(b.Float64[:0])
		}
		b.Float64 = nil
	case BlockFloat32:
		if cap(b.Float32) == DefaultMaxPointsPerBlock {
			float32Pool.Put(b.Float32[:0])
		}
		b.Float32 = nil
	case BlockInt64, BlockTime:
		if cap(b.Int64) == DefaultMaxPointsPerBlock {
			int64Pool.Put(b.Int64[:0])
		}
		b.Int64 = nil
	case BlockInt32:
		if cap(b.Int32) == DefaultMaxPointsPerBlock {
			int32Pool.Put(b.Int32[:0])
		}
		b.Int32 = nil
	case BlockInt16:
		if cap(b.Int16) == DefaultMaxPointsPerBlock {
			int16Pool.Put(b.Int16[:0])
		}
		b.Int16 = nil
	case BlockInt8:
		if cap(b.Int8) == DefaultMaxPointsPerBlock {
			int8Pool.Put(b.Int8[:0])
		}
		b.Int8 = nil
	case BlockUint64:
		if cap(b.Uint64) == DefaultMaxPointsPerBlock {
			uint64Pool.Put(b.Uint64[:0])
		}
		b.Uint64 = nil
	case BlockUint32:
		if cap(b.Uint32) == DefaultMaxPointsPerBlock {
			uint32Pool.Put(b.Uint32[:0])
		}
		b.Uint32 = nil
	case BlockUint16:
		if cap(b.Uint16) == DefaultMaxPointsPerBlock {
			uint16Pool.Put(b.Uint16[:0])
		}
		b.Uint16 = nil
	case BlockUint8:
		if cap(b.Uint8) == DefaultMaxPointsPerBlock {
			uint8Pool.Put(b.Uint8[:0])
		}
		b.Uint8 = nil
	case BlockBool:
		b.Bits.Close()
		b.Bits = nil
	case BlockString:
		for j := range b.Strings {
			b.Strings[j] = ""
		}
		if cap(b.Strings) == DefaultMaxPointsPerBlock {
			stringPool.Put(b.Strings[:0])
		}
		b.Strings = nil
	case BlockBytes:
		for j := range b.Bytes {
			b.Bytes[j] = nil
		}
		if cap(b.Bytes) == DefaultMaxPointsPerBlock {
			bytesPool.Put(b.Bytes[:0])
		}
		b.Bytes = nil
	case BlockInt128:
		if cap(b.Int128) == DefaultMaxPointsPerBlock {
			int128Pool.Put(b.Int128[:0])
		}
		b.Int128 = nil
	case BlockInt256:
		if cap(b.Int256) == DefaultMaxPointsPerBlock {
			int256Pool.Put(b.Int256[:0])
		}
		b.Int256 = nil
	case BlockIgnore:
		return
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
		n, err = encodeStringBlock(buf, b.Strings, b.Compression())
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
			b.Int64 = make([]int64, 0, sz)
		} else {
			b.Int64 = b.Int64[:0]
		}
		b.Int64, err = decodeTimeBlock(buf, b.Int64)

	case BlockFloat64:
		if b.Float64 == nil || cap(b.Float64) < sz {
			b.Float64 = make([]float64, 0, sz)
		} else {
			b.Float64 = b.Float64[:0]
		}
		b.Float64, err = decodeFloat64Block(buf, b.Float64)

	case BlockFloat32:
		if b.Float32 == nil || cap(b.Float32) < sz {
			b.Float32 = make([]float32, 0, sz)
		} else {
			b.Float32 = b.Float32[:0]
		}
		b.Float32, err = decodeFloat32Block(buf, b.Float32)

	case BlockInt64:
		if b.Int64 == nil || cap(b.Int64) < sz {
			b.Int64 = make([]int64, 0, sz)
		} else {
			b.Int64 = b.Int64[:0]
		}
		b.Int64, err = decodeInt64Block(buf, b.Int64)

	case BlockInt32:
		if b.Int32 == nil || cap(b.Int32) < sz {
			b.Int32 = make([]int32, 0, sz)
		} else {
			b.Int32 = b.Int32[:0]
		}
		b.Int32, err = decodeInt32Block(buf, b.Int32)

	case BlockInt16:
		if b.Int16 == nil || cap(b.Int16) < sz {
			b.Int16 = make([]int16, 0, sz)
		} else {
			b.Int16 = b.Int16[:0]
		}
		b.Int16, err = decodeInt16Block(buf, b.Int16)

	case BlockInt8:
		if b.Int8 == nil || cap(b.Int8) < sz {
			b.Int8 = make([]int8, 0, sz)
		} else {
			b.Int8 = b.Int8[:0]
		}
		b.Int8, err = decodeInt8Block(buf, b.Int8)

	case BlockUint64:
		if b.Uint64 == nil || cap(b.Uint64) < sz {
			b.Uint64 = make([]uint64, 0, sz)
		} else {
			b.Uint64 = b.Uint64[:0]
		}
		b.Uint64, err = decodeUint64Block(buf, b.Uint64)

	case BlockUint32:
		if b.Uint32 == nil || cap(b.Uint32) < sz {
			b.Uint32 = make([]uint32, 0, sz)
		} else {
			b.Uint32 = b.Uint32[:0]
		}
		b.Uint32, err = decodeUint32Block(buf, b.Uint32)

	case BlockUint16:
		if b.Uint16 == nil || cap(b.Uint16) < sz {
			b.Uint16 = make([]uint16, 0, sz)
		} else {
			b.Uint16 = b.Uint16[:0]
		}
		b.Uint16, err = decodeUint16Block(buf, b.Uint16)

	case BlockUint8:
		if b.Uint8 == nil || cap(b.Uint8) < sz {
			b.Uint8 = make([]uint8, 0, sz)
		} else {
			b.Uint8 = b.Uint8[:0]
		}
		b.Uint8, err = decodeUint8Block(buf, b.Uint8)

	case BlockBool:
		if b.Bits == nil || b.Bits.Cap() < sz {
			b.Bits = vec.NewBitset(sz)
			b.Bits.Reset()
		} else {
			b.Bits.Grow(sz).Reset()
		}
		b.Bits, err = decodeBoolBlock(buf, b.Bits)

	case BlockString:
		if b.Strings == nil || cap(b.Strings) < sz {
			b.Strings = make([]string, 0, sz)
		} else {
			b.Strings = b.Strings[:0]
		}
		b.Strings, err = decodeStringBlock(buf, b.Strings)

	case BlockBytes:
		if b.Bytes == nil || cap(b.Bytes) < sz {
			b.Bytes = make([][]byte, 0, sz)
		} else {
			b.Bytes = b.Bytes[:0]
		}
		b.Bytes, err = decodeBytesBlock(buf, b.Bytes)

	case BlockInt128:
		if b.Int128 == nil || cap(b.Int128) < sz {
			b.Int128 = make([]vec.Int128, 0, sz)
		} else {
			b.Int128 = b.Int128[:0]
		}
		b.Int128, err = decodeInt128Block(buf, b.Int128)

	case BlockInt256:
		if b.Int256 == nil || cap(b.Int256) < sz {
			b.Int256 = make([]vec.Int256, 0, sz)
		} else {
			b.Int256 = b.Int256[:0]
		}
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
		return vec.Strings.MinMax(b.Strings)
	case BlockBytes:
		return vec.Bytes.MinMax(b.Bytes)
	case BlockInt128:
		return vec.Int128Slice(b.Int128).MinMax()
	case BlockInt256:
		return vec.Int256Slice(b.Int256).MinMax()
	default:
		return nil, nil
	}
}

func (b *Block) Less(i, j int) bool {
	switch b.typ {
	case BlockInt256:
		return b.Int256[i].Lt(b.Int256[j])
	case BlockInt128:
		return b.Int128[i].Lt(b.Int128[j])
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
	case BlockString:
		return b.Strings[i] < b.Strings[j]
	case BlockBytes:
		return bytes.Compare(b.Bytes[i], b.Bytes[j]) < 0
	default:
		return false
	}
}

func (b *Block) Swap(i, j int) {
	switch b.typ {
	case BlockBytes:
		b.Bytes[i], b.Bytes[j] = b.Bytes[j], b.Bytes[i]

	case BlockString:
		b.Strings[i], b.Strings[j] = b.Strings[j], b.Strings[i]

	case BlockBool:
		b.Bits.Swap(i, j)

	case BlockFloat64:
		b.Float64[i], b.Float64[j] = b.Float64[j], b.Float64[i]

	case BlockFloat32:
		b.Float32[i], b.Float32[j] = b.Float32[j], b.Float32[i]

	case BlockInt256:
		b.Int256[i], b.Int256[j] = b.Int256[j], b.Int256[i]

	case BlockInt128:
		b.Int128[i], b.Int128[j] = b.Int128[j], b.Int128[i]

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
