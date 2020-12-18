// Copyright (c) 2018-2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package block

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"time"

	"blockwatch.cc/knoxdb/encoding/compress"
	"blockwatch.cc/knoxdb/util"
	. "blockwatch.cc/knoxdb/vec"
)

var bigEndian = binary.BigEndian

// unused, 2 bit reserved
type BlockFlags byte

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
	head  Header
	dirty bool

	// TODO: measure performance impact of using an interface instead of direct slices
	//       this can save up to 15x storage for slice headers / pointers
	//       but adds another level of indirection on each data access
	// data interface{}
	Strings []string
	Bytes   [][]byte
	Bools   []bool  // -> BitSet
	Int64   []int64 // re-used by Decimal64, Timestamps
	Int32   []int32 // re-used by Decimal32
	Int16   []int16
	Int8    []int8
	Uint64  []uint64
	Uint32  []uint32
	Uint16  []uint16
	Uint8   []uint8
	Float64 []float64
	Float32 []float32
	Int128  []Int128 // re-used by Decimal128, Int128
	Int256  []Int256 // re-used by Decimal256, Int256
}

func (b Block) Header() Header {
	return b.head.Clone()
}

func (b Block) Type() BlockType {
	return b.head.Type
}

func (b Block) Compression() Compression {
	return b.head.Compression
}

func (b Block) Scale() int {
	return b.head.Scale
}

func (b *Block) SetScale(s int) {
	b.head.Scale = s
}

func (b *Block) SetIgnore() {
	b.head.Type = BlockIgnore
}

func (b *Block) SetDirty() {
	b.dirty = true
}

func (b Block) Dirty() bool {
	return b.dirty
}

func NewBlock(typ BlockType, sz int, comp Compression, scale int, flags BlockFlags) (*Block, error) {
	b := &Block{
		head: NewHeader(typ, comp, scale, flags),
	}
	switch typ {
	case BlockTime:
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
	case BlockInt64:
		if sz <= DefaultMaxPointsPerBlock {
			b.Int64 = int64Pool.Get().([]int64)
		} else {
			b.Int64 = make([]int64, 0, sz)
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
		if sz <= DefaultMaxPointsPerBlock {
			b.Bools = boolPool.Get().([]bool)
		} else {
			b.Bools = make([]bool, 0, sz)
		}
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
			b.Int128 = int128Pool.Get().([]Int128)
		} else {
			b.Int128 = make([]Int128, 0, sz)
		}
	case BlockInt256:
		if sz <= DefaultMaxPointsPerBlock {
			b.Int256 = int256Pool.Get().([]Int256)
		} else {
			b.Int256 = make([]Int256, 0, sz)
		}
	default:
		return nil, fmt.Errorf("pack: invalid data type %d", typ)
	}
	return b, nil
}

func (b *Block) Clone(sz int, copydata bool) (*Block, error) {
	cp := &Block{
		head: b.head.Clone(),
	}
	if !copydata {
		cp.head.Clear()
	}
	switch b.Type() {
	case BlockTime:
		if copydata {
			if sz <= DefaultMaxPointsPerBlock {
				cp.Int64 = int64Pool.Get().([]int64)[:sz]
			} else {
				cp.Int64 = make([]int64, sz)
			}
			copy(cp.Int64, b.Int64)
		} else {
			if sz <= DefaultMaxPointsPerBlock {
				cp.Int64 = int64Pool.Get().([]int64)[:0]
			} else {
				cp.Int64 = make([]int64, 0, sz)
			}
		}
	case BlockFloat64:
		if copydata {
			if sz <= DefaultMaxPointsPerBlock {
				cp.Float64 = float64Pool.Get().([]float64)[:sz]
			} else {
				cp.Float64 = make([]float64, sz)
			}
			copy(cp.Float64, b.Float64)
		} else {
			if sz <= DefaultMaxPointsPerBlock {
				cp.Float64 = float64Pool.Get().([]float64)[:0]
			} else {
				cp.Float64 = make([]float64, 0, sz)
			}
		}
	case BlockFloat32:
		if copydata {
			if sz <= DefaultMaxPointsPerBlock {
				cp.Float32 = float32Pool.Get().([]float32)[:sz]
			} else {
				cp.Float32 = make([]float32, sz)
			}
			copy(cp.Float32, b.Float32)
		} else {
			if sz <= DefaultMaxPointsPerBlock {
				cp.Float32 = float32Pool.Get().([]float32)[:0]
			} else {
				cp.Float32 = make([]float32, 0, sz)
			}
		}
	case BlockInt64:
		if copydata {
			if sz <= DefaultMaxPointsPerBlock {
				cp.Int64 = int64Pool.Get().([]int64)[:sz]
			} else {
				cp.Int64 = make([]int64, sz)
			}
			copy(cp.Int64, b.Int64)
		} else {
			if sz <= DefaultMaxPointsPerBlock {
				cp.Int64 = int64Pool.Get().([]int64)[:0]
			} else {
				cp.Int64 = make([]int64, 0, sz)
			}
		}
	case BlockInt32:
		if copydata {
			if sz <= DefaultMaxPointsPerBlock {
				cp.Int32 = int32Pool.Get().([]int32)[:sz]
			} else {
				cp.Int32 = make([]int32, sz)
			}
			copy(cp.Int32, b.Int32)
		} else {
			if sz <= DefaultMaxPointsPerBlock {
				cp.Int32 = int32Pool.Get().([]int32)[:0]
			} else {
				cp.Int32 = make([]int32, 0, sz)
			}
		}
	case BlockInt16:
		if copydata {
			if sz <= DefaultMaxPointsPerBlock {
				cp.Int16 = int16Pool.Get().([]int16)[:sz]
			} else {
				cp.Int16 = make([]int16, sz)
			}
			copy(cp.Int16, b.Int16)
		} else {
			if sz <= DefaultMaxPointsPerBlock {
				cp.Int16 = int16Pool.Get().([]int16)[:0]
			} else {
				cp.Int16 = make([]int16, 0, sz)
			}
		}
	case BlockInt8:
		if copydata {
			if sz <= DefaultMaxPointsPerBlock {
				cp.Int8 = int8Pool.Get().([]int8)[:sz]
			} else {
				cp.Int8 = make([]int8, sz)
			}
			copy(cp.Int8, b.Int8)
		} else {
			if sz <= DefaultMaxPointsPerBlock {
				cp.Int8 = int8Pool.Get().([]int8)[:0]
			} else {
				cp.Int8 = make([]int8, 0, sz)
			}
		}
	case BlockUint64:
		if copydata {
			if sz <= DefaultMaxPointsPerBlock {
				cp.Uint64 = uint64Pool.Get().([]uint64)[:sz]
			} else {
				cp.Uint64 = make([]uint64, sz)
			}
			copy(cp.Uint64, b.Uint64)
		} else {
			if sz <= DefaultMaxPointsPerBlock {
				cp.Uint64 = uint64Pool.Get().([]uint64)[:0]
			} else {
				cp.Uint64 = make([]uint64, 0, sz)
			}
		}
	case BlockUint32:
		if copydata {
			if sz <= DefaultMaxPointsPerBlock {
				cp.Uint32 = uint32Pool.Get().([]uint32)[:sz]
			} else {
				cp.Uint32 = make([]uint32, sz)
			}
			copy(cp.Uint32, b.Uint32)
		} else {
			if sz <= DefaultMaxPointsPerBlock {
				cp.Uint32 = uint32Pool.Get().([]uint32)[:0]
			} else {
				cp.Uint32 = make([]uint32, 0, sz)
			}
		}
	case BlockUint16:
		if copydata {
			if sz <= DefaultMaxPointsPerBlock {
				cp.Uint16 = uint16Pool.Get().([]uint16)[:sz]
			} else {
				cp.Uint16 = make([]uint16, sz)
			}
			copy(cp.Uint16, b.Uint16)
		} else {
			if sz <= DefaultMaxPointsPerBlock {
				cp.Uint16 = uint16Pool.Get().([]uint16)[:0]
			} else {
				cp.Uint16 = make([]uint16, 0, sz)
			}
		}
	case BlockUint8:
		if copydata {
			if sz <= DefaultMaxPointsPerBlock {
				cp.Uint8 = uint8Pool.Get().([]uint8)[:sz]
			} else {
				cp.Uint8 = make([]uint8, sz)
			}
			copy(cp.Uint8, b.Uint8)
		} else {
			if sz <= DefaultMaxPointsPerBlock {
				cp.Uint8 = uint8Pool.Get().([]uint8)[:0]
			} else {
				cp.Uint8 = make([]uint8, 0, sz)
			}
		}
	case BlockBool:
		if copydata {
			if sz <= DefaultMaxPointsPerBlock {
				cp.Bools = boolPool.Get().([]bool)[:sz]
			} else {
				cp.Bools = make([]bool, sz)
			}
			copy(cp.Bools, b.Bools)
		} else {
			if sz <= DefaultMaxPointsPerBlock {
				cp.Bools = boolPool.Get().([]bool)[:0]
			} else {
				cp.Bools = make([]bool, 0, sz)
			}
		}
	case BlockString:
		if copydata {
			if sz <= DefaultMaxPointsPerBlock {
				cp.Strings = stringPool.Get().([]string)[:sz]
			} else {
				cp.Strings = make([]string, sz)
			}
			copy(cp.Strings, b.Strings)
		} else {
			if sz <= DefaultMaxPointsPerBlock {
				cp.Strings = stringPool.Get().([]string)[:0]
			} else {
				cp.Strings = make([]string, 0, sz)
			}
		}
	case BlockBytes:
		if copydata {
			if sz <= DefaultMaxPointsPerBlock {
				cp.Bytes = bytesPool.Get().([][]byte)[:sz]
			} else {
				cp.Bytes = make([][]byte, sz)
			}
			for i, v := range b.Bytes {
				cp.Bytes[i] = make([]byte, len(v))
				copy(cp.Bytes[i], v)
			}
		} else {
			if sz <= DefaultMaxPointsPerBlock {
				cp.Bytes = bytesPool.Get().([][]byte)[:0]
			} else {
				cp.Bytes = make([][]byte, 0, sz)
			}
		}
	case BlockInt128:
		if copydata {
			if sz <= DefaultMaxPointsPerBlock {
				cp.Int128 = int128Pool.Get().([]Int128)[:sz]
			} else {
				cp.Int128 = make([]Int128, sz)
			}
			copy(cp.Int128, b.Int128)
		} else {
			if sz <= DefaultMaxPointsPerBlock {
				cp.Int128 = int128Pool.Get().([]Int128)[:0]
			} else {
				cp.Int128 = make([]Int128, 0, sz)
			}
		}
	case BlockInt256:
		if copydata {
			if sz <= DefaultMaxPointsPerBlock {
				cp.Int256 = int256Pool.Get().([]Int256)[:sz]
			} else {
				cp.Int256 = make([]Int256, sz)
			}
		} else {
			if sz <= DefaultMaxPointsPerBlock {
				cp.Int256 = int256Pool.Get().([]Int256)[:0]
			} else {
				cp.Int256 = make([]Int256, 0, sz)
			}
		}

	default:
		return nil, fmt.Errorf("pack: invalid data type %s (%[1]d)", b.Type())
	}
	return cp, nil
}

func (b *Block) Len() int {
	switch b.Type() {
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
		return len(b.Bools)
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
	switch b.Type() {
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
		return cap(b.Bools)
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
// This size hint is used to properly dimension the decoder buffer before
// decoding as is required by LZ4.
func (b *Block) MaxStoredSize() int {
	var sz int
	switch b.Type() {
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
		sz = compress.BooleanArrayEncodedSize(b.Bools)
	case BlockString:
		sz = compress.StringArrayEncodedSize(b.Strings)
	case BlockBytes:
		sz = compress.BytesArrayEncodedSize(b.Bytes)
	case BlockInt128:
		sz = compress.Int128ArrayEncodedSize(b.Int128)
	case BlockInt256:
		sz = compress.Int256ArrayEncodedSize(b.Int256)
	}
	return sz + encodedBlockHeaderSize + b.head.Compression.HeaderSize(sz)
}

func (b *Block) HeapSize() int {
	const (
		sliceSize  = 24 // reflect.SliceHeader incl. padding
		stringSize = 16 // reflect.StringHeader incl. padding
		headerSize = 28 // base header with interface pointers (?)
	)
	sz := headerSize + 15*sliceSize
	switch b.Type() {
	case BlockFloat64:
		sz += len(b.Float64)*8 + 2*8
	case BlockFloat32:
		sz += len(b.Float32)*4 + 2*4
	case BlockInt64, BlockTime:
		sz += len(b.Int64)*8 + 2*8
	case BlockInt32:
		sz += len(b.Int32)*4 + 2*4
	case BlockInt16:
		sz += len(b.Int16)*2 + 2*2
	case BlockInt8:
		sz += len(b.Int8) + 2
	case BlockUint64:
		sz += len(b.Uint64)*8 + 2*8
	case BlockUint32:
		sz += len(b.Uint32)*4 + 2*4
	case BlockUint16:
		sz += len(b.Uint16)*2 + 2*2
	case BlockUint8:
		sz += len(b.Uint8) + 2
	case BlockBool:
		sz += len(b.Bools)*1 + 2
	case BlockString:
		min, max := 0, 0
		for _, v := range b.Strings {
			l := len(v)
			min = util.Min(min, l)
			max = util.Max(max, l)
			sz += l + stringSize
		}
		sz += min + max + 2*stringSize
	case BlockBytes:
		min, max := 0, 0
		for _, v := range b.Bytes {
			l := len(v)
			min = util.Min(min, l)
			max = util.Max(max, l)
			sz += l + sliceSize
		}
		sz += min + max + 2*sliceSize
	case BlockInt128:
		sz += len(b.Int128)*16 + 2*16
	case BlockInt256:
		sz += len(b.Int256)*32 + 2*32
	}
	return sz
}

func (b *Block) Clear() {
	b.head.Clear()
	switch b.Type() {
	case BlockTime:
		b.Int64 = b.Int64[:0]
	case BlockInt64:
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
		for j, _ := range b.Strings {
			b.Strings[j] = ""
		}
		b.Strings = b.Strings[:0]
	case BlockBytes:
		for j, _ := range b.Bytes {
			b.Bytes[j] = nil
		}
		b.Bytes = b.Bytes[:0]
	case BlockBool:
		b.Bools = b.Bools[:0]
	case BlockInt128:
		b.Int128 = b.Int128[:0]
	case BlockInt256:
		b.Int256 = b.Int256[:0]
	}
	b.dirty = false
}

func (b *Block) Release() {
	b.head.MinValue = nil
	b.head.MaxValue = nil
	b.dirty = false
	switch b.Type() {
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
		if cap(b.Bools) == DefaultMaxPointsPerBlock {
			boolPool.Put(b.Bools[:0])
		}
		b.Bools = nil
	case BlockString:
		for j, _ := range b.Strings {
			b.Strings[j] = ""
		}
		if cap(b.Strings) == DefaultMaxPointsPerBlock {
			stringPool.Put(b.Strings[:0])
		}
		b.Strings = nil
	case BlockBytes:
		for j, _ := range b.Bytes {
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
	}
}

// FIXME: pass a buffer to avoid alloc+memcopy
func (b *Block) Encode() ([]byte, []byte, error) {
	body, err := b.EncodeBody()
	if err != nil {
		return nil, nil, err
	}
	// encode header second, to allow for reparsing min/max values during encode
	buf := bytes.NewBuffer(make([]byte, 0, b.head.EncodedSize()))
	err = b.head.Encode(buf)
	if err != nil {
		return nil, nil, err
	}
	return buf.Bytes(), body, nil
}

// FIXME: pass a buffer to avoid alloc+memcopy
// values -> raw
// - analyzes values to determine best compression
// - uses buffer pool to allocate output slice once and avoid memcopy
// - compression is used as hint, data may be stored uncompressed
func (b *Block) EncodeBody() ([]byte, error) {
	var buf *bytes.Buffer
	sz := b.MaxStoredSize()
	if sz <= BlockSizeHint {
		buf = bytes.NewBuffer(BlockEncoderPool.Get().([]byte)[:0])
	} else {
		buf = bytes.NewBuffer(make([]byte, 0, sz))
	}

	switch b.Type() {
	case BlockTime:
		min, max, err := encodeTimeBlock(buf, b.Int64, b.Compression())
		if err != nil {
			return nil, err
		}
		if b.dirty {
			b.head.MinValue = time.Unix(0, min).UTC()
			b.head.MaxValue = time.Unix(0, max).UTC()
			b.dirty = false
		}

	case BlockFloat64:
		min, max, err := encodeFloat64Block(buf, b.Float64, b.Compression())
		if err != nil {
			return nil, err
		}
		if b.dirty {
			b.head.MinValue = min
			b.head.MaxValue = max
			b.dirty = false
		}

	case BlockFloat32:
		min, max, err := encodeFloat32Block(buf, b.Float32, b.Compression())
		if err != nil {
			return nil, err
		}
		if b.dirty {
			b.head.MinValue = min
			b.head.MaxValue = max
			b.dirty = false
		}

	case BlockInt64:
		min, max, err := encodeInt64Block(buf, b.Int64, b.Compression())
		if err != nil {
			return nil, err
		}
		if b.dirty {
			b.head.MinValue = min
			b.head.MaxValue = max
			b.dirty = false
		}

	case BlockInt32:
		min, max, err := encodeInt32Block(buf, b.Int32, b.Compression())
		if err != nil {
			return nil, err
		}
		if b.dirty {
			b.head.MinValue = min
			b.head.MaxValue = max
			b.dirty = false
		}

	case BlockInt16:
		min, max, err := encodeInt16Block(buf, b.Int16, b.Compression())
		if err != nil {
			return nil, err
		}
		if b.dirty {
			b.head.MinValue = min
			b.head.MaxValue = max
			b.dirty = false
		}
	case BlockInt8:
		min, max, err := encodeInt8Block(buf, b.Int8, b.Compression())
		if err != nil {
			return nil, err
		}
		if b.dirty {
			b.head.MinValue = min
			b.head.MaxValue = max
			b.dirty = false
		}

	case BlockUint64:
		min, max, err := encodeUint64Block(buf, b.Uint64, b.Compression())
		if err != nil {
			return nil, err
		}
		if b.dirty {
			b.head.MinValue = min
			b.head.MaxValue = max
			b.dirty = false
		}

	case BlockUint32:
		min, max, err := encodeUint32Block(buf, b.Uint32, b.Compression())
		if err != nil {
			return nil, err
		}
		if b.dirty {
			b.head.MinValue = min
			b.head.MaxValue = max
			b.dirty = false
		}

	case BlockUint16:
		min, max, err := encodeUint16Block(buf, b.Uint16, b.Compression())
		if err != nil {
			return nil, err
		}
		if b.dirty {
			b.head.MinValue = min
			b.head.MaxValue = max
			b.dirty = false
		}

	case BlockUint8:
		min, max, err := encodeUint8Block(buf, b.Uint8, b.Compression())
		if err != nil {
			return nil, err
		}
		if b.dirty {
			b.head.MinValue = min
			b.head.MaxValue = max
			b.dirty = false
		}

	case BlockBool:
		min, max, err := encodeBoolBlock(buf, b.Bools, b.Compression())
		if err != nil {
			return nil, err
		}
		if b.dirty {
			b.head.MinValue = min
			b.head.MaxValue = max
			b.dirty = false
		}

	case BlockString:
		min, max, err := encodeStringBlock(buf, b.Strings, b.Compression())
		if err != nil {
			return nil, err
		}
		if b.dirty {
			b.head.MinValue = min
			b.head.MaxValue = max
			b.dirty = false
		}

	case BlockBytes:
		min, max, err := encodeBytesBlock(buf, b.Bytes, b.Compression())
		if err != nil {
			return nil, err
		}
		if b.dirty {
			b.head.MinValue = min
			b.head.MaxValue = max
			b.dirty = false
		}

	case BlockInt128:
		min, max, err := encodeInt128Block(buf, b.Int128, b.Compression())
		if err != nil {
			return nil, err
		}
		if b.dirty {
			b.head.MinValue = min
			b.head.MaxValue = max
			b.dirty = false
		}

	case BlockInt256:
		min, max, err := encodeInt256Block(buf, b.Int256, b.Compression())
		if err != nil {
			return nil, err
		}
		if b.dirty {
			b.head.MinValue = min
			b.head.MaxValue = max
			b.dirty = false
		}

	case BlockIgnore:
		b.dirty = false
		return nil, nil

	default:
		return nil, fmt.Errorf("pack: invalid data type %d", b.Type)
	}
	return buf.Bytes(), nil
}

// func (b *Block) EncodeHeader() ([]byte, error) {
// 	// re-use large enough buffer
// 	buf := bytes.NewBuffer(BlockEncoderPool.Get().([]byte)[:0])

// 	// 8                 7 6          5 4 3 2 1
// 	// ext header flag   compression  block type
// 	buf.WriteByte(byte(b.Type&0x1f) | byte(b.Compression()&0x3)<<5 | 0x80)

// 	// extension header
// 	// - 6 lower bits are used for storing scale (0..63)
// 	// - 2 upper bits are flags
// 	buf.WriteByte((byte(b.Flags)&0x3)<<6 | byte(b.Scale)&0x3f)

// 	switch b.Type {
// 	case BlockTime:
// 		// FIXME: for consistency purposes, time blocks are int64
// 		//        and headers should be int64 as well. only BlockHeader
// 		//        contains real values
// 		var v [16]byte
// 		min, max := b.head.MinValue.(time.Time), b.head.MaxValue.(time.Time)
// 		vmin, vmax := min.UnixNano(), max.UnixNano()
// 		bigEndian.PutUint64(v[0:], uint64(vmin))
// 		bigEndian.PutUint64(v[8:], uint64(vmax))
// 		_, _ = buf.Write(v[:])

// 	case BlockFloat64:
// 		var v [16]byte
// 		min, max := b.head.MinValue.(float64), b.head.MaxValue.(float64)
// 		bigEndian.PutUint64(v[0:], math.Float64bits(min))
// 		bigEndian.PutUint64(v[8:], math.Float64bits(max))
// 		_, _ = buf.Write(v[:])

// 	case BlockFloat32:
// 		var v [8]byte
// 		min, max := b.head.MinValue.(float32), b.head.MaxValue.(float32)
// 		bigEndian.PutUint32(v[0:], math.Float32bits(min))
// 		bigEndian.PutUint32(v[4:], math.Float32bits(max))
// 		_, _ = buf.Write(v[:])

// 	case BlockInt64:
// 		var v [16]byte
// 		min, max := b.head.MinValue.(int64), b.head.MaxValue.(int64)
// 		bigEndian.PutUint64(v[0:], uint64(min))
// 		bigEndian.PutUint64(v[8:], uint64(max))
// 		_, _ = buf.Write(v[:])

// 	case BlockInt32:
// 		var v [8]byte
// 		min, max := b.head.MinValue.(int32), b.head.MaxValue.(int32)
// 		bigEndian.PutUint32(v[0:], uint32(min))
// 		bigEndian.PutUint32(v[4:], uint32(max))
// 		_, _ = buf.Write(v[:])

// 	case BlockInt16:
// 		var v [4]byte
// 		min, max := b.head.MinValue.(int16), b.head.MaxValue.(int16)
// 		bigEndian.PutUint16(v[0:], uint16(min))
// 		bigEndian.PutUint16(v[2:], uint16(max))
// 		_, _ = buf.Write(v[:])

// 	case BlockInt8:
// 		var v [2]byte
// 		min, max := b.head.MinValue.(int8), b.head.MaxValue.(int8)
// 		v[0] = uint8(min)
// 		v[1] = uint8(max)
// 		_, _ = buf.Write(v[:])

// 	case BlockUint64:
// 		// always stored as uint, conversion happens only for BlockHeader
// 		var v [16]byte
// 		min, max := b.head.MinValue.(uint64), b.head.MaxValue.(uint64)
// 		bigEndian.PutUint64(v[0:], min)
// 		bigEndian.PutUint64(v[8:], max)
// 		_, _ = buf.Write(v[:])

// 	case BlockUint32:
// 		// always stored as uint, conversion happens only for BlockHeader
// 		var v [8]byte
// 		min, max := b.head.MinValue.(uint32), b.head.MaxValue.(uint32)
// 		bigEndian.PutUint32(v[0:], min)
// 		bigEndian.PutUint32(v[4:], max)
// 		_, _ = buf.Write(v[:])

// 	case BlockUint16:
// 		// always stored as uint, conversion happens only for BlockHeader
// 		var v [4]byte
// 		min, max := b.head.MinValue.(uint16), b.head.MaxValue.(uint16)
// 		bigEndian.PutUint16(v[0:], min)
// 		bigEndian.PutUint16(v[2:], max)
// 		_, _ = buf.Write(v[:])

// 	case BlockUint8:
// 		// always stored as uint, conversion happens only for BlockHeader
// 		var v [2]byte
// 		min, max := b.head.MinValue.(uint8), b.head.MaxValue.(uint8)
// 		v[0] = min
// 		v[1] = max
// 		_, _ = buf.Write(v[:])

// 	case BlockBool:
// 		var v byte
// 		min, max := b.head.MinValue.(bool), b.head.MaxValue.(bool)
// 		if min {
// 			v = 1
// 		}
// 		if max {
// 			v += 2
// 		}
// 		buf.WriteByte(v)

// 	case BlockString:
// 		// null terminated string
// 		min, max := b.head.MinValue.(string), b.head.MaxValue.(string)
// 		_, _ = buf.WriteString(min)
// 		buf.WriteByte(0)
// 		_, _ = buf.WriteString(max)
// 		buf.WriteByte(0)

// 	case BlockBytes:
// 		// len prefixed byte slice
// 		min, max := b.head.MinValue.([]byte), b.head.MaxValue.([]byte)
// 		var v [8]byte
// 		i := binary.PutUvarint(v[:], uint64(len(min)))
// 		_, _ = buf.Write(v[:i])
// 		_, _ = buf.Write(min)

// 		i = binary.PutUvarint(v[:], uint64(len(max)))
// 		_, _ = buf.Write(v[:i])
// 		_, _ = buf.Write(max)

// 	case BlockDecimal32:
// 		var v [8]byte
// 		min, max := b.head.MinValue.(Decimal32).Int64(), b.head.MaxValue.(Decimal32).Int64()
// 		bigEndian.PutUint32(v[0:], uint32(min))
// 		bigEndian.PutUint32(v[4:], uint32(max))
// 		_, _ = buf.Write(v[:])

// 	case BlockDecimal64:
// 		var v [16]byte
// 		min, max := b.head.MinValue.(Decimal64).Int64(), b.head.MaxValue.(Decimal64).Int64()
// 		bigEndian.PutUint64(v[0:], uint64(min))
// 		bigEndian.PutUint64(v[8:], uint64(max))
// 		_, _ = buf.Write(v[:])

// 	case BlockDecimal128:
// 		var v [32]byte
// 		min, max := b.head.MinValue.(Decimal128).Int128(), b.head.MaxValue.(Decimal128).Int128()
// 		bigEndian.PutUint64(v[0:], uint64(min[0]))
// 		bigEndian.PutUint64(v[8:], uint64(min[1]))
// 		bigEndian.PutUint64(v[16:], uint64(max[0]))
// 		bigEndian.PutUint64(v[24:], uint64(max[1]))
// 		_, _ = buf.Write(v[:])

// 	case BlockDecimal256:
// 		var v [64]byte
// 		min, max := b.head.MinValue.(Decimal256).Int256(), b.head.MaxValue.(Decimal256).Int256()
// 		bigEndian.PutUint64(v[0:], uint64(min[0]))
// 		bigEndian.PutUint64(v[8:], uint64(min[1]))
// 		bigEndian.PutUint64(v[16:], uint64(min[2]))
// 		bigEndian.PutUint64(v[24:], uint64(min[3]))
// 		bigEndian.PutUint64(v[32:], uint64(max[0]))
// 		bigEndian.PutUint64(v[40:], uint64(max[1]))
// 		bigEndian.PutUint64(v[48:], uint64(max[2]))
// 		bigEndian.PutUint64(v[56:], uint64(max[3]))
// 		_, _ = buf.Write(v[:])

// 	case BlockInt128:
// 		var v [32]byte
// 		min, max := b.head.MinValue.(Int128), b.head.MaxValue.(Int128)
// 		bigEndian.PutUint64(v[0:], uint64(min[0]))
// 		bigEndian.PutUint64(v[8:], uint64(min[1]))
// 		bigEndian.PutUint64(v[16:], uint64(max[0]))
// 		bigEndian.PutUint64(v[24:], uint64(max[1]))
// 		_, _ = buf.Write(v[:])

// 	case BlockInt256:
// 		var v [64]byte
// 		min, max := b.head.MinValue.(Int256), b.head.MaxValue.(Int256)
// 		bigEndian.PutUint64(v[0:], uint64(min[0]))
// 		bigEndian.PutUint64(v[8:], uint64(min[1]))
// 		bigEndian.PutUint64(v[16:], uint64(min[2]))
// 		bigEndian.PutUint64(v[24:], uint64(min[3]))
// 		bigEndian.PutUint64(v[32:], uint64(max[0]))
// 		bigEndian.PutUint64(v[40:], uint64(max[1]))
// 		bigEndian.PutUint64(v[48:], uint64(max[2]))
// 		bigEndian.PutUint64(v[56:], uint64(max[3]))
// 		_, _ = buf.Write(v[:])

// 	case BlockIgnore:
// 		return nil, nil

// 	default:
// 		return nil, fmt.Errorf("pack: invalid data type %d", b.Type)
// 	}

// 	return buf.Bytes(), nil
// }

func (b *Block) DecodeHeader(buf *bytes.Buffer) error {
	return b.head.Decode(buf)
}

// func (b *Block) DecodeHeader(buf *bytes.Buffer) error {
// 	val := buf.Next(1)
// 	typ, err := readBlockType(val)
// 	if err != nil {
// 		return err
// 	}
// 	comp, err := readBlockCompression(val)
// 	if err != nil {
// 		return err
// 	}

// 	var (
// 		scale int
// 		flags BlockFlags
// 	)

// 	if val[0]&0x80 > 0 {
// 		val = buf.Next(1)
// 		scale = readBlockScale(val)
// 		flags = readBlockFlags(val)
// 	}

// 	switch typ {
// 	case BlockTime:
// 		v := buf.Next(16)
// 		vmin := bigEndian.Uint64(v[0:])
// 		vmax := bigEndian.Uint64(v[8:])
// 		if b.Type != BlockIgnore {
// 			b.Type = typ
// 			b.Compression() = comp
// 			b.head.MinValue = time.Unix(0, int64(vmin)).UTC()
// 			b.head.MaxValue = time.Unix(0, int64(vmax)).UTC()
// 		}

// 	case BlockFloat64:
// 		v := buf.Next(16)
// 		if b.Type != BlockIgnore {
// 			b.Type = typ
// 			b.Compression() = comp
// 			b.head.MinValue = math.Float64frombits(bigEndian.Uint64(v[0:]))
// 			b.head.MaxValue = math.Float64frombits(bigEndian.Uint64(v[8:]))
// 		}

// 	case BlockFloat32:
// 		v := buf.Next(8)
// 		if b.Type != BlockIgnore {
// 			b.Type = typ
// 			b.Compression() = comp
// 			b.head.MinValue = math.Float32frombits(bigEndian.Uint32(v[0:]))
// 			b.head.MaxValue = math.Float32frombits(bigEndian.Uint32(v[4:]))
// 		}

// 	case BlockInt64:
// 		v := buf.Next(16)
// 		if b.Type != BlockIgnore {
// 			b.Type = typ
// 			b.Compression() = comp
// 			b.head.MinValue = int64(bigEndian.Uint64(v[0:]))
// 			b.head.MaxValue = int64(bigEndian.Uint64(v[8:]))
// 		}

// 	case BlockInt32:
// 		v := buf.Next(8)
// 		if b.Type != BlockIgnore {
// 			b.Type = typ
// 			b.Compression() = comp
// 			b.head.MinValue = int32(bigEndian.Uint32(v[0:]))
// 			b.head.MaxValue = int32(bigEndian.Uint32(v[4:]))
// 		}

// 	case BlockInt16:
// 		v := buf.Next(4)
// 		if b.Type != BlockIgnore {
// 			b.Type = typ
// 			b.Compression() = comp
// 			b.head.MinValue = int16(bigEndian.Uint16(v[0:]))
// 			b.head.MaxValue = int16(bigEndian.Uint16(v[2:]))
// 		}

// 	case BlockInt8:
// 		v := buf.Next(2)
// 		if b.Type != BlockIgnore {
// 			b.Type = typ
// 			b.Compression() = comp
// 			b.head.MinValue = int8(v[0])
// 			b.head.MaxValue = int8(v[1])
// 		}

// 	case BlockUint64:
// 		v := buf.Next(16)
// 		if b.Type != BlockIgnore {
// 			b.Type = typ
// 			b.Compression() = comp
// 			b.Flags = flags
// 			b.head.MinValue = bigEndian.Uint64(v[0:])
// 			b.head.MaxValue = bigEndian.Uint64(v[8:])
// 		}

// 	case BlockUint32:
// 		v := buf.Next(8)
// 		if b.Type != BlockIgnore {
// 			b.Type = typ
// 			b.Compression() = comp
// 			b.Flags = flags
// 			b.head.MinValue = bigEndian.Uint32(v[0:])
// 			b.head.MaxValue = bigEndian.Uint32(v[4:])
// 		}

// 	case BlockUint16:
// 		v := buf.Next(4)
// 		if b.Type != BlockIgnore {
// 			b.Type = typ
// 			b.Compression() = comp
// 			b.head.MinValue = bigEndian.Uint16(v[0:])
// 			b.head.MaxValue = bigEndian.Uint16(v[2:])
// 		}

// 	case BlockUint8:
// 		v := buf.Next(2)
// 		if b.Type != BlockIgnore {
// 			b.Type = typ
// 			b.Compression() = comp
// 			b.head.MinValue = uint8(v[0])
// 			b.head.MaxValue = uint8(v[1])
// 		}

// 	case BlockBool:
// 		v := buf.Next(1)
// 		if b.Type != BlockIgnore {
// 			b.Type = typ
// 			b.Compression() = comp
// 			b.head.MinValue = v[0]&1 > 0
// 			b.head.MaxValue = v[0]&2 > 0
// 		}

// 	case BlockString:
// 		min, err := buf.ReadString(0)
// 		if err != nil {
// 			return fmt.Errorf("pack: reading min string block header: %v", err)
// 		}
// 		max, err := buf.ReadString(0)
// 		if err != nil {
// 			return fmt.Errorf("pack: reading max string block header: %v", err)
// 		}
// 		if b.Type != BlockIgnore {
// 			b.Type = typ
// 			b.Compression() = comp
// 			// don't reference buffer data!
// 			mincopy := min[:len(min)-1]
// 			maxcopy := max[:len(max)-1]
// 			b.head.MinValue = mincopy
// 			b.head.MaxValue = maxcopy
// 		}

// 	case BlockBytes:
// 		length, err := binary.ReadUvarint(buf)
// 		if err != nil {
// 			return fmt.Errorf("pack: reading min []byte block header: %v", err)
// 		}
// 		min := buf.Next(int(length))
// 		length, err = binary.ReadUvarint(buf)
// 		if err != nil {
// 			return fmt.Errorf("pack: reading max []byte block header: %v", err)
// 		}
// 		max := buf.Next(int(length))

// 		if b.Type != BlockIgnore {
// 			b.Type = typ
// 			b.Compression() = comp
// 			// don't reference buffer data!
// 			mincopy := make([]byte, len(min))
// 			maxcopy := make([]byte, len(max))
// 			copy(mincopy, min)
// 			copy(maxcopy, max)
// 			b.head.MinValue = mincopy
// 			b.head.MaxValue = maxcopy
// 		}

// 	case BlockDecimal32:
// 		v := buf.Next(8)
// 		if b.Type != BlockIgnore {
// 			b.Type = typ
// 			b.Compression() = comp
// 			b.Scale = scale
// 			b.head.MinValue, err = NewDecimal32(int32(bigEndian.Uint32(v[0:])), scale)
// 			if err != nil {
// 				return fmt.Errorf("pack: min block header: %v", err)
// 			}
// 			b.head.MaxValue, err = NewDecimal32(int32(bigEndian.Uint32(v[4:])), scale)
// 			if err != nil {
// 				return fmt.Errorf("pack: min block header: %v", err)
// 			}
// 		}

// 	case BlockDecimal64:
// 		v := buf.Next(16)
// 		if b.Type != BlockIgnore {
// 			b.Type = typ
// 			b.Compression() = comp
// 			b.Scale = scale
// 			b.head.MinValue, err = NewDecimal64(int64(bigEndian.Uint64(v[0:])), scale)
// 			if err != nil {
// 				return fmt.Errorf("pack: min block header: %v", err)
// 			}
// 			b.head.MaxValue, err = NewDecimal64(int64(bigEndian.Uint64(v[8:])), scale)
// 			if err != nil {
// 				return fmt.Errorf("pack: min block header: %v", err)
// 			}
// 		}

// 	case BlockDecimal128:
// 		v := buf.Next(32)
// 		if b.Type != BlockIgnore {
// 			b.Type = typ
// 			b.Compression() = comp
// 			b.Scale = scale
// 			min128 := NewInt128(
// 				int64(bigEndian.Uint64(v[0:])),
// 				bigEndian.Uint64(v[8:]),
// 			)
// 			b.head.MinValue, err = NewDecimal128(min128, scale)
// 			if err != nil {
// 				return fmt.Errorf("pack: min block header: %v", err)
// 			}
// 			max128 := NewInt128(
// 				int64(bigEndian.Uint64(v[16:])),
// 				bigEndian.Uint64(v[24:]),
// 			)
// 			b.head.MaxValue, err = NewDecimal128(max128, scale)
// 			if err != nil {
// 				return fmt.Errorf("pack: min block header: %v", err)
// 			}
// 		}

// 	case BlockDecimal256:
// 		v := buf.Next(64)
// 		if b.Type != BlockIgnore {
// 			b.Type = typ
// 			b.Compression() = comp
// 			b.Scale = scale
// 			min256 := NewInt256(
// 				int64(bigEndian.Uint64(v[0:])),
// 				bigEndian.Uint64(v[8:]),
// 				bigEndian.Uint64(v[16:]),
// 				bigEndian.Uint64(v[24:]),
// 			)
// 			b.head.MinValue, err = NewDecimal256(min256, scale)
// 			if err != nil {
// 				return fmt.Errorf("pack: min block header: %v", err)
// 			}
// 			max256 := NewInt256(
// 				int64(bigEndian.Uint64(v[32:])),
// 				bigEndian.Uint64(v[40:]),
// 				bigEndian.Uint64(v[48:]),
// 				bigEndian.Uint64(v[56:]),
// 			)
// 			b.head.MaxValue, err = NewDecimal256(max256, scale)
// 			if err != nil {
// 				return fmt.Errorf("pack: min block header: %v", err)
// 			}
// 		}

// 	case BlockInt128:
// 		v := buf.Next(32)
// 		if b.Type != BlockIgnore {
// 			b.Type = typ
// 			b.Compression() = comp
// 			b.head.MinValue = NewInt128(
// 				int64(bigEndian.Uint64(v[0:])),
// 				bigEndian.Uint64(v[8:]),
// 			)
// 			b.head.MaxValue = NewInt128(
// 				int64(bigEndian.Uint64(v[16:])),
// 				bigEndian.Uint64(v[24:]),
// 			)
// 		}

// 	case BlockInt256:
// 		v := buf.Next(64)
// 		if b.Type != BlockIgnore {
// 			b.Type = typ
// 			b.Compression() = comp
// 			b.head.MinValue = NewInt256(
// 				int64(bigEndian.Uint64(v[0:])),
// 				bigEndian.Uint64(v[8:]),
// 				bigEndian.Uint64(v[16:]),
// 				bigEndian.Uint64(v[24:]),
// 			)
// 			b.head.MaxValue = NewInt256(
// 				int64(bigEndian.Uint64(v[32:])),
// 				bigEndian.Uint64(v[40:]),
// 				bigEndian.Uint64(v[48:]),
// 				bigEndian.Uint64(v[56:]),
// 			)
// 		}

// 	case BlockIgnore:

// 	default:
// 		return fmt.Errorf("pack: invalid data type %d", b.Type)
// 	}

// 	return nil
// }

// raw -> values
func (b *Block) DecodeBody(buf []byte, sz int) error {
	// skip blocks that are set to type ignore before decoding
	// this is the core magic of skipping blocks on load
	if b.Type() == BlockIgnore {
		return nil
	}

	var err error
	b.head.Type, err = readBlockType(buf)
	if err != nil {
		return err
	}

	switch b.Type() {
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
		if b.Bools == nil || cap(b.Bools) < sz {
			b.Bools = make([]bool, 0, sz)
		} else {
			b.Bools = b.Bools[:0]
		}
		b.Bools, err = decodeBoolBlock(buf, b.Bools)

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
			b.Int128 = make([]Int128, 0, sz)
		} else {
			b.Int128 = b.Int128[:0]
		}
		b.Int128, err = decodeInt128Block(buf, b.Int128)

	case BlockInt256:
		if b.Int256 == nil || cap(b.Int256) < sz {
			b.Int256 = make([]Int256, 0, sz)
		} else {
			b.Int256 = b.Int256[:0]
		}
		b.Int256, err = decodeInt256Block(buf, b.Int256)

	case BlockIgnore:

	default:
		err = fmt.Errorf("pack: invalid data type %d", b.Type)
	}
	return err
}
