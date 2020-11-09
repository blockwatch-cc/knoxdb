// Copyright (c) 2018-2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package block

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"time"

	"blockwatch.cc/knoxdb/encoding/compress"
)

var bigEndian = binary.BigEndian

type BlockFlags byte

const (
	BlockFlagConvert BlockFlags = 1 << iota
	BlockFlagCompress
)

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
	case BlockIgnore:
		return "ignore"
	default:
		return "invalid block type"
	}
}

type Block struct {
	Type        BlockType
	Compression Compression
	Precision   int
	Flags       BlockFlags
	MinValue    interface{}
	MaxValue    interface{}
	Dirty       bool

	Strings    []string
	Bytes      [][]byte
	Bools      []bool
	Int64      []int64
	Int32      []int32
	Int16      []int16
	Int8       []int8
	Uint64     []uint64
	Uint32     []uint32
	Uint16     []uint16
	Uint8      []uint8
	Timestamps []int64
	Float64    []float64
	Float32    []float32
}

func NewBlock(typ BlockType, sz int, comp Compression, prec int, flags BlockFlags) (*Block, error) {
	b := &Block{
		Type:        typ,
		Compression: comp,
		Precision:   prec,
		Flags:       flags,
	}
	switch typ {
	case BlockTime:
		if sz <= DefaultMaxPointsPerBlock {
			b.Timestamps = int64Pool.Get().([]int64)
		} else {
			b.Timestamps = make([]int64, 0, sz)
		}
		b.MinValue = time.Time{}
		b.MaxValue = time.Time{}
	case BlockFloat64:
		if sz <= DefaultMaxPointsPerBlock {
			b.Float64 = float64Pool.Get().([]float64)
		} else {
			b.Float64 = make([]float64, 0, sz)
		}
		b.MinValue = float64(0.0)
		b.MaxValue = float64(0.0)
	case BlockFloat32:
		if sz <= DefaultMaxPointsPerBlock {
			b.Float32 = float32Pool.Get().([]float32)
		} else {
			b.Float32 = make([]float32, 0, sz)
		}
		b.MinValue = float32(0.0)
		b.MaxValue = float32(0.0)
	case BlockInt64:
		if sz <= DefaultMaxPointsPerBlock {
			b.Int64 = int64Pool.Get().([]int64)
		} else {
			b.Int64 = make([]int64, 0, sz)
		}
		b.MinValue = int64(0)
		b.MaxValue = int64(0)
	case BlockInt32:
		if sz <= DefaultMaxPointsPerBlock {
			b.Int32 = int32Pool.Get().([]int32)
		} else {
			b.Int32 = make([]int32, 0, sz)
		}
		b.MinValue = int32(0)
		b.MaxValue = int32(0)
	case BlockInt16:
		if sz <= DefaultMaxPointsPerBlock {
			b.Int16 = int16Pool.Get().([]int16)
		} else {
			b.Int16 = make([]int16, 0, sz)
		}
		b.MinValue = int16(0)
		b.MaxValue = int16(0)
	case BlockInt8:
		if sz <= DefaultMaxPointsPerBlock {
			b.Int8 = int8Pool.Get().([]int8)
		} else {
			b.Int8 = make([]int8, 0, sz)
		}
		b.MinValue = int8(0)
		b.MaxValue = int8(0)
	case BlockUint64:
		if sz <= DefaultMaxPointsPerBlock {
			b.Uint64 = uint64Pool.Get().([]uint64)
		} else {
			b.Uint64 = make([]uint64, 0, sz)
		}
		b.MinValue = uint64(0)
		b.MaxValue = uint64(0)
	case BlockUint32:
		if sz <= DefaultMaxPointsPerBlock {
			b.Uint32 = uint32Pool.Get().([]uint32)
		} else {
			b.Uint32 = make([]uint32, 0, sz)
		}
		b.MinValue = uint32(0)
		b.MaxValue = uint32(0)
	case BlockUint16:
		if sz <= DefaultMaxPointsPerBlock {
			b.Uint16 = uint16Pool.Get().([]uint16)
		} else {
			b.Uint16 = make([]uint16, 0, sz)
		}
		b.MinValue = uint16(0)
		b.MaxValue = uint16(0)
	case BlockUint8:
		if sz <= DefaultMaxPointsPerBlock {
			b.Uint8 = uint8Pool.Get().([]uint8)
		} else {
			b.Uint8 = make([]uint8, 0, sz)
		}
		b.MinValue = uint8(0)
		b.MaxValue = uint8(0)
	case BlockBool:
		if sz <= DefaultMaxPointsPerBlock {
			b.Bools = boolPool.Get().([]bool)
		} else {
			b.Bools = make([]bool, 0, sz)
		}
		b.MinValue = false
		b.MaxValue = false
	case BlockString:
		if sz <= DefaultMaxPointsPerBlock {
			b.Strings = stringPool.Get().([]string)
		} else {
			b.Strings = make([]string, 0, sz)
		}
		b.MinValue = ""
		b.MaxValue = ""
	case BlockBytes:
		if sz <= DefaultMaxPointsPerBlock {
			b.Bytes = bytesPool.Get().([][]byte)
		} else {
			b.Bytes = make([][]byte, 0, sz)
		}
		b.MinValue = []byte{}
		b.MaxValue = []byte{}
	default:
		return nil, fmt.Errorf("pack: invalid data type %d", b.Type)
	}
	return b, nil
}

func (b *Block) Clone(sz int, copydata bool) (*Block, error) {
	cp := &Block{
		Type:        b.Type,
		Compression: b.Compression,
		Precision:   b.Precision,
		Flags:       b.Flags,
	}
	switch cp.Type {
	case BlockTime:
		if copydata {
			if sz <= DefaultMaxPointsPerBlock {
				cp.Timestamps = int64Pool.Get().([]int64)[:sz]
			} else {
				cp.Timestamps = make([]int64, sz)
			}
			copy(cp.Timestamps, b.Timestamps)
			min, max := b.MinValue.(time.Time), b.MaxValue.(time.Time)
			cp.MinValue = min
			cp.MaxValue = max
		} else {
			if sz <= DefaultMaxPointsPerBlock {
				cp.Timestamps = int64Pool.Get().([]int64)[:0]
			} else {
				cp.Timestamps = make([]int64, 0, sz)
			}
			cp.MinValue = time.Time{}
			cp.MaxValue = time.Time{}
		}
	case BlockFloat64:
		if copydata {
			if sz <= DefaultMaxPointsPerBlock {
				cp.Float64 = float64Pool.Get().([]float64)[:sz]
			} else {
				cp.Float64 = make([]float64, sz)
			}
			copy(cp.Float64, b.Float64)
			min, max := b.MinValue.(float64), b.MaxValue.(float64)
			cp.MinValue = min
			cp.MaxValue = max
		} else {
			if sz <= DefaultMaxPointsPerBlock {
				cp.Float64 = float64Pool.Get().([]float64)[:0]
			} else {
				cp.Float64 = make([]float64, 0, sz)
			}
			cp.MinValue = float64(0.0)
			cp.MaxValue = float64(0.0)
		}
	case BlockFloat32:
		if copydata {
			if sz <= DefaultMaxPointsPerBlock {
				cp.Float32 = float32Pool.Get().([]float32)[:sz]
			} else {
				cp.Float32 = make([]float32, sz)
			}
			copy(cp.Float32, b.Float32)
			min, max := b.MinValue.(float32), b.MaxValue.(float32)
			cp.MinValue = min
			cp.MaxValue = max
		} else {
			if sz <= DefaultMaxPointsPerBlock {
				cp.Float32 = float32Pool.Get().([]float32)[:0]
			} else {
				cp.Float32 = make([]float32, 0, sz)
			}
			cp.MinValue = float32(0.0)
			cp.MaxValue = float32(0.0)
		}
	case BlockInt64:
		if copydata {
			if sz <= DefaultMaxPointsPerBlock {
				cp.Int64 = int64Pool.Get().([]int64)[:sz]
			} else {
				cp.Int64 = make([]int64, sz)
			}
			copy(cp.Int64, b.Int64)
			min, max := b.MinValue.(int64), b.MaxValue.(int64)
			cp.MinValue = min
			cp.MaxValue = max
		} else {
			if sz <= DefaultMaxPointsPerBlock {
				cp.Int64 = int64Pool.Get().([]int64)[:0]
			} else {
				cp.Int64 = make([]int64, 0, sz)
			}
			cp.MinValue = int64(0)
			cp.MaxValue = int64(0)
		}
	case BlockInt32:
		if copydata {
			if sz <= DefaultMaxPointsPerBlock {
				cp.Int32 = int32Pool.Get().([]int32)[:sz]
			} else {
				cp.Int32 = make([]int32, sz)
			}
			copy(cp.Int32, b.Int32)
			min, max := b.MinValue.(int32), b.MaxValue.(int32)
			cp.MinValue = min
			cp.MaxValue = max
		} else {
			if sz <= DefaultMaxPointsPerBlock {
				cp.Int32 = int32Pool.Get().([]int32)[:0]
			} else {
				cp.Int32 = make([]int32, 0, sz)
			}
			cp.MinValue = int32(0)
			cp.MaxValue = int32(0)
		}
	case BlockInt16:
		if copydata {
			if sz <= DefaultMaxPointsPerBlock {
				cp.Int16 = int16Pool.Get().([]int16)[:sz]
			} else {
				cp.Int16 = make([]int16, sz)
			}
			copy(cp.Int16, b.Int16)
			min, max := b.MinValue.(int16), b.MaxValue.(int16)
			cp.MinValue = min
			cp.MaxValue = max
		} else {
			if sz <= DefaultMaxPointsPerBlock {
				cp.Int16 = int16Pool.Get().([]int16)[:0]
			} else {
				cp.Int16 = make([]int16, 0, sz)
			}
			cp.MinValue = int16(0)
			cp.MaxValue = int16(0)
		}
	case BlockInt8:
		if copydata {
			if sz <= DefaultMaxPointsPerBlock {
				cp.Int8 = int8Pool.Get().([]int8)[:sz]
			} else {
				cp.Int8 = make([]int8, sz)
			}
			copy(cp.Int8, b.Int8)
			min, max := b.MinValue.(int8), b.MaxValue.(int8)
			cp.MinValue = min
			cp.MaxValue = max
		} else {
			if sz <= DefaultMaxPointsPerBlock {
				cp.Int8 = int8Pool.Get().([]int8)[:0]
			} else {
				cp.Int8 = make([]int8, 0, sz)
			}
			cp.MinValue = int8(0)
			cp.MaxValue = int8(0)
		}
	case BlockUint64:
		if copydata {
			if sz <= DefaultMaxPointsPerBlock {
				cp.Uint64 = uint64Pool.Get().([]uint64)[:sz]
			} else {
				cp.Uint64 = make([]uint64, sz)
			}
			copy(cp.Uint64, b.Uint64)
			min, max := b.MinValue.(uint64), b.MaxValue.(uint64)
			cp.MinValue = min
			cp.MaxValue = max
		} else {
			if sz <= DefaultMaxPointsPerBlock {
				cp.Uint64 = uint64Pool.Get().([]uint64)[:0]
			} else {
				cp.Uint64 = make([]uint64, 0, sz)
			}
			cp.MinValue = uint64(0)
			cp.MaxValue = uint64(0)
		}
	case BlockUint32:
		if copydata {
			if sz <= DefaultMaxPointsPerBlock {
				cp.Uint32 = uint32Pool.Get().([]uint32)[:sz]
			} else {
				cp.Uint32 = make([]uint32, sz)
			}
			copy(cp.Uint32, b.Uint32)
			min, max := b.MinValue.(uint32), b.MaxValue.(uint32)
			cp.MinValue = min
			cp.MaxValue = max
		} else {
			if sz <= DefaultMaxPointsPerBlock {
				cp.Uint32 = uint32Pool.Get().([]uint32)[:0]
			} else {
				cp.Uint32 = make([]uint32, 0, sz)
			}
			cp.MinValue = uint32(0)
			cp.MaxValue = uint32(0)
		}
	case BlockUint16:
		if copydata {
			if sz <= DefaultMaxPointsPerBlock {
				cp.Uint16 = uint16Pool.Get().([]uint16)[:sz]
			} else {
				cp.Uint16 = make([]uint16, sz)
			}
			copy(cp.Uint16, b.Uint16)
			min, max := b.MinValue.(uint16), b.MaxValue.(uint16)
			cp.MinValue = min
			cp.MaxValue = max
		} else {
			if sz <= DefaultMaxPointsPerBlock {
				cp.Uint16 = uint16Pool.Get().([]uint16)[:0]
			} else {
				cp.Uint16 = make([]uint16, 0, sz)
			}
			cp.MinValue = uint16(0)
			cp.MaxValue = uint16(0)
		}
	case BlockUint8:
		if copydata {
			if sz <= DefaultMaxPointsPerBlock {
				cp.Uint8 = uint8Pool.Get().([]uint8)[:sz]
			} else {
				cp.Uint8 = make([]uint8, sz)
			}
			copy(cp.Uint8, b.Uint8)
			min, max := b.MinValue.(uint8), b.MaxValue.(uint8)
			cp.MinValue = min
			cp.MaxValue = max
		} else {
			if sz <= DefaultMaxPointsPerBlock {
				cp.Uint8 = uint8Pool.Get().([]uint8)[:0]
			} else {
				cp.Uint8 = make([]uint8, 0, sz)
			}
			cp.MinValue = uint8(0)
			cp.MaxValue = uint8(0)
		}
	case BlockBool:
		if copydata {
			if sz <= DefaultMaxPointsPerBlock {
				cp.Bools = boolPool.Get().([]bool)[:sz]
			} else {
				cp.Bools = make([]bool, sz)
			}
			copy(cp.Bools, b.Bools)
			min, max := b.MinValue.(bool), b.MaxValue.(bool)
			cp.MinValue = min
			cp.MaxValue = max
		} else {
			if sz <= DefaultMaxPointsPerBlock {
				cp.Bools = boolPool.Get().([]bool)[:0]
			} else {
				cp.Bools = make([]bool, 0, sz)
			}
			cp.MinValue = false
			cp.MaxValue = false
		}

	case BlockString:
		if copydata {
			if sz <= DefaultMaxPointsPerBlock {
				cp.Strings = stringPool.Get().([]string)[:sz]
			} else {
				cp.Strings = make([]string, sz)
			}
			copy(cp.Strings, b.Strings)
			min, max := b.MinValue.(string), b.MaxValue.(string)
			cp.MinValue = min
			cp.MaxValue = max
		} else {
			if sz <= DefaultMaxPointsPerBlock {
				cp.Strings = stringPool.Get().([]string)[:0]
			} else {
				cp.Strings = make([]string, 0, sz)
			}
			cp.MinValue = ""
			cp.MaxValue = ""
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
			min := make([]byte, len(b.MinValue.([]byte)))
			max := make([]byte, len(b.MaxValue.([]byte)))
			copy(min, b.MinValue.([]byte))
			copy(max, b.MaxValue.([]byte))
			cp.MinValue = min
			cp.MaxValue = max
		} else {
			if sz <= DefaultMaxPointsPerBlock {
				cp.Bytes = bytesPool.Get().([][]byte)[:0]
			} else {
				cp.Bytes = make([][]byte, 0, sz)
			}
			cp.MinValue = []byte{}
			cp.MaxValue = []byte{}
		}
	default:
		return nil, fmt.Errorf("pack: invalid data type %d", b.Type)
	}
	return cp, nil
}

func (b *Block) Len() int {
	switch b.Type {
	case BlockTime:
		return len(b.Timestamps)
	case BlockFloat64:
		return len(b.Float64)
	case BlockFloat32:
		return len(b.Float32)
	case BlockInt64:
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
	switch b.Type {
	case BlockTime:
		sz = compress.TimeArrayEncodedSize(b.Timestamps)
	case BlockFloat64:
		sz = compress.Float64ArrayEncodedSize(b.Float64)
	case BlockFloat32:
		sz = compress.Float32ArrayEncodedSize(b.Float32)
	case BlockInt64:
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
	default:
		return 0
	}
	return sz + encodedBlockHeaderSize + b.Compression.HeaderSize(sz)
}

func (b *Block) Size() int {
	const (
		sliceSize  = 24 // reflect.SliceHeader incl. padding
		stringSize = 16 // reflect.StringHeader incl. padding
	)
	switch b.Type {
	case BlockTime:
		return len(b.Timestamps)*8 + sliceSize
	case BlockFloat64:
		return len(b.Float64)*8 + sliceSize
	case BlockFloat32:
		return len(b.Float32)*4 + sliceSize
	case BlockInt64:
		return len(b.Int64)*8 + sliceSize
	case BlockInt32:
		return len(b.Int32)*4 + sliceSize
	case BlockInt16:
		return len(b.Int16)*2 + sliceSize
	case BlockInt8:
		return len(b.Int8) + sliceSize
	case BlockUint64:
		return len(b.Uint64)*8 + sliceSize
	case BlockUint32:
		return len(b.Uint32)*4 + sliceSize
	case BlockUint16:
		return len(b.Uint16)*2 + sliceSize
	case BlockUint8:
		return len(b.Uint8) + sliceSize
	case BlockBool:
		return len(b.Bools)*1 + sliceSize
	case BlockString:
		var sz int
		for _, v := range b.Strings {
			sz += len(v) + stringSize
		}
		return sz + sliceSize
	case BlockBytes:
		var sz int
		for _, v := range b.Bytes {
			sz += len(v) + sliceSize
		}
		return sz + sliceSize
	default:
		return 0
	}
}

func (b *Block) Clear() {
	switch b.Type {
	case BlockInt64:
		b.Int64 = b.Int64[:0]
		b.MinValue = int64(0)
		b.MaxValue = int64(0)
	case BlockInt32:
		b.Int32 = b.Int32[:0]
		b.MinValue = int32(0)
		b.MaxValue = int32(0)
	case BlockInt16:
		b.Int16 = b.Int16[:0]
		b.MinValue = int16(0)
		b.MaxValue = int16(0)
	case BlockInt8:
		b.Int8 = b.Int8[:0]
		b.MinValue = int8(0)
		b.MaxValue = int8(0)
	case BlockUint64:
		b.Uint64 = b.Uint64[:0]
		b.MinValue = uint64(0)
		b.MaxValue = uint64(0)
	case BlockUint32:
		b.Uint32 = b.Uint32[:0]
		b.MinValue = uint32(0)
		b.MaxValue = uint32(0)
	case BlockUint16:
		b.Uint16 = b.Uint16[:0]
		b.MinValue = uint16(0)
		b.MaxValue = uint16(0)
	case BlockUint8:
		b.Uint8 = b.Uint8[:0]
		b.MinValue = uint8(0)
		b.MaxValue = uint8(0)
	case BlockFloat64:
		b.Float64 = b.Float64[:0]
		b.MinValue = float64(0.0)
		b.MaxValue = float64(0.0)
	case BlockFloat32:
		b.Float32 = b.Float32[:0]
		b.MinValue = float64(0.0)
		b.MaxValue = float64(0.0)
	case BlockString:
		for j, _ := range b.Strings {
			b.Strings[j] = ""
		}
		b.Strings = b.Strings[:0]
		b.MinValue = ""
		b.MaxValue = ""
	case BlockBytes:
		for j, _ := range b.Bytes {
			b.Bytes[j] = nil
		}
		b.Bytes = b.Bytes[:0]
		b.MinValue = []byte{}
		b.MaxValue = []byte{}
	case BlockBool:
		b.Bools = b.Bools[:0]
		b.MinValue = false
		b.MaxValue = false
	case BlockTime:
		b.Timestamps = b.Timestamps[:0]
		b.MinValue = time.Time{}
		b.MaxValue = time.Time{}
	}
	b.Dirty = false
}

func (b *Block) Release() {
	b.MinValue = nil
	b.MaxValue = nil
	b.Dirty = false
	switch b.Type {
	case BlockTime:
		if cap(b.Timestamps) == DefaultMaxPointsPerBlock {
			int64Pool.Put(b.Timestamps[:0])
		}
		b.Timestamps = nil
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
	case BlockInt64:
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
	case BlockIgnore:
	}
}

func (b *Block) Encode() ([]byte, []byte, error) {
	body, err := b.EncodeBody()
	if err != nil {
		return nil, nil, err
	}
	// encode header second, to allow for reparsing min/max values during encode
	head, err := b.EncodeHeader()
	if err != nil {
		return nil, nil, err
	}
	return head, body, nil
}

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

	switch b.Type {
	case BlockTime:
		min, max, err := encodeTimeBlock(buf, b.Timestamps, b.Compression)
		if err != nil {
			return nil, err
		}
		if b.Dirty {
			b.MinValue = time.Unix(0, min).UTC()
			b.MaxValue = time.Unix(0, max).UTC()
			b.Dirty = false
		}

	case BlockFloat64:
		min, max, err := encodeFloat64Block(buf, b.Float64, b.Compression)
		if err != nil {
			return nil, err
		}
		if b.Dirty {
			b.MinValue = min
			b.MaxValue = max
			b.Dirty = false
		}

	case BlockFloat32:
		min, max, err := encodeFloat32Block(buf, b.Float32, b.Compression)
		if err != nil {
			return nil, err
		}
		if b.Dirty {
			b.MinValue = min
			b.MaxValue = max
			b.Dirty = false
		}

	case BlockInt64:
		min, max, err := encodeInt64Block(buf, b.Int64, b.Compression)
		if err != nil {
			return nil, err
		}
		if b.Dirty {
			b.MinValue = min
			b.MaxValue = max
			b.Dirty = false
		}

	case BlockInt32:
		min, max, err := encodeInt32Block(buf, b.Int32, b.Compression)
		if err != nil {
			return nil, err
		}
		if b.Dirty {
			b.MinValue = min
			b.MaxValue = max
			b.Dirty = false
		}

	case BlockInt16:
		min, max, err := encodeInt16Block(buf, b.Int16, b.Compression)
		if err != nil {
			return nil, err
		}
		if b.Dirty {
			b.MinValue = min
			b.MaxValue = max
			b.Dirty = false
		}
	case BlockInt8:
		min, max, err := encodeInt8Block(buf, b.Int8, b.Compression)
		if err != nil {
			return nil, err
		}
		if b.Dirty {
			b.MinValue = min
			b.MaxValue = max
			b.Dirty = false
		}

	case BlockUint64:
		min, max, err := encodeUint64Block(buf, b.Uint64, b.Compression)
		if err != nil {
			return nil, err
		}
		if b.Dirty {
			b.MinValue = min
			b.MaxValue = max
			b.Dirty = false
		}

	case BlockUint32:
		min, max, err := encodeUint32Block(buf, b.Uint32, b.Compression)
		if err != nil {
			return nil, err
		}
		if b.Dirty {
			b.MinValue = min
			b.MaxValue = max
			b.Dirty = false
		}

	case BlockUint16:
		min, max, err := encodeUint16Block(buf, b.Uint16, b.Compression)
		if err != nil {
			return nil, err
		}
		if b.Dirty {
			b.MinValue = min
			b.MaxValue = max
			b.Dirty = false
		}

	case BlockUint8:
		min, max, err := encodeUint8Block(buf, b.Uint8, b.Compression)
		if err != nil {
			return nil, err
		}
		if b.Dirty {
			b.MinValue = min
			b.MaxValue = max
			b.Dirty = false
		}

	case BlockBool:
		min, max, err := encodeBoolBlock(buf, b.Bools, b.Compression)
		if err != nil {
			return nil, err
		}
		if b.Dirty {
			b.MinValue = min
			b.MaxValue = max
			b.Dirty = false
		}

	case BlockString:
		min, max, err := encodeStringBlock(buf, b.Strings, b.Compression)
		if err != nil {
			return nil, err
		}
		if b.Dirty {
			b.MinValue = min
			b.MaxValue = max
			b.Dirty = false
		}

	case BlockBytes:
		min, max, err := encodeBytesBlock(buf, b.Bytes, b.Compression)
		if err != nil {
			return nil, err
		}
		if b.Dirty {
			b.MinValue = min
			b.MaxValue = max
			b.Dirty = false
		}

	case BlockIgnore:
		b.Dirty = false
		return nil, nil

	default:
		return nil, fmt.Errorf("pack: invalid data type %d", b.Type)
	}
	return buf.Bytes(), nil
}

func (b *Block) EncodeHeader() ([]byte, error) {
	// re-use large enough buffer
	buf := bytes.NewBuffer(BlockEncoderPool.Get().([]byte)[:0])

	// 8                 7 6          5 4 3 2 1
	// ext header flag   compression  block type
	buf.WriteByte(byte(b.Type&0x1f) | byte(b.Compression&0x3)<<5 | 0x80)

	// extension header
	// - 4 lower bits are used for storing precision
	// - 4 upper bits are flags
	buf.WriteByte((byte(b.Flags)&0xf)<<4 | byte(b.Precision)&0xf)

	switch b.Type {
	case BlockTime:
		var v [16]byte
		min, max := b.MinValue.(time.Time), b.MaxValue.(time.Time)
		vmin, vmax := min.UnixNano(), max.UnixNano()
		bigEndian.PutUint64(v[0:], uint64(vmin))
		bigEndian.PutUint64(v[8:], uint64(vmax))
		_, _ = buf.Write(v[:])

	case BlockFloat64:
		var v [16]byte
		min, max := b.MinValue.(float64), b.MaxValue.(float64)
		bigEndian.PutUint64(v[0:], math.Float64bits(min))
		bigEndian.PutUint64(v[8:], math.Float64bits(max))
		_, _ = buf.Write(v[:])

	case BlockFloat32:
		var v [8]byte
		min, max := b.MinValue.(float32), b.MaxValue.(float32)
		bigEndian.PutUint32(v[0:], math.Float32bits(min))
		bigEndian.PutUint32(v[4:], math.Float32bits(max))
		_, _ = buf.Write(v[:])

	case BlockInt64:
		var v [16]byte
		min, max := b.MinValue.(int64), b.MaxValue.(int64)
		bigEndian.PutUint64(v[0:], uint64(min))
		bigEndian.PutUint64(v[8:], uint64(max))
		_, _ = buf.Write(v[:])

	case BlockInt32:
		var v [8]byte
		min, max := b.MinValue.(int32), b.MaxValue.(int32)
		bigEndian.PutUint32(v[0:], uint32(min))
		bigEndian.PutUint32(v[4:], uint32(max))
		_, _ = buf.Write(v[:])

	case BlockInt16:
		var v [4]byte
		min, max := b.MinValue.(int16), b.MaxValue.(int16)
		bigEndian.PutUint16(v[0:], uint16(min))
		bigEndian.PutUint16(v[2:], uint16(max))
		_, _ = buf.Write(v[:])

	case BlockInt8:
		var v [2]byte
		min, max := b.MinValue.(int8), b.MaxValue.(int8)
		v[0] = uint8(min)
		v[1] = uint8(max)
		_, _ = buf.Write(v[:])

	case BlockUint64:
		// always stored as uint, conversion happens only for BlockHeader
		var v [16]byte
		min, max := b.MinValue.(uint64), b.MaxValue.(uint64)
		bigEndian.PutUint64(v[0:], min)
		bigEndian.PutUint64(v[8:], max)
		_, _ = buf.Write(v[:])

	case BlockUint32:
		// always stored as uint, conversion happens only for BlockHeader
		var v [8]byte
		min, max := b.MinValue.(uint32), b.MaxValue.(uint32)
		bigEndian.PutUint32(v[0:], min)
		bigEndian.PutUint32(v[4:], max)
		_, _ = buf.Write(v[:])

	case BlockUint16:
		// always stored as uint, conversion happens only for BlockHeader
		var v [4]byte
		min, max := b.MinValue.(uint16), b.MaxValue.(uint16)
		bigEndian.PutUint16(v[0:], min)
		bigEndian.PutUint16(v[2:], max)
		_, _ = buf.Write(v[:])

	case BlockUint8:
		// always stored as uint, conversion happens only for BlockHeader
		var v [2]byte
		min, max := b.MinValue.(uint8), b.MaxValue.(uint8)
		v[0] = min
		v[1] = max
		_, _ = buf.Write(v[:])

	case BlockBool:
		var v byte
		min, max := b.MinValue.(bool), b.MaxValue.(bool)
		if min {
			v = 1
		}
		if max {
			v += 2
		}
		buf.WriteByte(v)

	case BlockString:
		// null terminated string
		min, max := b.MinValue.(string), b.MaxValue.(string)
		_, _ = buf.WriteString(min)
		buf.WriteByte(0)
		_, _ = buf.WriteString(max)
		buf.WriteByte(0)

	case BlockBytes:
		// len prefixed byte slice
		min, max := b.MinValue.([]byte), b.MaxValue.([]byte)
		var v [8]byte
		i := binary.PutUvarint(v[:], uint64(len(min)))
		_, _ = buf.Write(v[:i])
		_, _ = buf.Write(min)

		i = binary.PutUvarint(v[:], uint64(len(max)))
		_, _ = buf.Write(v[:i])
		_, _ = buf.Write(max)

	case BlockIgnore:
		return nil, nil

	default:
		return nil, fmt.Errorf("pack: invalid data type %d", b.Type)
	}

	return buf.Bytes(), nil
}

func (b *Block) DecodeHeader(buf *bytes.Buffer) error {
	val := buf.Next(1)
	typ, err := readBlockType(val)
	if err != nil {
		return err
	}
	comp, err := readBlockCompression(val)
	if err != nil {
		return err
	}

	var (
		prec  int
		flags BlockFlags
	)

	if val[0]&0x80 > 0 {
		val = buf.Next(1)
		prec = readBlockPrecision(val)
		flags = readBlockFlags(val)
	}

	switch typ {
	case BlockTime:
		v := buf.Next(16)
		vmin := bigEndian.Uint64(v[0:])
		vmax := bigEndian.Uint64(v[8:])
		if b.Type != BlockIgnore {
			b.Type = typ
			b.Compression = comp
			b.MinValue = time.Unix(0, int64(vmin)).UTC()
			b.MaxValue = time.Unix(0, int64(vmax)).UTC()
		}

	case BlockFloat64:
		v := buf.Next(16)
		if b.Type != BlockIgnore {
			b.Type = typ
			b.Compression = comp
			b.MinValue = math.Float64frombits(bigEndian.Uint64(v[0:]))
			b.MaxValue = math.Float64frombits(bigEndian.Uint64(v[8:]))
		}

	case BlockFloat32:
		v := buf.Next(8)
		if b.Type != BlockIgnore {
			b.Type = typ
			b.Compression = comp
			b.MinValue = math.Float32frombits(bigEndian.Uint32(v[0:]))
			b.MaxValue = math.Float32frombits(bigEndian.Uint32(v[4:]))
		}

	case BlockInt64:
		v := buf.Next(16)
		if b.Type != BlockIgnore {
			b.Type = typ
			b.Compression = comp
			b.MinValue = int64(bigEndian.Uint64(v[0:]))
			b.MaxValue = int64(bigEndian.Uint64(v[8:]))
		}

	case BlockInt32:
		v := buf.Next(8)
		if b.Type != BlockIgnore {
			b.Type = typ
			b.Compression = comp
			b.MinValue = int32(bigEndian.Uint32(v[0:]))
			b.MaxValue = int32(bigEndian.Uint32(v[4:]))
		}

	case BlockInt16:
		v := buf.Next(4)
		if b.Type != BlockIgnore {
			b.Type = typ
			b.Compression = comp
			b.MinValue = int16(bigEndian.Uint16(v[0:]))
			b.MaxValue = int16(bigEndian.Uint16(v[2:]))
		}

	case BlockInt8:
		v := buf.Next(2)
		if b.Type != BlockIgnore {
			b.Type = typ
			b.Compression = comp
			b.MinValue = int8(v[0])
			b.MaxValue = int8(v[1])
		}

	case BlockUint64:
		v := buf.Next(16)
		if b.Type != BlockIgnore {
			b.Type = typ
			b.Compression = comp
			b.Precision = prec
			b.Flags = flags
			b.MinValue = bigEndian.Uint64(v[0:])
			b.MaxValue = bigEndian.Uint64(v[8:])
		}

	case BlockUint32:
		v := buf.Next(8)
		if b.Type != BlockIgnore {
			b.Type = typ
			b.Compression = comp
			b.MinValue = bigEndian.Uint32(v[0:])
			b.MaxValue = bigEndian.Uint32(v[4:])
		}

	case BlockUint16:
		v := buf.Next(4)
		if b.Type != BlockIgnore {
			b.Type = typ
			b.Compression = comp
			b.MinValue = bigEndian.Uint16(v[0:])
			b.MaxValue = bigEndian.Uint16(v[2:])
		}

	case BlockUint8:
		v := buf.Next(2)
		if b.Type != BlockIgnore {
			b.Type = typ
			b.Compression = comp
			b.MinValue = uint8(v[0])
			b.MaxValue = uint8(v[1])
		}

	case BlockBool:
		v := buf.Next(1)
		if b.Type != BlockIgnore {
			b.Type = typ
			b.Compression = comp
			b.MinValue = v[0]&1 > 0
			b.MaxValue = v[0]&2 > 0
		}

	case BlockString:
		min, err := buf.ReadString(0)
		if err != nil {
			return fmt.Errorf("pack: reading min string block header: %v", err)
		}
		max, err := buf.ReadString(0)
		if err != nil {
			return fmt.Errorf("pack: reading max string block header: %v", err)
		}
		if b.Type != BlockIgnore {
			b.Type = typ
			b.Compression = comp
			// don't reference buffer data!
			mincopy := min[:len(min)-1]
			maxcopy := max[:len(max)-1]
			b.MinValue = mincopy
			b.MaxValue = maxcopy
		}

	case BlockBytes:
		length, err := binary.ReadUvarint(buf)
		if err != nil {
			return fmt.Errorf("pack: reading min []byte block header: %v", err)
		}
		min := buf.Next(int(length))
		length, err = binary.ReadUvarint(buf)
		if err != nil {
			return fmt.Errorf("pack: reading max []byte block header: %v", err)
		}
		max := buf.Next(int(length))

		if b.Type != BlockIgnore {
			b.Type = typ
			b.Compression = comp
			// don't reference buffer data!
			mincopy := make([]byte, len(min))
			maxcopy := make([]byte, len(max))
			copy(mincopy, min)
			copy(maxcopy, max)
			b.MinValue = mincopy
			b.MaxValue = maxcopy
		}

	case BlockIgnore:

	default:
		return fmt.Errorf("pack: invalid data type %d", b.Type)
	}

	return nil
}

// raw -> values
func (b *Block) DecodeBody(buf []byte, sz int) error {
	// skip blocks that are set to type ignore before decoding
	// this is the core magic of skipping blocks on load
	if b.Type == BlockIgnore {
		return nil
	}

	var err error
	b.Type, err = readBlockType(buf)
	if err != nil {
		return err
	}

	switch b.Type {
	case BlockTime:
		if b.Timestamps == nil || cap(b.Timestamps) < sz {
			b.Timestamps = make([]int64, 0, sz)
		} else {
			b.Timestamps = b.Timestamps[:0]
		}
		b.Timestamps, err = decodeTimeBlock(buf, b.Timestamps)

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

	case BlockIgnore:

	default:
		err = fmt.Errorf("pack: invalid data type %d", b.Type)
	}
	return err
}
