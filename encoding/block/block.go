// Copyright (c) 2018-2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package block

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"time"

	"blockwatch.cc/packdb-pro/encoding/compress"
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
	BlockTime     = BlockType(0)
	BlockInteger  = BlockType(1)
	BlockUnsigned = BlockType(2)
	BlockFloat    = BlockType(3)
	BlockBool     = BlockType(4)
	BlockString   = BlockType(5)
	BlockBytes    = BlockType(6)
	BlockIgnore   = BlockType(255)
)

func (t BlockType) String() string {
	switch t {
	case BlockTime:
		return "time"
	case BlockInteger:
		return "integer"
	case BlockUnsigned:
		return "unsigned"
	case BlockFloat:
		return "float"
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
	Integers   []int64
	Unsigneds  []uint64
	Timestamps []int64
	Floats     []float64
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
			b.Timestamps = integerPool.Get().([]int64)
		} else {
			b.Timestamps = make([]int64, 0, sz)
		}
		b.MinValue = time.Time{}
		b.MaxValue = time.Time{}
	case BlockFloat:
		if sz <= DefaultMaxPointsPerBlock {
			b.Floats = floatPool.Get().([]float64)
		} else {
			b.Floats = make([]float64, 0, sz)
		}
		b.MinValue = float64(0.0)
		b.MaxValue = float64(0.0)
	case BlockInteger:
		if sz <= DefaultMaxPointsPerBlock {
			b.Integers = integerPool.Get().([]int64)
		} else {
			b.Integers = make([]int64, 0, sz)
		}
		b.MinValue = int64(0)
		b.MaxValue = int64(0)
	case BlockUnsigned:
		if sz <= DefaultMaxPointsPerBlock {
			b.Unsigneds = unsignedPool.Get().([]uint64)
		} else {
			b.Unsigneds = make([]uint64, 0, sz)
		}
		b.MinValue = uint64(0)
		b.MaxValue = uint64(0)
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
				cp.Timestamps = integerPool.Get().([]int64)[:sz]
			} else {
				cp.Timestamps = make([]int64, sz)
			}
			copy(cp.Timestamps, b.Timestamps)
			min, max := b.MinValue.(time.Time), b.MaxValue.(time.Time)
			cp.MinValue = min
			cp.MaxValue = max
		} else {
			if sz <= DefaultMaxPointsPerBlock {
				cp.Timestamps = integerPool.Get().([]int64)[:0]
			} else {
				cp.Timestamps = make([]int64, 0, sz)
			}
			cp.MinValue = time.Time{}
			cp.MaxValue = time.Time{}
		}
	case BlockFloat:
		if copydata {
			if sz <= DefaultMaxPointsPerBlock {
				cp.Floats = floatPool.Get().([]float64)[:sz]
			} else {
				cp.Floats = make([]float64, sz)
			}
			copy(cp.Floats, b.Floats)
			min, max := b.MinValue.(float64), b.MaxValue.(float64)
			cp.MinValue = min
			cp.MaxValue = max
		} else {
			if sz <= DefaultMaxPointsPerBlock {
				cp.Floats = floatPool.Get().([]float64)[:0]
			} else {
				cp.Floats = make([]float64, 0, sz)
			}
			cp.MinValue = float64(0.0)
			cp.MaxValue = float64(0.0)
		}
	case BlockInteger:
		if copydata {
			if sz <= DefaultMaxPointsPerBlock {
				cp.Integers = integerPool.Get().([]int64)[:sz]
			} else {
				cp.Integers = make([]int64, sz)
			}
			copy(cp.Integers, b.Integers)
			min, max := b.MinValue.(int64), b.MaxValue.(int64)
			cp.MinValue = min
			cp.MaxValue = max
		} else {
			if sz <= DefaultMaxPointsPerBlock {
				cp.Integers = integerPool.Get().([]int64)[:0]
			} else {
				cp.Integers = make([]int64, 0, sz)
			}
			cp.MinValue = int64(0)
			cp.MaxValue = int64(0)
		}
	case BlockUnsigned:
		if copydata {
			if sz <= DefaultMaxPointsPerBlock {
				cp.Unsigneds = unsignedPool.Get().([]uint64)[:sz]
			} else {
				cp.Unsigneds = make([]uint64, sz)
			}
			copy(cp.Unsigneds, b.Unsigneds)
			min, max := b.MinValue.(uint64), b.MaxValue.(uint64)
			cp.MinValue = min
			cp.MaxValue = max
		} else {
			if sz <= DefaultMaxPointsPerBlock {
				cp.Unsigneds = unsignedPool.Get().([]uint64)[:0]
			} else {
				cp.Unsigneds = make([]uint64, 0, sz)
			}
			cp.MinValue = uint64(0)
			cp.MaxValue = uint64(0)
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
	case BlockFloat:
		return len(b.Floats)
	case BlockInteger:
		return len(b.Integers)
	case BlockUnsigned:
		return len(b.Unsigneds)
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
	case BlockFloat:
		sz = compress.FloatArrayEncodedSize(b.Floats)
	case BlockInteger:
		sz = compress.IntegerArrayEncodedSize(b.Integers)
	case BlockUnsigned:
		sz = compress.UnsignedArrayEncodedSize(b.Unsigneds)
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
	case BlockFloat:
		return len(b.Floats)*8 + sliceSize
	case BlockInteger:
		return len(b.Integers)*8 + sliceSize
	case BlockUnsigned:
		return len(b.Unsigneds)*8 + sliceSize
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
	case BlockInteger:
		b.Integers = b.Integers[:0]
		b.MinValue = int64(0)
		b.MaxValue = int64(0)
	case BlockUnsigned:
		b.Unsigneds = b.Unsigneds[:0]
		b.MinValue = uint64(0)
		b.MaxValue = uint64(0)
	case BlockFloat:
		b.Floats = b.Floats[:0]
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
			integerPool.Put(b.Timestamps[:0])
		}
		b.Timestamps = nil
	case BlockFloat:
		if cap(b.Floats) == DefaultMaxPointsPerBlock {
			floatPool.Put(b.Floats[:0])
		}
		b.Floats = nil
	case BlockInteger:
		if cap(b.Integers) == DefaultMaxPointsPerBlock {
			integerPool.Put(b.Integers[:0])
		}
		b.Integers = nil
	case BlockUnsigned:
		if cap(b.Unsigneds) == DefaultMaxPointsPerBlock {
			unsignedPool.Put(b.Unsigneds[:0])
		}
		b.Unsigneds = nil
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

	case BlockFloat:
		min, max, err := encodeFloatBlock(buf, b.Floats, b.Compression)
		if err != nil {
			return nil, err
		}
		if b.Dirty {
			b.MinValue = min
			b.MaxValue = max
			b.Dirty = false
		}

	case BlockInteger:
		min, max, err := encodeIntegerBlock(buf, b.Integers, b.Compression)
		if err != nil {
			return nil, err
		}
		if b.Dirty {
			b.MinValue = min
			b.MaxValue = max
			b.Dirty = false
		}

	case BlockUnsigned:
		min, max, err := encodeUnsignedBlock(buf, b.Unsigneds, b.Compression)
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

	case BlockFloat:
		var v [16]byte
		min, max := b.MinValue.(float64), b.MaxValue.(float64)
		bigEndian.PutUint64(v[0:], math.Float64bits(min))
		bigEndian.PutUint64(v[8:], math.Float64bits(max))
		_, _ = buf.Write(v[:])

	case BlockInteger:
		var v [16]byte
		min, max := b.MinValue.(int64), b.MaxValue.(int64)
		bigEndian.PutUint64(v[0:], uint64(min))
		bigEndian.PutUint64(v[8:], uint64(max))
		_, _ = buf.Write(v[:])

	case BlockUnsigned:
		// always stored as uint, conversion happens only for BlockHeader
		var v [16]byte
		min, max := b.MinValue.(uint64), b.MaxValue.(uint64)
		bigEndian.PutUint64(v[0:], min)
		bigEndian.PutUint64(v[8:], max)
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

	case BlockFloat:
		v := buf.Next(16)
		if b.Type != BlockIgnore {
			b.Type = typ
			b.Compression = comp
			b.MinValue = math.Float64frombits(bigEndian.Uint64(v[0:]))
			b.MaxValue = math.Float64frombits(bigEndian.Uint64(v[8:]))
		}

	case BlockInteger:
		v := buf.Next(16)
		if b.Type != BlockIgnore {
			b.Type = typ
			b.Compression = comp
			b.MinValue = int64(bigEndian.Uint64(v[0:]))
			b.MaxValue = int64(bigEndian.Uint64(v[8:]))
		}

	case BlockUnsigned:
		v := buf.Next(16)
		if b.Type != BlockIgnore {
			b.Type = typ
			b.Compression = comp
			b.Precision = prec
			b.Flags = flags
			b.MinValue = bigEndian.Uint64(v[0:])
			b.MaxValue = bigEndian.Uint64(v[8:])
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

	case BlockFloat:
		if b.Floats == nil || cap(b.Floats) < sz {
			b.Floats = make([]float64, 0, sz)
		} else {
			b.Floats = b.Floats[:0]
		}
		b.Floats, err = decodeFloatBlock(buf, b.Floats)

	case BlockInteger:
		if b.Integers == nil || cap(b.Integers) < sz {
			b.Integers = make([]int64, 0, sz)
		} else {
			b.Integers = b.Integers[:0]
		}
		b.Integers, err = decodeIntegerBlock(buf, b.Integers)

	case BlockUnsigned:
		if b.Unsigneds == nil || cap(b.Unsigneds) < sz {
			b.Unsigneds = make([]uint64, 0, sz)
		} else {
			b.Unsigneds = b.Unsigneds[:0]
		}
		b.Unsigneds, err = decodeUnsignedBlock(buf, b.Unsigneds)

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
