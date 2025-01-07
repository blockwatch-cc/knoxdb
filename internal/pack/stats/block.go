// Copyright (c) 2023 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package stats

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"slices"

	"blockwatch.cc/knoxdb/internal/block"
	"blockwatch.cc/knoxdb/internal/filter/bloom"
	"blockwatch.cc/knoxdb/internal/types"
	"blockwatch.cc/knoxdb/internal/xroar"
	"blockwatch.cc/knoxdb/pkg/num"
	"blockwatch.cc/knoxdb/pkg/schema"
)

const (
	blockTypeMask   byte = 0x1f
	blockFilterMask byte = 0x03
)

type FilterType byte

const (
	FilterTypeNone = iota
	FilterTypeBloom
	FilterTypeBits
)

type BlockStats struct {
	Type        BlockType
	MinValue    any           // vector min
	MaxValue    any           // vector max
	Cardinality int           // unique items in vector
	Bloom       *bloom.Filter // optimized bloom filter for high cardinality data
	Bits        *xroar.Bitmap // sparse id list for low cardinality integer data
	StoredSize  int           // block size on disk
	dirty       bool          // update required
}

// EmptyBlockStats occupies minimal storage and may be used for deleted schema fields.
var EmptyBlockStats = BlockStats{
	Type:     BlockBool,
	MinValue: false,
	MaxValue: false,
}

func (m BlockStats) IsValid() bool {
	return m.Type.IsValid() && m.MinValue != nil && m.MaxValue != nil
}

func (m BlockStats) IsDirty() bool {
	return m.dirty
}

func (m *BlockStats) SetDirty() {
	m.dirty = true
}

func NewBlockStats(b *block.Block, f *schema.Field) BlockStats {
	m := BlockStats{
		Type:  b.Type(),
		dirty: b.Len() > 0,
	}

	// pk slices are sorted
	if f.Is(types.FieldFlagPrimary) {
		m.MinValue, m.MaxValue = b.FirstLast()
	} else {
		m.MinValue, m.MaxValue = b.MinMax()
	}

	// estimate cardinality, use precision 16 for 16k fixed LLB memory
	m.Cardinality = b.EstimateCardinality(16)

	// build filters
	if m.Cardinality > 2 {
		if f.Index() == types.IndexTypeBits || m.canUseBitsets() {
			// use bitsets when the range of values is small
			m.Bits = b.BuildBitsFilter(m.Cardinality)

		} else if f.Index() == types.IndexTypeBloom {
			// use bloom filters when the range of values is large (bytes, >16k range)
			// configure false positive rate via field scale factor 2..n
			m.Bloom = b.BuildBloomFilter(m.Cardinality, int(f.Scale()))
		}
	}

	return m
}

const bitsetCutoffLog2 = 7 // 128

func (m BlockStats) canUseBitsets() bool {
	var minVal, maxVal uint64
	switch m.Type {
	case BlockInt64, BlockTime:
		minVal, maxVal = uint64(m.MinValue.(int64)), uint64(m.MaxValue.(int64))

	case BlockInt32:
		minVal, maxVal = uint64(m.MinValue.(int32)), uint64(m.MaxValue.(int32))

	case BlockInt16:
		minVal, maxVal = uint64(m.MinValue.(int16)), uint64(m.MaxValue.(int16))

	case BlockUint64:
		minVal, maxVal = m.MinValue.(uint64), m.MaxValue.(uint64)

	case BlockUint32:
		minVal, maxVal = uint64(m.MinValue.(uint32)), uint64(m.MaxValue.(uint32))

	case BlockUint16:
		minVal, maxVal = uint64(m.MinValue.(uint16)), uint64(m.MaxValue.(uint16))

	case BlockInt8, BlockUint8:
		return true

	case BlockBool, BlockInt128, BlockInt256, BlockBytes, BlockFloat32, BlockFloat64:
		return false

	default:
		return false
	}

	if minVal > maxVal {
		return false
	}
	return (maxVal - minVal) < 1<<bitsetCutoffLog2
}

func (m BlockStats) EncodedSize() int {
	sz := 1 + m.StoredSize
	switch m.Type {
	case BlockInt64, BlockTime, BlockUint64, BlockFloat64:
		sz += 16
	case BlockBool:
		sz++
	case BlockInt32, BlockUint32, BlockFloat32:
		sz += 8
	case BlockInt16, BlockUint16:
		sz += 4
	case BlockInt8, BlockUint8:
		sz += 2
	case BlockInt128:
		sz += 32
	case BlockInt256:
		sz += 64
	case BlockBytes:
		min, max := m.MinValue.([]byte), m.MaxValue.([]byte)
		l1, l2 := len(min), len(max)
		var v [binary.MaxVarintLen64]byte
		i1 := binary.PutUvarint(v[:], uint64(l1))
		i2 := binary.PutUvarint(v[:], uint64(l2))
		sz += l1 + l2 + i1 + i2
	}
	if m.Bloom != nil {
		sz += len(m.Bloom.Bytes())
	}
	if m.Bits != nil {
		sz += len(m.Bits.ToBuffer())
	}
	return sz
}

func (m BlockStats) HeapSize() int {
	sz := szBlockStats
	switch m.Type {
	case BlockInt64, BlockTime, BlockUint64, BlockFloat64:
		sz += 16
	case BlockInt32, BlockUint32, BlockFloat32:
		sz += 8
	case BlockInt16, BlockUint16:
		sz += 4
	case BlockInt8, BlockUint8:
		sz += 2
	case BlockInt128:
		sz += 32
	case BlockInt256:
		sz += 64
	case BlockBytes:
		min, max := m.MinValue.([]byte), m.MaxValue.([]byte)
		l1, l2 := len(min), len(max)
		sz += l1 + l2 + 2*24
	case BlockBool:
		sz++
	}
	if m.Bloom != nil {
		sz += szBloomFilter + len(m.Bloom.Bytes())
	}
	if m.Bits != nil {
		sz += szBitset + len(m.Bits.ToBuffer())
	}
	return sz
}

func (m *BlockStats) Encode(buf *bytes.Buffer) error {
	// type encoding
	// 8                         7 6          5 4 3 2 1
	// ext header flag (unused)  filter type  block type
	typ := byte(m.Type) & blockTypeMask
	switch {
	case m.Bloom != nil:
		typ |= FilterTypeBloom << 5
	case m.Bits != nil:
		typ |= FilterTypeBits << 5
	}
	buf.WriteByte(typ)

	var b [4]byte
	// write size, 32bit
	BE.PutUint32(b[:], uint32(m.EncodedSize()))
	_, _ = buf.Write(b[:])

	// write cardinality, 32bit
	BE.PutUint32(b[:], uint32(m.Cardinality))
	_, _ = buf.Write(b[:])

	// write type-specific min/max values
	switch m.Type {
	case BlockTime, BlockInt64:
		var v [16]byte
		min, max := m.MinValue.(int64), m.MaxValue.(int64)
		BE.PutUint64(v[0:], uint64(min))
		BE.PutUint64(v[8:], uint64(max))
		_, _ = buf.Write(v[:])

	case BlockInt32:
		var v [8]byte
		min, max := m.MinValue.(int32), m.MaxValue.(int32)
		BE.PutUint32(v[0:], uint32(min))
		BE.PutUint32(v[4:], uint32(max))
		_, _ = buf.Write(v[:])

	case BlockInt16:
		var v [4]byte
		min, max := m.MinValue.(int16), m.MaxValue.(int16)
		BE.PutUint16(v[0:], uint16(min))
		BE.PutUint16(v[2:], uint16(max))
		_, _ = buf.Write(v[:])

	case BlockInt8:
		var v [2]byte
		min, max := m.MinValue.(int8), m.MaxValue.(int8)
		v[0] = uint8(min)
		v[1] = uint8(max)
		_, _ = buf.Write(v[:])

	case BlockUint64:
		var v [16]byte
		min, max := m.MinValue.(uint64), m.MaxValue.(uint64)
		BE.PutUint64(v[0:], min)
		BE.PutUint64(v[8:], max)
		_, _ = buf.Write(v[:])

	case BlockUint32:
		var v [8]byte
		min, max := m.MinValue.(uint32), m.MaxValue.(uint32)
		BE.PutUint32(v[0:], min)
		BE.PutUint32(v[4:], max)
		_, _ = buf.Write(v[:])

	case BlockUint16:
		var v [4]byte
		min, max := m.MinValue.(uint16), m.MaxValue.(uint16)
		BE.PutUint16(v[0:], min)
		BE.PutUint16(v[2:], max)
		_, _ = buf.Write(v[:])

	case BlockUint8:
		var v [2]byte
		min, max := m.MinValue.(uint8), m.MaxValue.(uint8)
		v[0] = min
		v[1] = max
		_, _ = buf.Write(v[:])

	case BlockFloat64:
		var v [16]byte
		min, max := m.MinValue.(float64), m.MaxValue.(float64)
		BE.PutUint64(v[0:], math.Float64bits(min))
		BE.PutUint64(v[8:], math.Float64bits(max))
		_, _ = buf.Write(v[:])

	case BlockFloat32:
		var v [8]byte
		min, max := m.MinValue.(float32), m.MaxValue.(float32)
		BE.PutUint32(v[0:], math.Float32bits(min))
		BE.PutUint32(v[4:], math.Float32bits(max))
		_, _ = buf.Write(v[:])

	case BlockBool:
		var v byte
		min, max := m.MinValue.(bool), m.MaxValue.(bool)
		if min {
			v = 1
		}
		if max {
			v += 2
		}
		buf.WriteByte(v)

	case BlockBytes:
		// len prefixed byte slice
		min, max := m.MinValue.([]byte), m.MaxValue.([]byte)
		var v [binary.MaxVarintLen64]byte
		i := binary.PutUvarint(v[:], uint64(len(min)))
		_, _ = buf.Write(v[:i])
		_, _ = buf.Write(min)

		i = binary.PutUvarint(v[:], uint64(len(max)))
		_, _ = buf.Write(v[:i])
		_, _ = buf.Write(max)

	case BlockInt128:
		min, max := m.MinValue.(num.Int128).Bytes16(), m.MaxValue.(num.Int128).Bytes16()
		_, _ = buf.Write(min[:])
		_, _ = buf.Write(max[:])

	case BlockInt256:
		min, max := m.MinValue.(num.Int256).Bytes32(), m.MaxValue.(num.Int256).Bytes32()
		_, _ = buf.Write(min[:])
		_, _ = buf.Write(max[:])
	}

	// write bloom and bitmap filter data
	switch {
	case m.Bloom != nil:
		data := m.Bloom.Bytes()
		BE.PutUint32(b[:], uint32(len(data)))
		_, _ = buf.Write(b[:])
		_, _ = buf.Write(data)
	case m.Bits != nil:
		data := m.Bits.ToBuffer()
		BE.PutUint32(b[:], uint32(len(data)))
		_, _ = buf.Write(b[:])
		_, _ = buf.Write(data)
	}

	m.dirty = false
	return nil
}

func (m *BlockStats) Decode(buf *bytes.Buffer, version byte) error {
	typ := buf.Next(1)
	m.Type = block.BlockType(typ[0] & blockTypeMask)
	m.dirty = false
	m.StoredSize = int(BE.Uint32(buf.Next(4)))
	m.Cardinality = int(BE.Uint32(buf.Next(4)))

	switch m.Type {
	case BlockTime, BlockInt64:
		v := buf.Next(16)
		m.MinValue, m.MaxValue = int64(BE.Uint64(v[0:])), int64(BE.Uint64(v[8:]))

	case BlockInt32:
		v := buf.Next(8)
		m.MinValue, m.MaxValue = int32(BE.Uint32(v[0:])), int32(BE.Uint32(v[4:]))

	case BlockInt16:
		v := buf.Next(4)
		m.MinValue, m.MaxValue = int16(BE.Uint16(v[0:])), int16(BE.Uint16(v[2:]))

	case BlockInt8:
		v := buf.Next(2)
		m.MinValue, m.MaxValue = int8(v[0]), int8(v[1])

	case BlockUint64:
		v := buf.Next(16)
		m.MinValue, m.MaxValue = BE.Uint64(v[0:]), BE.Uint64(v[8:])

	case BlockUint32:
		v := buf.Next(8)
		m.MinValue, m.MaxValue = BE.Uint32(v[0:]), BE.Uint32(v[4:])

	case BlockUint16:
		v := buf.Next(4)
		m.MinValue, m.MaxValue = BE.Uint16(v[0:]), BE.Uint16(v[2:])

	case BlockUint8:
		v := buf.Next(2)
		m.MinValue, m.MaxValue = v[0], v[1]

	case BlockFloat64:
		v := buf.Next(16)
		m.MinValue = math.Float64frombits(BE.Uint64(v[0:]))
		m.MaxValue = math.Float64frombits(BE.Uint64(v[8:]))

	case BlockFloat32:
		v := buf.Next(8)
		m.MinValue = math.Float32frombits(BE.Uint32(v[0:]))
		m.MaxValue = math.Float32frombits(BE.Uint32(v[4:]))

	case BlockBool:
		v := buf.Next(1)
		m.MinValue, m.MaxValue = v[0]&1 > 0, v[0]&2 > 0

	case BlockBytes:
		length, err := binary.ReadUvarint(buf)
		if err != nil {
			return fmt.Errorf("knox: reading block metadata []byte min: %w", err)
		}
		min := buf.Next(int(length))
		length, err = binary.ReadUvarint(buf)
		if err != nil {
			return fmt.Errorf("knox: reading byte block metadata []byte max: %w", err)
		}
		max := buf.Next(int(length))

		// don't reference buffer data!
		mincopy := make([]byte, len(min))
		maxcopy := make([]byte, len(max))
		copy(mincopy, min)
		copy(maxcopy, max)
		m.MinValue, m.MaxValue = mincopy, maxcopy

	case BlockInt128:
		v := buf.Next(32)
		m.MinValue, m.MaxValue = num.Int128FromBytes(v[0:16]), num.Int128FromBytes(v[16:32])

	case BlockInt256:
		v := buf.Next(64)
		m.MinValue, m.MaxValue = num.Int256FromBytes(v[0:32]), num.Int256FromBytes(v[32:64])

	default:
		return fmt.Errorf("knox: invalid block type %d", m.Type)
	}

	// read filter data
	switch FilterType((typ[0] >> 5) & blockFilterMask) {
	case FilterTypeBloom:
		// read filter size
		sz := int(BE.Uint32(buf.Next(4)))
		b := buf.Next(sz)
		if len(b) < sz {
			return fmt.Errorf("knox: reading bloom filter: %v", io.ErrShortBuffer)
		}
		// copy data to avoid referencing memory
		bloom, err := bloom.NewFilterBuffer(slices.Clone(b))
		if err != nil {
			return fmt.Errorf("knox: reading bloom filter data: %w", err)
		}
		m.Bloom = bloom
	case FilterTypeBits:
		// read filter size
		sz := int(BE.Uint32(buf.Next(4)))
		b := buf.Next(sz)
		if len(b) < sz {
			return fmt.Errorf("knox: reading bitmap filter: %v", io.ErrShortBuffer)
		}
		m.Bits = xroar.FromBufferWithCopy(b)
	}

	return nil
}
