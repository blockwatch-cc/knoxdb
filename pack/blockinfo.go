// Copyright (c) 2018-2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package pack

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"time"

	"blockwatch.cc/knoxdb/encoding/block"
	"blockwatch.cc/knoxdb/filter/bloomVec"
	"blockwatch.cc/knoxdb/vec"
)

const (
	headerBaseSize            = 3
	headerListVersion    byte = 2 // 2: +cardinality, bloom
	blockTypeMask        byte = 0x1f
	blockCompressionMask byte = 0x03
	blockScaleMask       byte = 0x7f
	blockFilterMask      byte = 0x03
)

type BlockInfo struct {
	Type        block.BlockType
	Compression block.Compression
	Scale       int

	// statistics
	MinValue    interface{}      // vector min
	MaxValue    interface{}      // vector max
	Bitmap      *vec.Bitset      // Bitmap for 8/16 bit datatypes
	Bloom       *bloomVec.Filter // optimized bloom filter for other datatypes
	Cardinality uint32           // unique items in vector
	dirty       bool             // update required
}

func (h BlockInfo) IsValid() bool {
	return h.Type != block.BlockIgnore && h.MinValue != nil && h.MaxValue != nil
}

func (h BlockInfo) IsDirty() bool {
	return h.dirty
}

func (h BlockInfo) SetDirty() {
	h.dirty = true
}

type BlockInfoList []BlockInfo

func (h BlockInfoList) Encode(buf *bytes.Buffer) error {
	buf.WriteByte(headerListVersion)
	var b [4]byte
	binary.BigEndian.PutUint32(b[:], uint32(len(h)))
	buf.Write(b[:])
	for _, v := range h {
		if err := v.Encode(buf); err != nil {
			return err
		}
	}
	return nil
}

func (h *BlockInfoList) Decode(buf *bytes.Buffer) error {
	if buf.Len() < 5 {
		return fmt.Errorf("pack: short block info list, length %d", buf.Len())
	}

	// read and check version byte
	ver, _ := buf.ReadByte()
	if ver > headerListVersion {
		return fmt.Errorf("pack: invalid block info list version %d", ver)
	}

	// read slice length
	l := int(binary.BigEndian.Uint32(buf.Next(4)))

	// alloc slice
	*h = make(BlockInfoList, l)

	// decode header parts
	for i := range *h {
		if err := (*h)[i].Decode(buf, ver); err != nil {
			return err
		}
	}
	return nil
}

func NewBlockInfo(b *block.Block, field Field) BlockInfo {
	h := BlockInfo{
		Type:        b.Type(),
		Compression: b.Compression(),
		Scale:       field.Scale,
		dirty:       b.Len() > 0,
	}
	switch b.Type() {
	case block.BlockTime:
		h.MinValue = time.Time{}
		h.MaxValue = time.Time{}
	case block.BlockFloat64:
		h.MinValue = float64(0.0)
		h.MaxValue = float64(0.0)
	case block.BlockFloat32:
		h.MinValue = float32(0.0)
		h.MaxValue = float32(0.0)
	case block.BlockInt64:
		h.MinValue = int64(0)
		h.MaxValue = int64(0)
	case block.BlockInt32:
		h.MinValue = int32(0)
		h.MaxValue = int32(0)
	case block.BlockInt16:
		h.MinValue = int16(0)
		h.MaxValue = int16(0)
	case block.BlockInt8:
		h.MinValue = int8(0)
		h.MaxValue = int8(0)
	case block.BlockUint64:
		h.MinValue = uint64(0)
		h.MaxValue = uint64(0)
	case block.BlockUint32:
		h.MinValue = uint32(0)
		h.MaxValue = uint32(0)
	case block.BlockUint16:
		h.MinValue = uint16(0)
		h.MaxValue = uint16(0)
	case block.BlockUint8:
		h.MinValue = uint8(0)
		h.MaxValue = uint8(0)
	case block.BlockBool:
		h.MinValue = false
		h.MaxValue = false
	case block.BlockString:
		h.MinValue = ""
		h.MaxValue = ""
	case block.BlockBytes:
		h.MinValue = []byte{}
		h.MaxValue = []byte{}
	case block.BlockInt128:
		h.MinValue = vec.ZeroInt128
		h.MaxValue = vec.ZeroInt128
	case block.BlockInt256:
		h.MinValue = vec.ZeroInt256
		h.MaxValue = vec.ZeroInt256
	}
	return h
}

func (h BlockInfo) EncodedSize() int {
	sz := headerBaseSize
	if h.Bloom != nil {
		sz += len(h.Bloom.Bytes())
	}
	switch h.Type {
	case block.BlockInt64,
		block.BlockTime,
		block.BlockUint64,
		block.BlockFloat64:
		return sz + 16
	case block.BlockBool:
		return sz + 1
	case block.BlockString:
		return sz + len(h.MinValue.(string)) + len(h.MaxValue.(string)) + 2
	case block.BlockBytes:
		min, max := h.MinValue.([]byte), h.MaxValue.([]byte)
		l1, l2 := len(min), len(max)
		var v [binary.MaxVarintLen64]byte
		i1 := binary.PutUvarint(v[:], uint64(l1))
		i2 := binary.PutUvarint(v[:], uint64(l2))
		return sz + l1 + l2 + i1 + i2
	case block.BlockInt32:
		return sz + 8
	case block.BlockInt16:
		return sz + 4
	case block.BlockInt8:
		return sz + 2
	case block.BlockUint32:
		return sz + 8
	case block.BlockUint16:
		return sz + 4
	case block.BlockUint8:
		return sz + 2
	case block.BlockFloat32:
		return sz + 8
	case block.BlockInt128:
		return sz + 32
	case block.BlockInt256:
		return sz + 64
	default:
		return 0
	}
}

func (h BlockInfo) Encode(buf *bytes.Buffer) error {
	// same encoding as lower level block header,
	// 8                 7 6          5 4 3 2 1
	// ext header flag   compression  block type
	buf.WriteByte(byte(h.Type)&blockTypeMask | (byte(h.Compression)&blockCompressionMask)<<5 | 0x80)

	// extension header
	// - 7 lower bits are used for storing scale (0..127)
	// - 1 MSB is extension flag
	buf.WriteByte(byte(h.Scale)&blockScaleMask | 0x80)

	// extension header
	// - 1 MSB is extension flag (currently unused)
	// - 2 bits filter type
	// - 5 bits unused
	filter := block.NoFilter
	if h.Bloom != nil {
		filter = block.BloomFilter
	}
	buf.WriteByte((byte(filter) & blockFilterMask) << 5)

	// write cardinality, 32bit
	var b [4]byte
	bigEndian.PutUint32(b[0:], h.Cardinality)
	_, _ = buf.Write(b[:])

	// write type-specific min/max values
	switch h.Type {
	case block.BlockTime:
		var v [16]byte
		min, max := h.MinValue.(time.Time), h.MaxValue.(time.Time)
		vmin, vmax := min.UnixNano(), max.UnixNano()
		bigEndian.PutUint64(v[0:], uint64(vmin))
		bigEndian.PutUint64(v[8:], uint64(vmax))
		_, _ = buf.Write(v[:])

	case block.BlockFloat64:
		var v [16]byte
		min, max := h.MinValue.(float64), h.MaxValue.(float64)
		bigEndian.PutUint64(v[0:], math.Float64bits(min))
		bigEndian.PutUint64(v[8:], math.Float64bits(max))
		_, _ = buf.Write(v[:])

	case block.BlockFloat32:
		var v [8]byte
		min, max := h.MinValue.(float32), h.MaxValue.(float32)
		bigEndian.PutUint32(v[0:], math.Float32bits(min))
		bigEndian.PutUint32(v[4:], math.Float32bits(max))
		_, _ = buf.Write(v[:])

	case block.BlockInt64:
		var v [16]byte
		min, max := h.MinValue.(int64), h.MaxValue.(int64)
		bigEndian.PutUint64(v[0:], uint64(min))
		bigEndian.PutUint64(v[8:], uint64(max))
		_, _ = buf.Write(v[:])

	case block.BlockInt32:
		var v [8]byte
		min, max := h.MinValue.(int32), h.MaxValue.(int32)
		bigEndian.PutUint32(v[0:], uint32(min))
		bigEndian.PutUint32(v[4:], uint32(max))
		_, _ = buf.Write(v[:])

	case block.BlockInt16:
		var v [4]byte
		min, max := h.MinValue.(int16), h.MaxValue.(int16)
		bigEndian.PutUint16(v[0:], uint16(min))
		bigEndian.PutUint16(v[2:], uint16(max))
		_, _ = buf.Write(v[:])

	case block.BlockInt8:
		var v [2]byte
		min, max := h.MinValue.(int8), h.MaxValue.(int8)
		v[0] = uint8(min)
		v[1] = uint8(max)
		_, _ = buf.Write(v[:])

	case block.BlockUint64:
		var v [16]byte
		min, max := h.MinValue.(uint64), h.MaxValue.(uint64)
		bigEndian.PutUint64(v[0:], min)
		bigEndian.PutUint64(v[8:], max)
		_, _ = buf.Write(v[:])

	case block.BlockUint32:
		var v [8]byte
		min, max := h.MinValue.(uint32), h.MaxValue.(uint32)
		bigEndian.PutUint32(v[0:], min)
		bigEndian.PutUint32(v[4:], max)
		_, _ = buf.Write(v[:])

	case block.BlockUint16:
		var v [4]byte
		min, max := h.MinValue.(uint16), h.MaxValue.(uint16)
		bigEndian.PutUint16(v[0:], min)
		bigEndian.PutUint16(v[2:], max)
		_, _ = buf.Write(v[:])

	case block.BlockUint8:
		var v [2]byte
		min, max := h.MinValue.(uint8), h.MaxValue.(uint8)
		v[0] = min
		v[1] = max
		_, _ = buf.Write(v[:])

	case block.BlockBool:
		var v byte
		min, max := h.MinValue.(bool), h.MaxValue.(bool)
		if min {
			v = 1
		}
		if max {
			v += 2
		}
		buf.WriteByte(v)

	case block.BlockString:
		// null terminated string
		min, max := h.MinValue.(string), h.MaxValue.(string)
		_, _ = buf.WriteString(min)
		buf.WriteByte(0)
		_, _ = buf.WriteString(max)
		buf.WriteByte(0)

	case block.BlockBytes:
		// len prefixed byte slice
		min, max := h.MinValue.([]byte), h.MaxValue.([]byte)
		var v [binary.MaxVarintLen64]byte
		i := binary.PutUvarint(v[:], uint64(len(min)))
		_, _ = buf.Write(v[:i])
		_, _ = buf.Write(min)

		i = binary.PutUvarint(v[:], uint64(len(max)))
		_, _ = buf.Write(v[:i])
		_, _ = buf.Write(max)

	case block.BlockInt128:
		min, max := h.MinValue.(vec.Int128).Bytes16(), h.MaxValue.(vec.Int128).Bytes16()
		_, _ = buf.Write(min[:])
		_, _ = buf.Write(max[:])

	case block.BlockInt256:
		min, max := h.MinValue.(vec.Int256).Bytes32(), h.MaxValue.(vec.Int256).Bytes32()
		_, _ = buf.Write(min[:])
		_, _ = buf.Write(max[:])

	default:
		return fmt.Errorf("pack: invalid block type %d", h.Type)
	}

	// write bloom filter data (size can be calculated from other info, so we skip this)
	if h.Bloom != nil {
		buf.Write(h.Bloom.Bytes())
	}

	h.dirty = false
	return nil
}

func (h *BlockInfo) Decode(buf *bytes.Buffer, version byte) error {
	val := buf.Next(1)
	h.Type = block.BlockType(val[0] & blockTypeMask)
	h.Compression = block.Compression((val[0] >> 5) & blockCompressionMask)
	if val[0]&0x80 > 0 {
		val = buf.Next(1)
		h.Scale = int(val[0] & blockScaleMask)
	}
	var filter block.Filter
	if val[0]&0x80 > 0 {
		val = buf.Next(1)
		filter = block.Filter((val[0] >> 5) & blockFilterMask)
	}
	h.dirty = false

	// be backwards compatible
	if version > 1 {
		h.Cardinality = bigEndian.Uint32(buf.Next(4))
	}

	switch h.Type {
	case block.BlockTime:
		v := buf.Next(16)
		vmin := bigEndian.Uint64(v[0:])
		vmax := bigEndian.Uint64(v[8:])
		h.MinValue = time.Unix(0, int64(vmin)).UTC()
		h.MaxValue = time.Unix(0, int64(vmax)).UTC()

	case block.BlockFloat64:
		v := buf.Next(16)
		h.MinValue = math.Float64frombits(bigEndian.Uint64(v[0:]))
		h.MaxValue = math.Float64frombits(bigEndian.Uint64(v[8:]))

	case block.BlockFloat32:
		v := buf.Next(8)
		h.MinValue = math.Float32frombits(bigEndian.Uint32(v[0:]))
		h.MaxValue = math.Float32frombits(bigEndian.Uint32(v[4:]))

	case block.BlockInt64:
		v := buf.Next(16)
		h.MinValue = int64(bigEndian.Uint64(v[0:]))
		h.MaxValue = int64(bigEndian.Uint64(v[8:]))

	case block.BlockInt32:
		v := buf.Next(8)
		h.MinValue = int32(bigEndian.Uint32(v[0:]))
		h.MaxValue = int32(bigEndian.Uint32(v[4:]))

	case block.BlockInt16:
		v := buf.Next(4)
		h.MinValue = int16(bigEndian.Uint16(v[0:]))
		h.MaxValue = int16(bigEndian.Uint16(v[2:]))

	case block.BlockInt8:
		v := buf.Next(2)
		h.MinValue = int8(v[0])
		h.MaxValue = int8(v[1])

	case block.BlockUint64:
		v := buf.Next(16)
		h.MinValue = bigEndian.Uint64(v[0:])
		h.MaxValue = bigEndian.Uint64(v[8:])

	case block.BlockUint32:
		v := buf.Next(8)
		h.MinValue = bigEndian.Uint32(v[0:])
		h.MaxValue = bigEndian.Uint32(v[4:])

	case block.BlockUint16:
		v := buf.Next(4)
		h.MinValue = uint16(bigEndian.Uint16(v[0:]))
		h.MaxValue = uint16(bigEndian.Uint16(v[2:]))

	case block.BlockUint8:
		v := buf.Next(2)
		h.MinValue = uint8(v[0])
		h.MaxValue = uint8(v[1])

	case block.BlockBool:
		v := buf.Next(1)
		h.MinValue = v[0]&1 > 0
		h.MaxValue = v[0]&2 > 0

	case block.BlockString:
		min, err := buf.ReadString(0)
		if err != nil {
			return fmt.Errorf("pack: reading min string block info: %w", err)
		}
		max, err := buf.ReadString(0)
		if err != nil {
			return fmt.Errorf("pack: reading max string block info: %w", err)
		}
		// don't reference buffer data!
		mincopy := min[:len(min)-1]
		maxcopy := max[:len(max)-1]
		h.MinValue = mincopy
		h.MaxValue = maxcopy

	case block.BlockBytes:
		length, err := binary.ReadUvarint(buf)
		if err != nil {
			return fmt.Errorf("pack: reading min []byte block info: %w", err)
		}
		min := buf.Next(int(length))
		length, err = binary.ReadUvarint(buf)
		if err != nil {
			return fmt.Errorf("pack: reading max []byte block info: %w", err)
		}
		max := buf.Next(int(length))

		// don't reference buffer data!
		mincopy := make([]byte, len(min))
		maxcopy := make([]byte, len(max))
		copy(mincopy, min)
		copy(maxcopy, max)
		h.MinValue = mincopy
		h.MaxValue = maxcopy

	case block.BlockInt128:
		v := buf.Next(32)
		h.MinValue = vec.Int128FromBytes(v[0:16])
		h.MaxValue = vec.Int128FromBytes(v[16:32])

	case block.BlockInt256:
		v := buf.Next(64)
		h.MinValue = vec.Int256FromBytes(v[0:32])
		h.MaxValue = vec.Int256FromBytes(v[32:64])

	default:
		return fmt.Errorf("pack: invalid block type %d", h.Type)
	}

	switch filter {
	case block.BloomFilter:
		// filter size is cardinality rounded up to next pow-2 times scale factor
		sz := h.Scale * int(pow2(int64(h.Cardinality)))
		b := buf.Next(sz)
		if len(b) < sz {
			return fmt.Errorf("pack: reading bloom filter: %w", io.ErrShortBuffer)
		}
		// we use a fixed number of 4 hash locations
		bCopy := make([]byte, sz)
		copy(bCopy, b)
		var err error
		h.Bloom, err = bloomVec.NewFilterBuffer(bCopy)
		if err != nil {
			return fmt.Errorf("pack: reading bloom filter: %w", err)
		}
	}

	return nil
}

func pow2(x int64) int64 {
	for i := int64(1); i < 1<<62; i = i << 1 {
		if i >= x {
			return i
		}
	}
	return 1
}
